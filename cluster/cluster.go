// Package cluster provides distributed synchronization for one or more
// bw databases using the [alan] UDP/QUIC peer discovery library.
//
// # Architecture
//
// One node in the cluster holds the "leader" lock (via alan's distributed
// lock). Only the leader writes to any database; every other node is a
// read-only follower that replicates data via incremental backups.
//
// A single Cluster can manage one OR several bw.DB instances. Each
// database is registered under a unique name (see [Cluster.AddDB] or the
// first DB passed to [New]) and is replicated independently on the wire —
// version probing, catch-up and push happen per-DB so a slow DB does not
// stall the others. Leader election itself is global to the cluster:
// whichever node holds the lock is the leader for ALL registered DBs.
//
// Leader election uses [alan.Alan.RunAsLeader]: every non-leader is parked
// inside alan.Lock waiting for the lock to become free. When the current
// leader disconnects (heartbeat timeout) or releases the lock, alan grants
// it to one of the waiters immediately — there is no retry gap, so at any
// moment either a leader exists or one is about to be granted leadership.
// Before accepting writes the new leader catches up each registered DB
// from the peer with the highest version, so no acknowledged data is lost
// (assuming at least one follower received the last sync).
//
// # Reads and writes
//
//   - Reads are always local. Every node (leader or follower) can serve
//     reads from its own copy of each database without any network
//     round-trip.
//   - Writes must go through the leader. The application checks
//     [Cluster.IsLeader] and either writes locally (if leader) or
//     forwards the request via [Cluster.Forward].
//   - After a successful write the leader calls [Cluster.NotifySync]
//     (optionally scoped to a specific DB name) which pushes the diff to
//     all behind followers and blocks until they have restored it (or
//     the caller's context is cancelled).
//
// # Sync protocol
//
// All cluster traffic is multiplexed onto a single alan handler keyed by
// the configured prefix (default "bw"). Two channels are used:
//
//   - Bytes (small, request/reply) on `<prefix>` for control. Every
//     payload that targets a specific DB starts with a length-prefixed
//     name (1 byte length + N bytes name) right after the tag byte.
//
//     Tag   Name             Direction        Payload (after tag)
//     0x01  LeaderAnnounce   leader -> all    (empty)        (fire-and-forget)
//     0x03  VersionRequest   any   -> any     [nameLen:1][name] -> [version:8]
//     0x04  Forward          any   -> leader  [app...] -> [resp]
//     0x05  PullRequest      any   -> any     [nameLen:1][name][since:8] -> [tag:1]
//
//   - Stream (arbitrary size) on `<prefix>-push` for diff data. Each
//     stream begins with [nameLen:1][name] followed by the output of
//     [bw.DB.Backup]; the receiver decodes the name, looks up the
//     matching DB, and pipes the rest into [bw.DB.Restore]. Stream
//     completion is the natural acknowledgement that the receiver has
//     finished applying the diff.
//
// PullRequest can be served by any node, which lets a newly elected leader
// catch up from whichever peer has the highest version for each DB.
//
// Followers also run a periodic pull loop (default every 5 minutes,
// configurable via [WithSyncInterval]) as a safety net in case a push was
// missed or the node joined late. The loop iterates every registered DB.
//
// # Quick start (single DB)
//
//	db, _ := bw.Open("/var/lib/myapp")
//	defer db.Close()
//
//	a, _ := alan.New(alan.Config{
//	    DNSAddr:  "myapp-headless.default.svc.cluster.local",
//	    Port:     7946,
//	    Replicas: 3,
//	})
//
//	c := cluster.New(db, a,
//	    cluster.WithSyncInterval(5*time.Minute),
//	    cluster.WithLockKey("myapp-leader"),
//	)
//	if err := c.Start(ctx); err != nil { log.Fatal(err) }
//	defer c.Stop()
//
// # Quick start (multiple DBs)
//
//	users, _   := bw.Open("/var/lib/myapp/users")
//	orders, _  := bw.Open("/var/lib/myapp/orders")
//	defer users.Close()
//	defer orders.Close()
//
//	c := cluster.New(users, a, cluster.WithDBName("users"))
//	c.AddDB("orders", orders)
//	c.Start(ctx)
//	defer c.Stop()
//
//	// per-DB NotifySync — narrow the broadcast to one DB:
//	c.NotifySyncDB(ctx, "users")
//	// or sync everything in parallel:
//	c.NotifySync(ctx)
//
// # Forwarding writes (application-level)
//
// The cluster package provides a built-in [Cluster.Forward] helper that
// sends an application request to the leader over alan and returns the
// response. Forwarding is DB-agnostic — the payload is opaque application
// data and the handler decides which DB(s) to touch:
//
//	c := cluster.New(users, a,
//	    cluster.WithDBName("users"),
//	    cluster.WithForwardHandler(func(ctx context.Context, data []byte) []byte {
//	        var req CreateUserRequest
//	        json.Unmarshal(data, &req)
//	        _ = users.Insert(ctx, &User{ID: req.ID, Name: req.Name})
//	        _ = c.NotifySyncDB(ctx, "users")
//	        resp, _ := json.Marshal(CreateUserResponse{OK: true})
//	        return resp
//	    }),
//	)
//
// # Limitations
//
//   - There is no automatic write-forwarding for transports other than alan.
//     The application must use [Cluster.Forward] or its own transport.
//   - Consistency is eventual unless the caller waits for [Cluster.NotifySync].
//   - Database names must be 1..255 bytes (length-prefixed by a single byte
//     on the wire).
//
// # Sharing an alan instance
//
// If your application needs its own alan messaging alongside cluster sync,
// use [WithExternalAlan] so both subsystems share one instance. In that
// mode [Cluster.Stop] only deregisters the handlers; alan's lifecycle is
// the caller's responsibility.
//
// [alan]: https://github.com/rakunlabs/alan
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rakunlabs/alan"
	"github.com/rakunlabs/bw"
)

// ErrNoLeader is returned by Forward when the leader address is not yet known.
var ErrNoLeader = errors.New("cluster: leader address unknown")

// ErrUnknownDB is returned when an operation references a database name
// that was never registered on this Cluster.
var ErrUnknownDB = errors.New("cluster: unknown database name")

// ErrInvalidDBName is returned when a database name is empty or too long
// to fit in the single-byte length prefix used on the wire (1..255 bytes).
var ErrInvalidDBName = errors.New("cluster: db name must be 1..255 bytes")

// DefaultDBName is the name used for the primary database passed to [New]
// when [WithDBName] is not supplied. Kept short to minimise wire overhead.
const DefaultDBName = "default"

// Internal message type tags (first byte of the alan bytes payload).
const (
	msgLeaderAnnounce byte = 0x01
	msgVersionRequest byte = 0x03
	msgForward        byte = 0x04
	msgPullRequest    byte = 0x05
)

// Stream sub-channel name. Streams are registered under
// "<prefix>" + streamSuffix to keep them distinct from the bytes channel.
const streamSuffix = "-push"

// Cluster wraps one or more bw.DB instances and an alan.Alan instance to
// form a replicated cluster.
type Cluster struct {
	alan *alan.Alan

	// dbsMu guards reads/writes of the dbs map AFTER Start. Registering
	// new DBs after Start is supported but rare; the map is read on
	// every inbound message so we keep the lock-free hot path by
	// snapshotting via dbs.Load when possible. For simplicity we use a
	// plain RWMutex here.
	dbsMu sync.RWMutex
	dbs   map[string]*bw.DB

	lockKey      string
	syncInterval time.Duration
	prefix       string // message namespace prefix to avoid collisions

	// externalAlan, when true, means alan is managed externally. The cluster
	// registers its handler on alan under its prefix but does not call
	// alan.Start/Stop.
	externalAlan bool

	isLeader   atomic.Bool
	leaderAddr atomic.Pointer[net.UDPAddr]

	// syncMu serializes inbound restores so concurrent pushes/pulls don't
	// race on any local database. A single mutex covers all DBs — restore
	// is rare enough relative to reads that the contention is acceptable
	// and the invariant (one restore at a time on this node) is simpler.
	syncMu sync.Mutex

	// onLeaderChange is called when leadership status changes.
	onLeaderChange func(isLeader bool)

	// forwardHandler processes forwarded requests on the leader.
	forwardHandler func(ctx context.Context, data []byte) []byte

	// primaryName is the name registered for the *bw.DB passed to New.
	// Retained so DB() can keep its single-DB return shape.
	primaryName string
}

// Option configures a Cluster.
type Option func(*Cluster)

// WithLockKey sets the distributed lock name used for leader election.
// Default: "bw-leader".
func WithLockKey(key string) Option {
	return func(c *Cluster) { c.lockKey = key }
}

// WithSyncInterval sets how often followers proactively pull from the leader
// as a safety net. Default: 5 minutes.
func WithSyncInterval(d time.Duration) Option {
	return func(c *Cluster) { c.syncInterval = d }
}

// WithPrefix sets the message namespace prefix. The cluster registers its
// bytes handler under prefix and its stream handler under prefix+"-push".
// Default: "bw".
func WithPrefix(prefix string) Option {
	return func(c *Cluster) { c.prefix = prefix }
}

// WithOnLeaderChange registers a callback invoked when this node gains or
// loses leadership.
func WithOnLeaderChange(fn func(isLeader bool)) Option {
	return func(c *Cluster) { c.onLeaderChange = fn }
}

// WithForwardHandler registers a function that the leader uses to process
// requests forwarded by followers via [Cluster.Forward].
func WithForwardHandler(fn func(ctx context.Context, data []byte) []byte) Option {
	return func(c *Cluster) { c.forwardHandler = fn }
}

// WithExternalAlan indicates that the alan instance is managed externally.
//
// Note: cluster.Start installs OnPeerJoin and OnPeerLeave handlers on alan;
// alan only stores a single handler per event, so any handlers the caller
// previously registered will be overwritten. Callers that need their own
// peer hooks should compose them by wrapping cluster.OnPeerLeave behaviour
// (clear leaderAddr if it matches) into their own handler before calling
// alan.OnPeerLeave themselves AFTER cluster.Start.
func WithExternalAlan() Option {
	return func(c *Cluster) { c.externalAlan = true }
}

// WithDBName sets the name registered for the primary *bw.DB passed to
// [New]. Default: [DefaultDBName]. Use [Cluster.AddDB] to register
// additional databases.
func WithDBName(name string) Option {
	return func(c *Cluster) { c.primaryName = name }
}

// New creates a new Cluster around a primary database. Additional
// databases can be attached with [Cluster.AddDB] before or after
// [Cluster.Start].
func New(db *bw.DB, a *alan.Alan, opts ...Option) *Cluster {
	c := &Cluster{
		alan:         a,
		dbs:          make(map[string]*bw.DB),
		lockKey:      "bw-leader",
		syncInterval: 5 * time.Minute,
		prefix:       "bw",
		primaryName:  DefaultDBName,
	}
	for _, opt := range opts {
		opt(c)
	}
	// Register the primary DB under its (possibly customised) name.
	if db != nil {
		c.dbs[c.primaryName] = db
	}
	return c
}

// AddDB registers an additional database under the given name. The name
// must be 1..255 bytes and unique within the cluster instance. It can be
// called before or after [Cluster.Start]; once registered, peers can
// immediately probe/sync this DB.
//
// Note: every node in the cluster must register the SAME set of DB names
// (and the corresponding bucket schemas via bw.RegisterBucket on each
// db). A peer that receives a sync request for a name it does not know
// will reply with an empty/zero version and the leader will skip pushing
// to it, leaving that peer permanently stale for that DB.
func (c *Cluster) AddDB(name string, db *bw.DB) error {
	if err := validateName(name); err != nil {
		return err
	}
	if db == nil {
		return errors.New("cluster: db is nil")
	}
	c.dbsMu.Lock()
	defer c.dbsMu.Unlock()
	if _, exists := c.dbs[name]; exists {
		return fmt.Errorf("cluster: db name %q already registered", name)
	}
	c.dbs[name] = db
	return nil
}

// DB returns the primary database (the one passed to [New]). For
// multi-DB clusters use [Cluster.DBByName] or [Cluster.DBNames].
func (c *Cluster) DB() *bw.DB {
	c.dbsMu.RLock()
	defer c.dbsMu.RUnlock()
	return c.dbs[c.primaryName]
}

// DBByName returns the database registered under name, or nil if the
// name is unknown.
func (c *Cluster) DBByName(name string) *bw.DB {
	c.dbsMu.RLock()
	defer c.dbsMu.RUnlock()
	return c.dbs[name]
}

// DBNames returns a sorted snapshot of every registered DB name.
func (c *Cluster) DBNames() []string {
	c.dbsMu.RLock()
	names := make([]string, 0, len(c.dbs))
	for n := range c.dbs {
		names = append(names, n)
	}
	c.dbsMu.RUnlock()
	sort.Strings(names)
	return names
}

// Alan returns the underlying *alan.Alan.
func (c *Cluster) Alan() *alan.Alan { return c.alan }

// IsLeader reports whether this node currently holds the leader lock.
func (c *Cluster) IsLeader() bool { return c.isLeader.Load() }

// LeaderHealthy reports whether this node is the leader AND the cluster
// still has quorum.
func (c *Cluster) LeaderHealthy() bool { return c.alan.LeaderHealthy(c.lockKey) }

// LeaderAddr returns the last-known leader address, or nil if unknown.
func (c *Cluster) LeaderAddr() *net.UDPAddr { return c.leaderAddr.Load() }

// Status is a point-in-time snapshot of this node's view of the cluster.
type Status struct {
	// IsLeader is true when this node currently holds the leader lock.
	IsLeader bool
	// LeaderHealthy is true when this node is the leader AND the cluster
	// still has quorum. Always false on followers.
	LeaderHealthy bool
	// LeaderAddr is the last-known address of the leader, or nil if
	// unknown. On the leader this is nil — use LocalAddr instead.
	LeaderAddr *net.UDPAddr
	// LocalAddr is this node's own listening address (nil before alan
	// has finished starting).
	LocalAddr net.Addr
	// PeerCount is the number of peers currently visible to alan
	// (does NOT include this node).
	PeerCount int
	// Peers is the list of peer addresses currently visible to alan.
	Peers []*net.UDPAddr
	// QuorumSize is the number of nodes required for progress.
	QuorumSize int
	// HasQuorum is true when alan currently sees at least QuorumSize nodes.
	HasQuorum bool
	// Versions maps each registered DB name to its local version. On the
	// leader these are authoritative; on followers they are what has
	// been successfully restored so far.
	Versions map[string]uint64
	// LockKey is the distributed lock name used for leader election.
	LockKey string
}

// Status returns a snapshot of this node's view of the cluster.
func (c *Cluster) Status() Status {
	c.dbsMu.RLock()
	versions := make(map[string]uint64, len(c.dbs))
	for name, db := range c.dbs {
		versions[name] = db.Version()
	}
	c.dbsMu.RUnlock()

	return Status{
		IsLeader:      c.isLeader.Load(),
		LeaderHealthy: c.alan.LeaderHealthy(c.lockKey),
		LeaderAddr:    c.leaderAddr.Load(),
		LocalAddr:     c.alan.LocalAddr(),
		PeerCount:     c.alan.PeerCount(),
		Peers:         c.alan.Peers(),
		QuorumSize:    c.alan.QuorumSize(),
		HasQuorum:     c.alan.HasQuorum(),
		Versions:      versions,
		LockKey:       c.lockKey,
	}
}

// String renders the status in a single human-readable line, suitable for
// log lines or a CLI `status` subcommand.
func (s Status) String() string {
	role := "follower"
	if s.IsLeader {
		role = "leader"
		if !s.LeaderHealthy {
			role = "leader(unhealthy)"
		}
	}
	leader := "unknown"
	if s.LeaderAddr != nil {
		leader = s.LeaderAddr.String()
	} else if s.IsLeader && s.LocalAddr != nil {
		leader = s.LocalAddr.String() + " (self)"
	}
	peerStrs := make([]string, 0, len(s.Peers))
	for _, p := range s.Peers {
		peerStrs = append(peerStrs, p.String())
	}
	// Deterministic version ordering for log diffability.
	names := make([]string, 0, len(s.Versions))
	for n := range s.Versions {
		names = append(names, n)
	}
	sort.Strings(names)
	versParts := make([]string, 0, len(names))
	for _, n := range names {
		versParts = append(versParts, fmt.Sprintf("%s=%d", n, s.Versions[n]))
	}
	return fmt.Sprintf(
		"role=%s versions=[%s] leader=%s peers=%d/%d quorum=%t [%s]",
		role, strings.Join(versParts, " "), leader, s.PeerCount, s.QuorumSize, s.HasQuorum,
		strings.Join(peerStrs, ", "),
	)
}

func (c *Cluster) streamType() string { return c.prefix + streamSuffix }

// Start initializes the cluster: starts alan (unless [WithExternalAlan]),
// begins leader election, and launches the periodic sync loop.
func (c *Cluster) Start(ctx context.Context) error {
	if len(c.dbs) == 0 {
		return errors.New("cluster: no databases registered")
	}

	c.alan.Handle(c.prefix, c.handleMsg)
	c.alan.HandleStream(c.streamType(), c.handleStream)

	if !c.externalAlan {
		errCh := make(chan error, 1)
		go func() {
			errCh <- c.alan.Start(ctx)
		}()

		select {
		case <-c.alan.Ready():
		case err := <-errCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// On peer join: if we are the leader and the joiner is behind us on
	// any DB, push a snapshot for that DB immediately so they don't have
	// to wait for the next write or the periodic-sync ticker.
	c.alan.OnPeerJoin(func(addr *net.UDPAddr) {
		slog.Info("cluster: peer joined", "addr", addr, "peers", c.alan.PeerCount())
		if !c.alan.IsLeader(c.lockKey) {
			return
		}
		go func() {
			pushCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			for _, name := range c.DBNames() {
				if err := c.pushTo(pushCtx, addr, name, 0); err != nil && !isPeerGone(err) {
					slog.Warn("cluster: initial push to new peer failed",
						"addr", addr, "db", name, "error", err)
				}
			}
		}()
	})

	// Clear cached leader address whenever the leader peer disappears.
	c.alan.OnPeerLeave(func(addr *net.UDPAddr) {
		slog.Info("cluster: peer left", "addr", addr, "peers", c.alan.PeerCount())
		if cur := c.leaderAddr.Load(); cur != nil && udpEqual(cur, addr) {
			slog.Info("cluster: leader peer left, clearing leader addr", "addr", addr)
			c.leaderAddr.CompareAndSwap(cur, nil)
		}
	})

	go c.leaderLoop(ctx)
	go c.periodicSync(ctx)

	return nil
}

// udpEqual compares two UDP addresses by IP+Port (ignoring Zone differences).
func udpEqual(a, b *net.UDPAddr) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Port == b.Port && a.IP.Equal(b.IP)
}

// Stop gracefully shuts down the cluster.
func (c *Cluster) Stop() {
	if c.externalAlan {
		c.alan.Remove(c.prefix)
		c.alan.Remove(c.streamType())
	} else {
		_ = c.alan.Stop()
	}
}

// NotifySync pushes the latest diff of EVERY registered DB to all
// followers that are behind, and blocks until each one has finished
// applying every diff (or ctx is cancelled). Per-DB syncs run in
// parallel; errors from individual peers/DBs are joined via
// [errors.Join].
//
// If this node is not the leader, NotifySync is a no-op and returns nil.
func (c *Cluster) NotifySync(ctx context.Context) error {
	if !c.isLeader.Load() {
		return nil
	}
	if !c.alan.LeaderHealthy(c.lockKey) {
		return errors.New("cluster: leadership not healthy (quorum lost)")
	}

	names := c.DBNames()
	if len(names) == 0 {
		return nil
	}

	// Announce ourselves once so followers learn our address even if
	// every DB happens to be up-to-date.
	c.broadcastAnnounce()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	for _, name := range names {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			if err := c.notifySyncDB(ctx, name); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("db %q: %w", name, err))
				mu.Unlock()
			}
		}(name)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// NotifySyncDB pushes the latest diff of a single named DB to all
// followers that are behind. Equivalent to [Cluster.NotifySync] scoped
// to one DB; useful when only one DB has just been written and you
// don't want to probe versions for the rest.
//
// Returns [ErrUnknownDB] if name was not registered. No-op on followers.
func (c *Cluster) NotifySyncDB(ctx context.Context, name string) error {
	if !c.isLeader.Load() {
		return nil
	}
	if !c.alan.LeaderHealthy(c.lockKey) {
		return errors.New("cluster: leadership not healthy (quorum lost)")
	}
	if c.DBByName(name) == nil {
		return ErrUnknownDB
	}
	c.broadcastAnnounce()
	return c.notifySyncDB(ctx, name)
}

// notifySyncDB is the per-DB worker shared by NotifySync and NotifySyncDB.
// Caller is responsible for the leader/quorum/announce preamble.
func (c *Cluster) notifySyncDB(ctx context.Context, name string) error {
	db := c.DBByName(name)
	if db == nil {
		return ErrUnknownDB
	}
	myVersion := db.Version()

	req := encodeNameMsg(msgVersionRequest, name, nil)
	replies, err := c.alan.SendAndWaitReply(ctx, c.prefix, req)
	if err != nil && len(replies) == 0 {
		return err
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	for _, reply := range replies {
		if len(reply.Data) < 8 {
			continue
		}
		peerVersion := binary.BigEndian.Uint64(reply.Data)
		if peerVersion >= myVersion {
			continue
		}
		addr := reply.Addr

		wg.Add(1)
		go func(addr *net.UDPAddr, since uint64) {
			defer wg.Done()
			if err := c.pushTo(ctx, addr, name, since); err != nil && !isPeerGone(err) {
				mu.Lock()
				errs = append(errs, fmt.Errorf("push to %s: %w", addr, err))
				mu.Unlock()
			}
		}(addr, peerVersion)
	}

	wg.Wait()
	return errors.Join(errs...)
}

// Forward sends an application-level request to the current leader and
// returns its response. If this node is the leader, the registered
// forward handler is invoked directly without a network round-trip.
//
// The payload is opaque to the cluster: forwarding does not know or care
// which DB(s) the handler will touch. Encode that information inside
// `data` if you need it.
func (c *Cluster) Forward(ctx context.Context, data []byte) ([]byte, error) {
	if c.isLeader.Load() {
		if c.forwardHandler == nil {
			return nil, nil
		}
		return c.forwardHandler(ctx, data), nil
	}

	addr := c.leaderAddr.Load()
	if addr == nil {
		return nil, ErrNoLeader
	}

	payload := make([]byte, 1+len(data))
	payload[0] = msgForward
	copy(payload[1:], data)

	reply, err := c.alan.SendToAndWaitReply(ctx, addr, c.prefix, payload)
	if err != nil {
		return nil, err
	}
	return reply.Data, nil
}

// ---------------------------------------------------------------------------
// Leader election
// ---------------------------------------------------------------------------

func (c *Cluster) leaderLoop(ctx context.Context) {
	slog.Info("cluster: waiting for leadership",
		"key", c.lockKey,
		"peers", c.alan.PeerCount(),
		"quorum", c.alan.QuorumSize(),
		"has_quorum", c.alan.HasQuorum(),
	)
	_ = c.alan.LeaderLoop(ctx, c.lockKey, time.Second, c.runAsLeader)
}

// runAsLeader is invoked by alan once this node acquires the lock.
func (c *Cluster) runAsLeader(ctx context.Context) error {
	c.leaderAddr.Store(nil)

	c.isLeader.Store(true)
	if c.onLeaderChange != nil {
		c.onLeaderChange(true)
	}
	defer func() {
		c.isLeader.Store(false)
		if c.onLeaderChange != nil {
			c.onLeaderChange(false)
		}
	}()

	slog.Info("cluster: became leader",
		"peers", c.alan.PeerCount(), "dbs", c.DBNames())

	// Catch up every DB from the peer with the highest version. Bounded
	// so a slow / stuck peer can't block us from announcing leadership.
	catchCtx, catchCancel := context.WithTimeout(ctx, 30*time.Second)
	for _, name := range c.DBNames() {
		c.catchUp(catchCtx, name)
	}
	catchCancel()

	c.broadcastAnnounce()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			c.broadcastAnnounce()
		}
	}
}

// catchUp asks all peers for their version of the named DB, then pulls
// an incremental backup from whichever peer is furthest ahead.
func (c *Cluster) catchUp(ctx context.Context, name string) {
	db := c.DBByName(name)
	if db == nil {
		return
	}
	myVersion := db.Version()

	if c.alan.PeerCount() == 0 {
		return
	}

	req := encodeNameMsg(msgVersionRequest, name, nil)
	replies, err := c.alan.SendAndWaitReply(ctx, c.prefix, req)
	if err != nil && len(replies) == 0 {
		slog.Warn("cluster: catch-up version probe failed", "db", name, "error", err)
		return
	}

	var bestPeer *net.UDPAddr
	var bestVersion uint64
	for _, reply := range replies {
		if len(reply.Data) < 8 {
			continue
		}
		v := binary.BigEndian.Uint64(reply.Data)
		if v > bestVersion {
			bestVersion = v
			bestPeer = reply.Addr
		}
	}

	if bestVersion <= myVersion || bestPeer == nil {
		return
	}

	slog.Info("cluster: catching up",
		"db", name, "peer", bestPeer, "from", myVersion, "to", bestVersion)

	if err := c.pullFrom(ctx, bestPeer, name); err != nil {
		slog.Error("cluster: catch-up failed", "db", name, "error", err)
	}
}

// ---------------------------------------------------------------------------
// Sync helpers
// ---------------------------------------------------------------------------

// pushTo opens an outbound stream to addr and writes an incremental
// backup of the named DB since the given version. The stream header
// carries the DB name so the receiver can route to the right DB.
func (c *Cluster) pushTo(ctx context.Context, addr *net.UDPAddr, name string, since uint64) error {
	db := c.DBByName(name)
	if db == nil {
		return fmt.Errorf("%w: %q", ErrUnknownDB, name)
	}

	pr, pw := io.Pipe()

	// Producer: write the name header, then the backup body.
	go func() {
		if err := writeNameHeader(pw, name); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_, err := db.Backup(pw, since, false)
		_ = pw.CloseWithError(err)
	}()

	_, err := c.alan.SendToStream(ctx, addr, c.streamType(), pr)
	_ = pr.CloseWithError(err)
	return err
}

// pullFrom asks addr for an incremental backup of the named DB since our
// current version of it.
func (c *Cluster) pullFrom(ctx context.Context, addr *net.UDPAddr, name string) error {
	db := c.DBByName(name)
	if db == nil {
		return fmt.Errorf("%w: %q", ErrUnknownDB, name)
	}

	version := db.Version()
	tail := make([]byte, 8)
	binary.BigEndian.PutUint64(tail, version)
	payload := encodeNameMsg(msgPullRequest, name, tail)

	_, err := c.alan.SendToAndWaitReply(ctx, addr, c.prefix, payload)
	return err
}

// broadcastAnnounce tells followers who the leader is. Followers use this
// only to learn the leader's address; per-DB versions are probed
// separately.
func (c *Cluster) broadcastAnnounce() {
	payload := []byte{msgLeaderAnnounce}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.alan.Send(ctx, c.prefix, payload)
}

// periodicSync runs on followers and pulls every registered DB from the
// leader every syncInterval as a safety net (in case a push was missed).
func (c *Cluster) periodicSync(ctx context.Context) {
	ticker := time.NewTicker(c.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if c.isLeader.Load() {
				continue
			}

			addr := c.leaderAddr.Load()
			if addr == nil {
				continue
			}

			for _, name := range c.DBNames() {
				if err := c.pullFrom(ctx, addr, name); err != nil {
					if isPeerGone(err) {
						slog.Debug("cluster: leader unreachable, clearing address",
							"addr", addr, "db", name)
						c.leaderAddr.CompareAndSwap(addr, nil)
						break // no point trying further DBs against the same dead peer
					}
					slog.Error("cluster: periodic sync failed",
						"db", name, "error", err)
				}
			}
		}
	}
}

// isPeerGone returns true if the error indicates the peer has left the
// cluster. These are transient and expected during leader failover.
func isPeerGone(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "peer disconnected") ||
		strings.Contains(msg, "no QUIC connection") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no recent network activity")
}

// ---------------------------------------------------------------------------
// Inbound message handlers
// ---------------------------------------------------------------------------

// handleMsg dispatches incoming bytes messages by tag byte.
func (c *Cluster) handleMsg(ctx context.Context, msg alan.Message) {
	if len(msg.Data) == 0 {
		return
	}

	switch msg.Data[0] {
	case msgLeaderAnnounce:
		c.onLeaderAnnounce(msg)
	case msgPullRequest:
		c.onPullRequest(ctx, msg)
	case msgVersionRequest:
		c.onVersionRequest(ctx, msg)
	case msgForward:
		c.onForward(ctx, msg)
	}
}

// handleStream ingests an incoming push. The stream header is the
// length-prefixed DB name; the rest is fed straight into the matching
// bw.DB.Restore.
func (c *Cluster) handleStream(ctx context.Context, msg alan.Message, body io.Reader) error {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()

	name, err := readNameHeader(body)
	if err != nil {
		slog.Error("cluster: read stream name header failed", "from", msg.Addr, "error", err)
		return err
	}

	db := c.DBByName(name)
	if db == nil {
		// Drain the body so the sender's SendToStream unblocks cleanly,
		// then surface the error. Without the drain the sender would
		// hang until its ctx fires.
		_, _ = io.Copy(io.Discard, body)
		slog.Warn("cluster: stream for unknown db", "from", msg.Addr, "db", name)
		return fmt.Errorf("%w: %q", ErrUnknownDB, name)
	}

	before := db.Version()
	if err := db.ApplyBackup(body); err != nil {
		slog.Error("cluster: restore from stream failed",
			"from", msg.Addr, "db", name, "error", err)
		return err
	}
	after := db.Version()
	if after == before {
		slog.Info("cluster: sync complete (no change)",
			"from", msg.Addr, "db", name, "version", after)
	} else {
		slog.Info("cluster: sync complete",
			"from", msg.Addr, "db", name,
			"from_version", before, "to_version", after)
	}
	return nil
}

// onLeaderAnnounce records the leader's address.
func (c *Cluster) onLeaderAnnounce(msg alan.Message) {
	if c.alan.IsLeader(c.lockKey) {
		return
	}
	c.leaderAddr.Store(msg.Addr)
}

// onPullRequest serves an incremental backup of the requested DB since
// the requested version. Any node can serve this.
func (c *Cluster) onPullRequest(ctx context.Context, msg alan.Message) {
	if !msg.IsRequest() {
		return
	}

	name, rest, ok := decodeNameMsg(msg.Data)
	if !ok || len(rest) < 8 {
		_, _ = c.alan.Reply(ctx, msg, nil)
		return
	}
	since := binary.BigEndian.Uint64(rest[:8])

	db := c.DBByName(name)
	if db == nil {
		_, _ = c.alan.Reply(ctx, msg, nil)
		return
	}

	if since >= db.Version() {
		_, _ = c.alan.Reply(ctx, msg, nil) // already up to date
		return
	}

	if err := c.pushTo(ctx, msg.Addr, name, since); err != nil {
		slog.Error("cluster: pull-response push failed",
			"to", msg.Addr, "db", name, "error", err)
	}

	_, _ = c.alan.Reply(ctx, msg, []byte{1})
}

// onVersionRequest replies with this node's current version of the named
// DB. Unknown DBs reply with version 0 (treated as "fully behind" by the
// requester, which is harmless: the leader will skip pushing because the
// peer has no DB to push to either).
func (c *Cluster) onVersionRequest(ctx context.Context, msg alan.Message) {
	if !msg.IsRequest() {
		return
	}

	name, _, ok := decodeNameMsg(msg.Data)
	if !ok {
		_, _ = c.alan.Reply(ctx, msg, nil)
		return
	}

	var v uint64
	if db := c.DBByName(name); db != nil {
		v = db.Version()
	}
	resp := make([]byte, 8)
	binary.BigEndian.PutUint64(resp, v)
	_, _ = c.alan.Reply(ctx, msg, resp)
}

// onForward handles a forwarded application request on the leader.
func (c *Cluster) onForward(ctx context.Context, msg alan.Message) {
	if !msg.IsRequest() {
		return
	}

	if c.forwardHandler == nil || !c.alan.LeaderHealthy(c.lockKey) {
		_, _ = c.alan.Reply(ctx, msg, nil)
		return
	}

	var appData []byte
	if len(msg.Data) > 1 {
		appData = msg.Data[1:]
	}

	resp := c.forwardHandler(ctx, appData)
	_, _ = c.alan.Reply(ctx, msg, resp)
}

// ---------------------------------------------------------------------------
// Wire helpers (length-prefixed name encoding)
// ---------------------------------------------------------------------------

// validateName enforces the 1..255 byte length constraint for DB names.
func validateName(name string) error {
	if len(name) == 0 || len(name) > 255 {
		return ErrInvalidDBName
	}
	return nil
}

// encodeNameMsg builds a payload of the form [tag][nameLen:1][name][tail...].
// Panics on invalid name length — callers must use validated names. We
// also accept already-validated names from internal call sites where the
// name came from the local registry (those are guaranteed valid because
// AddDB / WithDBName enforce the constraint).
func encodeNameMsg(tag byte, name string, tail []byte) []byte {
	if len(name) == 0 || len(name) > 255 {
		// Internal call sites should never hit this; surface clearly.
		panic(fmt.Sprintf("cluster: invalid db name length %d", len(name)))
	}
	out := make([]byte, 2+len(name)+len(tail))
	out[0] = tag
	out[1] = byte(len(name))
	copy(out[2:], name)
	copy(out[2+len(name):], tail)
	return out
}

// decodeNameMsg parses [tag][nameLen:1][name][rest...]. Returns ok=false
// if the buffer is malformed.
func decodeNameMsg(data []byte) (name string, rest []byte, ok bool) {
	if len(data) < 2 {
		return "", nil, false
	}
	nameLen := int(data[1])
	if nameLen == 0 || len(data) < 2+nameLen {
		return "", nil, false
	}
	return string(data[2 : 2+nameLen]), data[2+nameLen:], true
}

// writeNameHeader writes [nameLen:1][name] to w.
func writeNameHeader(w io.Writer, name string) error {
	if len(name) == 0 || len(name) > 255 {
		return ErrInvalidDBName
	}
	buf := make([]byte, 1+len(name))
	buf[0] = byte(len(name))
	copy(buf[1:], name)
	_, err := w.Write(buf)
	return err
}

// readNameHeader reads [nameLen:1][name] from r.
func readNameHeader(r io.Reader) (string, error) {
	var lenBuf [1]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return "", err
	}
	n := int(lenBuf[0])
	if n == 0 {
		return "", ErrInvalidDBName
	}
	nameBuf := make([]byte, n)
	if _, err := io.ReadFull(r, nameBuf); err != nil {
		return "", err
	}
	return string(nameBuf), nil
}
