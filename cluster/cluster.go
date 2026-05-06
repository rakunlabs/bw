// Package cluster provides distributed synchronization for bw databases
// using the [alan] UDP/QUIC peer discovery library.
//
// # Architecture
//
// One node in the cluster holds the "leader" lock (via alan's distributed
// lock). Only the leader writes to the database; every other node is a
// read-only follower that replicates data via incremental backups.
//
// Leader election uses [alan.Alan.RunAsLeader]: every non-leader is parked
// inside alan.Lock waiting for the lock to become free. When the current
// leader disconnects (heartbeat timeout) or releases the lock, alan grants
// it to one of the waiters immediately — there is no retry gap, so at any
// moment either a leader exists or one is about to be granted leadership.
// Before accepting writes the new leader catches up from the peer with the
// highest version, so no acknowledged data is lost (assuming at least one
// follower received the last sync).
//
// # Reads and writes
//
//   - Reads are always local. Every node (leader or follower) can serve
//     reads from its own copy of the database without any network round-trip.
//   - Writes must go through the leader. The application checks [Cluster.IsLeader]
//     and either writes locally (if leader) or forwards the request via
//     [Cluster.Forward].
//   - After a successful write the leader calls [Cluster.NotifySync] which
//     pushes the diff to all behind followers and blocks until they have
//     restored it (or the caller's context is cancelled).
//
// # Sync protocol
//
// All cluster traffic is multiplexed onto a single alan handler keyed by
// the configured prefix (default "bw"). Two channels are used:
//
//   - Bytes (small, request/reply) on `<prefix>` for control:
//
//     Tag   Name             Direction        Payload
//     0x01  LeaderAnnounce   leader -> all    [version:8]   (fire-and-forget)
//     0x03  VersionRequest   any   -> any     (empty) -> [version:8]
//     0x04  Forward          any   -> leader  [app...] -> [resp]
//     0x05  PullRequest      any   -> any     [since:8] -> [tag:1]
//
//   - Stream (arbitrary size) on `<prefix>-push` for diff data:
//     the responder of a PullRequest, or the leader during NotifySync,
//     opens an outbound stream carrying the result of [bw.DB.Backup]; the
//     receiver pipes the stream into [bw.DB.Restore]. Stream completion is
//     the natural acknowledgement that the receiver has finished applying
//     the diff.
//
// PullRequest can be served by any node, which lets a newly elected leader
// catch up from whichever peer has the highest version.
//
// Followers also run a periodic pull loop (default every 5 minutes,
// configurable via [WithSyncInterval]) as a safety net in case a push was
// missed or the node joined late.
//
// # Quick start
//
//	// 1. Open the database.
//	db, _ := bw.Open("/var/lib/myapp", bw.WithInMemory(false))
//	defer db.Close()
//
//	// 2. Create an alan instance (NOT started yet).
//	a, _ := alan.New(alan.Config{
//	    DNSAddr:  "myapp-headless.default.svc.cluster.local",
//	    Port:     7946,
//	    Replicas: 3,
//	})
//
//	// 3. Create and start the cluster.
//	c := cluster.New(db, a,
//	    cluster.WithSyncInterval(5*time.Minute),
//	    cluster.WithLockKey("myapp-leader"),
//	)
//	if err := c.Start(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	defer c.Stop()
//
//	// 4. Register buckets as usual.
//	users, _ := bw.RegisterBucket[User](db, "users")
//
//	// 5. Reads — always local, no leader check needed.
//	user, _ := users.Get(ctx, "u1")
//
//	// 6. Writes — only on the leader.
//	if c.IsLeader() {
//	    _ = users.Insert(ctx, &User{ID: "u1", Name: "Elif"})
//	    if err := c.NotifySync(ctx); err != nil {
//	        // followers may be stale until next periodic sync
//	    }
//	}
//
// # Forwarding writes (application-level)
//
// The cluster package provides a built-in [Cluster.Forward] helper that
// sends an application request to the leader over alan and returns the
// response:
//
//	c := cluster.New(db, a,
//	    cluster.WithForwardHandler(func(ctx context.Context, data []byte) []byte {
//	        var req CreateUserRequest
//	        json.Unmarshal(data, &req)
//	        _ = users.Insert(ctx, &User{ID: req.ID, Name: req.Name})
//	        _ = c.NotifySync(ctx) // wait for followers to catch up
//	        resp, _ := json.Marshal(CreateUserResponse{OK: true})
//	        return resp
//	    }),
//	)
//
//	func (s *Server) CreateUser(w http.ResponseWriter, r *http.Request) {
//	    body, _ := io.ReadAll(r.Body)
//	    resp, err := s.cluster.Forward(r.Context(), body)
//	    // resp contains the leader's response regardless of which node
//	    // received the HTTP request.
//	}
//
// # Leader change callback
//
// Use [WithOnLeaderChange] to react when this node gains or loses
// leadership (e.g. drain in-flight writes, update a readiness probe).
//
// # Limitations
//
//   - There is no automatic write-forwarding for transports other than alan.
//     The application must use [Cluster.Forward] or its own transport.
//   - Consistency is eventual unless the caller waits for [Cluster.NotifySync].
//
// # Sharing an alan instance
//
// If your application needs its own alan messaging alongside cluster sync,
// use [WithExternalAlan] so both subsystems share one instance:
//
//	a.Handle("app", appHandler)
//	go a.Start(ctx)
//	<-a.Ready()
//
//	c := cluster.New(db, a, cluster.WithExternalAlan())
//	c.Start(ctx) // registers handlers under prefix; does NOT call alan.Start
//
// In this mode, [Cluster.Stop] deregisters the handlers but does NOT stop
// alan — the caller manages alan's lifecycle independently.
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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rakunlabs/alan"
	"github.com/rakunlabs/bw"
)

// ErrNoLeader is returned by Forward when the leader address is not yet known.
var ErrNoLeader = errors.New("cluster: leader address unknown")

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

// Cluster wraps a bw.DB and an alan.Alan instance to form a replicated cluster.
type Cluster struct {
	db   *bw.DB
	alan *alan.Alan

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
	// race on the local database.
	syncMu sync.Mutex

	// onLeaderChange is called when leadership status changes.
	onLeaderChange func(isLeader bool)

	// forwardHandler processes forwarded requests on the leader.
	forwardHandler func(ctx context.Context, data []byte) []byte
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

// New creates a new Cluster.
func New(db *bw.DB, a *alan.Alan, opts ...Option) *Cluster {
	c := &Cluster{
		db:           db,
		alan:         a,
		lockKey:      "bw-leader",
		syncInterval: 5 * time.Minute,
		prefix:       "bw",
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// DB returns the underlying *bw.DB.
func (c *Cluster) DB() *bw.DB { return c.db }

// Alan returns the underlying *alan.Alan.
func (c *Cluster) Alan() *alan.Alan { return c.alan }

// IsLeader reports whether this node currently holds the leader lock.
//
// Equivalent to c.Alan().IsLeader(c.lockKey()) — kept for API stability.
func (c *Cluster) IsLeader() bool { return c.isLeader.Load() }

// LeaderHealthy reports whether this node is the leader AND the cluster
// still has quorum. Use this as a load-balancer readiness probe or to
// refuse writes during a partition: a node that is "leader" only because
// it has been isolated from the majority will return false here.
//
// alan's RunAsLeader cancels the leader fn ctx on quorum loss, so most
// code paths inside the leader fn don't need this — but external paths
// (e.g. an HTTP handler that forwarded a request before the cancellation
// reached it) should check this before applying the write.
func (c *Cluster) LeaderHealthy() bool { return c.alan.LeaderHealthy(c.lockKey) }

// LeaderAddr returns the last-known leader address, or nil if unknown.
func (c *Cluster) LeaderAddr() *net.UDPAddr { return c.leaderAddr.Load() }

func (c *Cluster) streamType() string { return c.prefix + streamSuffix }

// Start initializes the cluster: starts alan (unless [WithExternalAlan]),
// begins leader election, and launches the periodic sync loop.
func (c *Cluster) Start(ctx context.Context) error {
	if err := c.alan.Handle(c.prefix, c.handleMsg); err != nil {
		return fmt.Errorf("cluster: handle %q: %w", c.prefix, err)
	}
	if err := c.alan.HandleStream(c.streamType(), c.handleStream); err != nil {
		c.alan.Remove(c.prefix)
		return fmt.Errorf("cluster: handle stream %q: %w", c.streamType(), err)
	}

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

	// On peer join: if we are the leader and the joiner is behind us,
	// push a snapshot immediately so they don't have to wait for the
	// next write or the periodic-sync ticker. This mirrors the pattern
	// in alan's example/sync/main.go.
	c.alan.OnPeerJoin(func(addr *net.UDPAddr) {
		slog.Info("cluster: peer joined", "addr", addr, "peers", c.alan.PeerCount())
		if !c.alan.IsLeader(c.lockKey) {
			return
		}
		go func() {
			// Use a fresh background ctx with a generous bound so a
			// slow joiner doesn't pin a goroutine forever.
			pushCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := c.pushTo(pushCtx, addr, 0); err != nil && !isPeerGone(err) {
				slog.Warn("cluster: initial push to new peer failed", "addr", addr, "error", err)
			}
		}()
	})

	// Clear cached leader address whenever the leader peer disappears, so
	// followers don't keep forwarding writes to a dead node and so the
	// next announce is accepted as authoritative. Lock-release for any
	// locks held by the departing peer is handled inside alan itself.
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

// udpEqual compares two UDP addresses by IP+Port (ignoring Zone differences
// that can appear on link-local IPv6).
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

// NotifySync pushes the latest diff to all followers that are behind, and
// blocks until each one has finished applying it (or ctx is cancelled).
//
// The leader first asks every peer for its version, then in parallel opens
// a stream to each behind peer carrying the incremental backup. The
// underlying [alan.Alan.SendToStream] only returns once the receiver's
// stream handler has read the body to completion, so when this method
// returns nil all reachable followers are caught up. Errors from individual
// peers are joined with [errors.Join]; transient peer-disconnect errors are
// dropped because they will be picked up by the next periodic sync on
// recovery.
//
// If this node is not the leader, NotifySync is a no-op and returns nil.
// There is no internal timeout: pass a deadline-bound ctx if you want one.
func (c *Cluster) NotifySync(ctx context.Context) error {
	if !c.isLeader.Load() {
		return nil
	}
	// If we are leader-on-paper but the cluster has lost quorum (we
	// were partitioned away from the majority), refuse to broadcast.
	// alan will cancel the leader fn ctx shortly after, but external
	// callers may race that cancellation.
	if !c.alan.LeaderHealthy(c.lockKey) {
		return errors.New("cluster: leadership not healthy (quorum lost)")
	}

	myVersion := c.db.Version()

	// Always announce our current version so followers learn our address.
	c.broadcastAnnounce()

	replies, err := c.alan.SendAndWaitReply(ctx, c.prefix, []byte{msgVersionRequest})
	if err != nil && len(replies) == 0 {
		return err
	}

	// For every peer behind us, push the diff in parallel.
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
			if err := c.pushTo(ctx, addr, since); err != nil && !isPeerGone(err) {
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
// returns its response. If this node is the leader, the registered forward
// handler is invoked directly without a network round-trip.
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

	// alan.LeaderLoop:
	//   - blocks inside alan.Lock until this node wins the lock,
	//   - calls runAsLeader while we hold it,
	//   - cancels runAsLeader's ctx if quorum is lost (alan v0.4.1+),
	//   - on fn return, releases the lock and re-enters Lock so we
	//     re-acquire as soon as quorum returns.
	//
	// There is no retry gap: at any moment either someone holds the
	// lock or one of the waiters is about to be granted it.
	_ = c.alan.LeaderLoop(ctx, c.lockKey, time.Second, c.runAsLeader)
}

// runAsLeader is the leader-only main routine. It is invoked by alan once
// this node acquires the lock and runs until ctx is cancelled or it
// returns. Returning releases the lock for another waiter to acquire.
func (c *Cluster) runAsLeader(ctx context.Context) error {
	// We are now the leader. Clear any stale leaderAddr (it would point
	// to the previous leader if we were a follower) so a stale pointer
	// doesn't surface in LeaderAddr() while we hold the lock.
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

	slog.Info("cluster: became leader", "peers", c.alan.PeerCount())

	// Catch up from the peer with the highest version so we don't start
	// serving stale data. Bounded so a slow / stuck peer can't block us
	// from announcing leadership.
	catchCtx, catchCancel := context.WithTimeout(ctx, 30*time.Second)
	c.catchUp(catchCtx)
	catchCancel()

	// Announce ourselves immediately, then re-announce periodically so
	// followers that join late (or whose announce was lost) discover us.
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

// catchUp asks all peers for their version, then pulls an incremental backup
// from whichever peer is furthest ahead.
func (c *Cluster) catchUp(ctx context.Context) {
	myVersion := c.db.Version()

	if c.alan.PeerCount() == 0 {
		return // nothing to catch up from
	}

	replies, err := c.alan.SendAndWaitReply(ctx, c.prefix, []byte{msgVersionRequest})
	if err != nil && len(replies) == 0 {
		slog.Warn("cluster: catch-up version probe failed", "error", err)
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

	slog.Info("cluster: catching up", "peer", bestPeer, "from", myVersion, "to", bestVersion)

	if err := c.pullFrom(ctx, bestPeer); err != nil {
		slog.Error("cluster: catch-up failed", "error", err)
	}
}

// ---------------------------------------------------------------------------
// Sync helpers
// ---------------------------------------------------------------------------

// pushTo opens an outbound stream to addr and writes an incremental backup
// since the given version. SendToStream blocks until the receiver's stream
// handler returns, so this function returning nil means addr has finished
// restoring the diff.
func (c *Cluster) pushTo(ctx context.Context, addr *net.UDPAddr, since uint64) error {
	pr, pw := io.Pipe()

	// Producer: feed the backup into the pipe.
	go func() {
		_, err := c.db.Backup(pw, since, false)
		_ = pw.CloseWithError(err)
	}()

	_, err := c.alan.SendToStream(ctx, addr, c.streamType(), pr)
	// Drain/close the reader in case SendToStream aborted early.
	_ = pr.CloseWithError(err)
	return err
}

// pullFrom asks addr for an incremental backup since our current version.
// The bytes RPC returns once addr has finished pushing the stream (which we
// ingest in handleStream).
func (c *Cluster) pullFrom(ctx context.Context, addr *net.UDPAddr) error {
	version := c.db.Version()

	payload := make([]byte, 9)
	payload[0] = msgPullRequest
	binary.BigEndian.PutUint64(payload[1:], version)

	_, err := c.alan.SendToAndWaitReply(ctx, addr, c.prefix, payload)
	return err
}

// broadcastAnnounce tells followers who the leader is and what version it
// has. Fire-and-forget — followers use this only to learn the leader addr.
func (c *Cluster) broadcastAnnounce() {
	version := c.db.Version()
	payload := make([]byte, 9)
	payload[0] = msgLeaderAnnounce
	binary.BigEndian.PutUint64(payload[1:], version)

	// Use a background ctx with a short deadline so a slow peer cannot
	// block leadership announcements indefinitely.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.alan.Send(ctx, c.prefix, payload)
}

// periodicSync runs on followers and pulls from the leader every syncInterval
// as a safety net (in case a push was missed).
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

			if err := c.pullFrom(ctx, addr); err != nil {
				if isPeerGone(err) {
					slog.Debug("cluster: leader unreachable, clearing address", "addr", addr)
					c.leaderAddr.CompareAndSwap(addr, nil)
				} else {
					slog.Error("cluster: periodic sync failed", "error", err)
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

// handleStream ingests an incoming push: the body is an incremental backup
// produced by the sender's bw.DB.Backup, which we feed straight into
// bw.DB.Restore. When this function returns, the sender's SendToStream
// unblocks — that is the cluster's "sync done" handshake.
func (c *Cluster) handleStream(ctx context.Context, msg alan.Message, body io.Reader) error {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()

	before := c.db.Version()
	if err := c.db.Restore(body); err != nil {
		slog.Error("cluster: restore from stream failed", "from", msg.Addr, "error", err)
		return err
	}
	after := c.db.Version()
	if after == before {
		slog.Info("cluster: sync complete (no change)", "from", msg.Addr, "version", after)
	} else {
		slog.Info("cluster: sync complete", "from", msg.Addr, "from_version", before, "to_version", after)
	}
	return nil
}

// onLeaderAnnounce records the leader's address. Followers no longer need
// to react by pulling — the leader pushes directly via NotifySync. We
// consult alan.IsLeader as the authoritative source rather than our local
// mirror, in case our mirror hasn't been updated yet.
func (c *Cluster) onLeaderAnnounce(msg alan.Message) {
	if c.alan.IsLeader(c.lockKey) {
		return
	}
	c.leaderAddr.Store(msg.Addr)
}

// onPullRequest serves an incremental backup since the requested version by
// pushing it back as a stream. The bytes reply is sent only after the
// stream completes, which signals to the requester that the sync is done.
//
// Any node can serve this — that allows a newly elected leader to catch up
// from any peer.
func (c *Cluster) onPullRequest(ctx context.Context, msg alan.Message) {
	if !msg.IsRequest() {
		return
	}

	if len(msg.Data) < 9 {
		_, _ = c.alan.Reply(ctx, msg, nil)
		return
	}

	since := binary.BigEndian.Uint64(msg.Data[1:9])

	if since >= c.db.Version() {
		_, _ = c.alan.Reply(ctx, msg, nil) // already up to date
		return
	}

	// Push the diff back as a stream and wait for it to drain.
	if err := c.pushTo(ctx, msg.Addr, since); err != nil {
		slog.Error("cluster: pull-response push failed", "to", msg.Addr, "error", err)
	}

	_, _ = c.alan.Reply(ctx, msg, []byte{1})
}

// onVersionRequest replies with this node's current database version.
func (c *Cluster) onVersionRequest(ctx context.Context, msg alan.Message) {
	if !msg.IsRequest() {
		return
	}

	resp := make([]byte, 8)
	binary.BigEndian.PutUint64(resp, c.db.Version())
	_, _ = c.alan.Reply(ctx, msg, resp)
}

// onForward handles a forwarded application request on the leader.
func (c *Cluster) onForward(ctx context.Context, msg alan.Message) {
	if !msg.IsRequest() {
		return
	}

	// Use alan.IsLeader as authority: our local mirror may briefly lag
	// during step-down/step-up. Also reject when leadership is unhealthy
	// so a partitioned leader doesn't accept writes that won't replicate.
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
