// Package cluster provides distributed synchronization for bw databases
// using the [alan] UDP peer discovery library.
//
// # Architecture
//
// One node in the cluster holds the "leader" lock (via alan's distributed
// lock). Only the leader writes to the database; every other node is a
// read-only follower that replicates data by pulling incremental backups.
//
// When the leader goes down, another node acquires the lock automatically
// and becomes the new leader. Before accepting writes it catches up from
// the peer with the highest version, so no acknowledged data is lost
// (assuming at least one follower received the last sync).
//
// # Reads and writes
//
//   - Reads are always local. Every node (leader or follower) can serve
//     reads from its own copy of the database without any network round-trip.
//   - Writes must go through the leader. The application checks [Cluster.IsLeader]
//     and either writes locally (if leader) or forwards the request to the
//     leader via its own transport (HTTP, gRPC, etc.).
//   - After a successful write the leader calls [Cluster.NotifySync] to
//     broadcast a sync notification. Followers that are behind automatically
//     pull the incremental diff and apply it.
//
// # Sync protocol
//
// Every message on the wire is prefixed with a namespace string (default
// "bw", configurable via [WithPrefix]). This ensures that if the same
// alan instance is shared with other subsystems, messages do not collide.
//
// After the prefix, three message types are distinguished by tag byte:
//
//	Tag   Name            Direction       Payload
//	0x01  SyncNotify      leader -> all   [version:8]         (fire-and-forget)
//	0x02  SyncRequest     any   -> any    [since:8] -> backup (request-reply)
//	0x03  VersionRequest  any   -> any    (empty)   -> [v:8]  (request-reply)
//	0x04  Forward         any   -> leader [app...] -> [resp]  (request-reply)
//
// SyncRequest can be served by any node. This allows a newly elected
// leader to catch up from whichever follower has the highest version.
//
// Followers also run a periodic sync loop (default every 5 minutes,
// configurable via [WithSyncInterval]) as a safety net in case a
// SyncNotify broadcast was missed.
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
//	    c.NotifySync() // tell followers to pull the diff
//	}
//
// # Forwarding writes (application-level)
//
// The cluster package provides a built-in [Cluster.Forward] helper that
// sends an application request to the leader over alan and returns the
// response. This eliminates the need for a separate HTTP/gRPC forwarding
// layer for simple use cases:
//
//	c := cluster.New(db, a,
//	    cluster.WithForwardHandler(func(ctx context.Context, data []byte) []byte {
//	        // Unmarshal data, write to DB, notify sync, marshal response.
//	        var req CreateUserRequest
//	        json.Unmarshal(data, &req)
//	        _ = users.Insert(ctx, &User{ID: req.ID, Name: req.Name})
//	        c.NotifySync()
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
// For high-throughput or large-payload writes that exceed UDP limits,
// you can still use your own HTTP/gRPC transport with [Cluster.IsLeader]
// and [Cluster.LeaderAddr] as before.
//
// # Leader change callback
//
// Use [WithOnLeaderChange] to react when this node gains or loses
// leadership (e.g. drain in-flight writes, update a readiness probe):
//
//	cluster.New(db, a, cluster.WithOnLeaderChange(func(isLeader bool) {
//	    if isLeader {
//	        log.Println("I am the leader now")
//	    } else {
//	        log.Println("stepped down")
//	    }
//	}))
//
// # Limitations
//
//   - As of alan v0.3.0, data is transferred over QUIC so there is no
//     payload size limit. Incremental diffs and forwarded requests of any
//     size are handled transparently.
//   - There is no automatic write-forwarding. The application must route
//     writes to the leader using its own transport.
//   - Consistency is eventual. A follower may serve slightly stale reads
//     between a leader write and the next sync pull.
//
// [alan]: https://github.com/rakunlabs/alan
package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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

// Internal message type tags (first byte of the alan payload).
const (
	msgSyncNotify     byte = 0x01
	msgSyncRequest    byte = 0x02
	msgVersionRequest byte = 0x03
	msgForward        byte = 0x04
)

// Cluster wraps a bw.DB and an alan.Alan instance to form a replicated cluster.
type Cluster struct {
	db   *bw.DB
	alan *alan.Alan

	lockKey      string
	syncInterval time.Duration
	prefix       []byte // message namespace prefix to avoid collisions

	isLeader   atomic.Bool
	leaderAddr atomic.Pointer[net.UDPAddr]

	// syncMu prevents concurrent sync operations on this node.
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

// WithSyncInterval sets how often followers proactively pull from the leader.
// Default: 5 minutes.
func WithSyncInterval(d time.Duration) Option {
	return func(c *Cluster) { c.syncInterval = d }
}

// WithPrefix sets the message namespace prefix. All messages sent by this
// cluster are prefixed with these bytes, and incoming messages without the
// prefix are silently ignored. This allows multiple independent users of the
// same alan instance to coexist without message collisions.
// Default: "bw".
func WithPrefix(prefix string) Option {
	return func(c *Cluster) { c.prefix = []byte(prefix) }
}

// WithOnLeaderChange registers a callback invoked when this node gains or
// loses leadership. It is called with true when becoming leader and false
// when stepping down.
func WithOnLeaderChange(fn func(isLeader bool)) Option {
	return func(c *Cluster) { c.onLeaderChange = fn }
}

// WithForwardHandler registers a function that the leader uses to process
// requests forwarded by followers via [Cluster.Forward]. The handler receives
// the raw application payload (without any framing) and returns a response
// that is sent back to the caller.
//
// This is the server side of the forward mechanism. Without a handler,
// forwarded requests receive an empty reply.
func WithForwardHandler(fn func(ctx context.Context, data []byte) []byte) Option {
	return func(c *Cluster) { c.forwardHandler = fn }
}

// New creates a new Cluster. The alan.Alan instance should NOT be started yet;
// Start will call alan.Start internally.
func New(db *bw.DB, a *alan.Alan, opts ...Option) *Cluster {
	c := &Cluster{
		db:           db,
		alan:         a,
		lockKey:      "bw-leader",
		syncInterval: 5 * time.Minute,
		prefix:       []byte("bw"),
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
func (c *Cluster) IsLeader() bool { return c.isLeader.Load() }

// LeaderAddr returns the last-known leader address, or nil if unknown.
func (c *Cluster) LeaderAddr() *net.UDPAddr { return c.leaderAddr.Load() }

// Start initializes the cluster: starts alan, begins leader election, and
// launches the periodic sync loop. It returns once alan is ready to
// send/receive. The background goroutines run until ctx is cancelled or
// Stop is called.
func (c *Cluster) Start(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.alan.Start(ctx, c.handleMessage)
	}()

	// Wait for alan to be ready or fail.
	select {
	case <-c.alan.Ready():
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}

	go c.leaderLoop(ctx)
	go c.periodicSync(ctx)

	return nil
}

// Stop gracefully shuts down the cluster and the underlying alan instance.
func (c *Cluster) Stop() { c.alan.Stop() }

// NotifySync broadcasts a sync notification to all followers. Call this after
// writing to the database while this node is the leader.
func (c *Cluster) NotifySync() {
	if !c.isLeader.Load() {
		return
	}
	c.broadcastVersion()
}

// Forward sends an application-level request to the current leader and returns
// its response. If this node is already the leader, the forward handler is
// invoked directly without a network round-trip.
//
// This is useful for write operations: followers can forward the request to
// the leader transparently, so the caller doesn't need to know which node is
// the leader or manage its own forwarding transport.
//
// A forward handler must be registered via [WithForwardHandler]; otherwise the
// leader will return an empty response.
//
// Example:
//
//	// On all nodes:
//	c := cluster.New(db, a,
//	    cluster.WithForwardHandler(func(ctx context.Context, data []byte) []byte {
//	        // Unmarshal data, perform the write, call c.NotifySync(), marshal response.
//	        return response
//	    }),
//	)
//
//	// When handling a user request (any node):
//	resp, err := c.Forward(ctx, requestBytes)
func (c *Cluster) Forward(ctx context.Context, data []byte) ([]byte, error) {
	// If we are the leader, handle locally.
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

	reply, err := c.alan.SendToAndWaitReply(ctx, addr, c.wrapMsg(payload))
	if err != nil {
		return nil, err
	}

	return reply.Data, nil
}

// ---------------------------------------------------------------------------
// Leader election
// ---------------------------------------------------------------------------

func (c *Cluster) leaderLoop(ctx context.Context) {
	_ = c.alan.LeaderLoop(ctx, c.lockKey, time.Second, func(ctx context.Context) error {
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

		slog.Info("cluster: became leader")

		// Catch up from the peer with the highest version so we don't
		// start serving stale data.
		c.catchUp(ctx)

		// Announce ourselves so followers learn our address and version.
		c.broadcastVersion()

		<-ctx.Done()
		return ctx.Err()
	})
}

// catchUp asks all peers for their version, then pulls an incremental backup
// from whichever peer is furthest ahead.
func (c *Cluster) catchUp(ctx context.Context) {
	myVersion := c.db.Version()

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	replies, err := c.alan.SendAndWaitReply(reqCtx, c.wrapMsg([]byte{msgVersionRequest}))
	if err != nil && len(replies) == 0 {
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

	if err := c.requestSyncFrom(ctx, bestPeer); err != nil {
		slog.Error("cluster: catch-up failed", "error", err)
	}
}

// ---------------------------------------------------------------------------
// Sync helpers
// ---------------------------------------------------------------------------

// requestSyncFrom sends an incremental backup request to addr and loads the
// response into the local database.
func (c *Cluster) requestSyncFrom(ctx context.Context, addr *net.UDPAddr) error {
	if !c.syncMu.TryLock() {
		return nil // another sync is already running
	}
	defer c.syncMu.Unlock()

	version := c.db.Version()

	payload := make([]byte, 9)
	payload[0] = msgSyncRequest
	binary.BigEndian.PutUint64(payload[1:], version)

	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	reply, err := c.alan.SendToAndWaitReply(reqCtx, addr, c.wrapMsg(payload))
	if err != nil {
		return err
	}

	if len(reply.Data) == 0 {
		return nil // nothing new
	}

	if err := c.db.Restore(bytes.NewReader(reply.Data)); err != nil {
		return err
	}

	slog.Info("cluster: synced", "from", addr, "bytes", len(reply.Data))
	return nil
}

func (c *Cluster) broadcastVersion() {
	version := c.db.Version()
	payload := make([]byte, 9)
	payload[0] = msgSyncNotify
	binary.BigEndian.PutUint64(payload[1:], version)
	c.alan.Send(c.wrapMsg(payload))
}

// periodicSync runs on followers and pulls from the leader every syncInterval.
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

			if err := c.requestSyncFrom(ctx, addr); err != nil {
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

// ---------------------------------------------------------------------------
// Message framing — prefix + tag
// ---------------------------------------------------------------------------

// wrapMsg prepends the namespace prefix to a message payload.
func (c *Cluster) wrapMsg(payload []byte) []byte {
	out := make([]byte, len(c.prefix)+len(payload))
	copy(out, c.prefix)
	copy(out[len(c.prefix):], payload)
	return out
}

// unwrapMsg checks the prefix and strips it, returning the inner payload.
// Returns nil if the prefix does not match.
func (c *Cluster) unwrapMsg(data []byte) []byte {
	if len(data) < len(c.prefix) {
		return nil
	}
	if !bytes.Equal(data[:len(c.prefix)], c.prefix) {
		return nil
	}
	return data[len(c.prefix):]
}

// isPeerGone returns true if the error indicates the peer has left the cluster
// (disconnected, no connection, etc.). These are transient and expected during
// leader failover.
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
// Message handler
// ---------------------------------------------------------------------------

func (c *Cluster) handleMessage(ctx context.Context, msg alan.Message) {
	inner := c.unwrapMsg(msg.Data)
	if len(inner) == 0 {
		return
	}

	// Replace the raw data with the unwrapped inner payload for handlers.
	msg.Data = inner

	switch inner[0] {
	case msgSyncNotify:
		c.onSyncNotify(ctx, msg)
	case msgSyncRequest:
		c.onSyncRequest(msg)
	case msgVersionRequest:
		c.onVersionRequest(msg)
	case msgForward:
		c.onForward(ctx, msg)
	}
}

// onSyncNotify is received by followers when the leader has new data.
func (c *Cluster) onSyncNotify(ctx context.Context, msg alan.Message) {
	if c.isLeader.Load() {
		return
	}

	// Remember the leader address.
	c.leaderAddr.Store(msg.Addr)

	if len(msg.Data) < 9 {
		return
	}

	leaderVersion := binary.BigEndian.Uint64(msg.Data[1:9])

	if leaderVersion <= c.db.Version() {
		return // already up to date
	}

	go func() {
		if err := c.requestSyncFrom(ctx, msg.Addr); err != nil {
			if isPeerGone(err) {
				slog.Debug("cluster: leader disconnected", "addr", msg.Addr)
				c.leaderAddr.CompareAndSwap(msg.Addr, nil)
			} else {
				slog.Error("cluster: sync from leader failed", "error", err)
			}
		}
	}()
}

// onSyncRequest replies with an incremental backup since the requested version.
// Any node can serve this (not just the leader), which allows a newly elected
// leader to catch up from any peer.
func (c *Cluster) onSyncRequest(msg alan.Message) {
	if !msg.IsRequest() {
		return
	}

	if len(msg.Data) < 9 {
		c.alan.Reply(msg, nil)
		return
	}

	since := binary.BigEndian.Uint64(msg.Data[1:9])

	// Already up to date — no need to run a backup.
	if since >= c.db.Version() {
		c.alan.Reply(msg, nil)
		return
	}

	var buf bytes.Buffer
	if _, err := c.db.Backup(&buf, since, false); err != nil {
		slog.Error("cluster: backup for sync request failed", "error", err)
		c.alan.Reply(msg, nil)
		return
	}

	c.alan.Reply(msg, buf.Bytes())
}

// onVersionRequest replies with this node's current database version.
func (c *Cluster) onVersionRequest(msg alan.Message) {
	if !msg.IsRequest() {
		return
	}

	resp := make([]byte, 8)
	binary.BigEndian.PutUint64(resp, c.db.Version())
	c.alan.Reply(msg, resp)
}

// onForward handles a forwarded application request on the leader.
func (c *Cluster) onForward(ctx context.Context, msg alan.Message) {
	if !msg.IsRequest() {
		return
	}

	if c.forwardHandler == nil || !c.isLeader.Load() {
		c.alan.Reply(msg, nil)
		return
	}

	// msg.Data[0] is the tag byte; the rest is the application payload.
	var appData []byte
	if len(msg.Data) > 1 {
		appData = msg.Data[1:]
	}

	resp := c.forwardHandler(ctx, appData)
	c.alan.Reply(msg, resp)
}
