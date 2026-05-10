package bw

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/rakunlabs/bw/schema"
)

// Vector storage layout (all keys live under \x00vec\x00<bucket>\x00<field>\x00):
//
//   v\x00<pk>     raw float32 LE bytes (one per non-deleted record)
//   m\x00         manifest: dim, metric, count
//   x\x00<pk>     soft-delete tombstone (1 byte presence)
//
// The search path is HNSW: an in-graph greedy descent through skip-
// list-style upper levels, then ef-bounded best-first search on level
// 0. Inserts are ANN-aware too, building the graph one node at a
// time. For very small collections (or while a graph is still
// degenerate, e.g. fewer nodes than ef) the search transparently
// falls back to brute force, which is exact and faster at that
// scale.
//
// Atomicity: every write goes through the caller's *badger.Txn — a
// rollback erases the vector AND its graph edges, a committed insert
// makes the new node searchable in the same instant the data record
// becomes visible. Backup/Restore are automatically portable because
// the entire vector state (including the graph) is plain Badger
// keys.

// VectorMetric selects the distance function used at search time.
// Higher Score in a SearchVectorHit always means "more similar":
// cosine and dot are returned as-is; L2 is negated so the heap
// invariant matches the other metrics.
type VectorMetric uint8

const (
	// MetricDefault tells SearchVector to use the metric configured
	// in the field's schema tag (or Cosine if none was set).
	MetricDefault VectorMetric = 0
	// Cosine similarity, expects normalised vectors for best
	// numerical behaviour.
	Cosine VectorMetric = 1
	// DotProduct similarity. Equivalent to Cosine when both vectors
	// are unit-normalised; faster otherwise (no magnitudes).
	DotProduct VectorMetric = 2
	// Euclidean (L2) distance; returned as -|a-b| so the highest
	// score is still the most similar.
	Euclidean VectorMetric = 3
)

func (m VectorMetric) String() string {
	switch m {
	case Cosine:
		return "cosine"
	case DotProduct:
		return "dot"
	case Euclidean:
		return "l2"
	default:
		return "default"
	}
}

func parseMetricName(s string) VectorMetric {
	switch s {
	case "cosine", "":
		return Cosine
	case "dot":
		return DotProduct
	case "l2":
		return Euclidean
	default:
		return Cosine
	}
}

// VectorHit is one result returned by Bucket.SearchVector.
type VectorHit[T any] struct {
	Record *T
	Score  float64
}

// SearchVectorOptions tunes a single SearchVector call.
//
// Filter is wired in by Bucket.SearchVector — it pre-resolves the
// query into a pk allow-set and hands the set to the vector index, so
// vectorIndex itself stays free of any query-engine dependency.
type SearchVectorOptions struct {
	// K is the number of hits to return. Zero means 10.
	K int
	// EfSearch tunes HNSW recall vs latency: bigger = higher
	// recall, slower. Zero defaults to 100 (or to K, whichever is
	// larger). Has no effect on the brute-force fallback path.
	EfSearch int
	// Metric overrides the field's default metric. Zero (MetricDefault)
	// keeps the schema setting.
	Metric VectorMetric
	// Filter, when non-nil, restricts the candidate set. The bucket
	// resolves the query into matching pks via the standard Find
	// machinery before the vector pass runs. Type is *any so the
	// vector module doesn't import query directly; bucket.go casts.
	Filter any
}

// vectorIndex bundles the per-bucket vector configuration. The handle
// holds no live OS resources; every read and write is a Badger op.
type vectorIndex struct {
	bucket   string
	field    *schema.Field
	defaultM VectorMetric

	// HNSW knobs. Zero means "use the manifest value, or the
	// hard-coded default if the manifest also has zero". Set via
	// WithVectorParams at registration time; once a graph contains
	// nodes, M is effectively immutable (changing it would orphan
	// existing neighbour lists).
	wantM              int
	wantEfConstruction int

	// rng feeds the geometric distribution that picks each new
	// node's max level. Per-index instance so tests can install a
	// deterministic seed via WithVectorSeed.
	rngMu sync.Mutex
	rng   *vecRand

	mu sync.Mutex
	// dim caches the manifest dimension after the first read or
	// successful write. Zero means "not yet locked in"; the next
	// write will set it.
	dim int
}

// vecRand is a tiny deterministic LCG so HNSW level selection is
// reproducible under test seeds. The math/rand package would do, but
// we want zero ambient state and no dependence on Go's global rng.
type vecRand struct {
	state uint64
}

func newVecRand(seed uint64) *vecRand {
	if seed == 0 {
		seed = 0x9E3779B97F4A7C15 // SplitMix64 default
	}
	return &vecRand{state: seed}
}

// next returns a uniform float64 in [0, 1).
func (r *vecRand) next() float64 {
	r.state = r.state*6364136223846793005 + 1442695040888963407
	// Top 53 bits → float64 mantissa
	return float64(r.state>>11) / (1 << 53)
}

// randomLevel draws an HNSW max level from the geometric distribution
// with parameter p = 1/M. levelMult = 1/ln(M). The cap is purely
// defensive; higher levels are exponentially unlikely.
func (vi *vectorIndex) randomLevel(M int) uint8 {
	vi.rngMu.Lock()
	defer vi.rngMu.Unlock()
	r := vi.rng.next()
	// Avoid log(0) - SplitMix64 cannot return exactly 0 in 53-bit
	// representation, but be defensive.
	if r <= 0 {
		return 0
	}
	mult := 1.0 / math.Log(float64(M))
	lvl := math.Floor(-math.Log(r) * mult)
	if lvl < 0 {
		return 0
	}
	if lvl > 31 {
		return 31
	}
	return uint8(lvl)
}

// vectorRegistry tracks per-bucket vector handles. Mirrors the FTS
// registry purely for symmetry; a vector handle is just config.
type vectorRegistry struct {
	mu      sync.RWMutex
	indexes map[string]*vectorIndex
}

func newVectorRegistry() *vectorRegistry {
	return &vectorRegistry{indexes: make(map[string]*vectorIndex)}
}

func (r *vectorRegistry) get(bucket string) *vectorIndex {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.indexes[bucket]
}

func (r *vectorRegistry) set(bucket string, idx *vectorIndex) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.indexes[bucket] = idx
}

func (r *vectorRegistry) closeAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.indexes = make(map[string]*vectorIndex)
}

// openVectorIndex returns the vector handle for a bucket. Stage A only
// supports a single vector field per bucket — VectorFields()[0] is
// taken. If multi-vector becomes a use case, this becomes a slice.
func openVectorIndex(_ *DB, bucket string, fields []*schema.Field) (*vectorIndex, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	if len(fields) > 1 {
		return nil, fmt.Errorf("bw: bucket %q has %d vector fields; only one is supported", bucket, len(fields))
	}
	f := fields[0]
	if f.Type.Kind() != reflect.Slice || f.Type.Elem().Kind() != reflect.Float32 {
		return nil, fmt.Errorf("bw: vector field %q must be []float32", f.Name)
	}
	vi := &vectorIndex{
		bucket:   bucket,
		field:    f,
		defaultM: parseMetricName(f.VectorMetric),
		dim:      f.VectorDim,
		rng:      newVecRand(0),
	}
	return vi, nil
}

// HNSW defaults. Conservative; bigger M trades insert latency for
// recall. Override via WithVectorParams.
const (
	defaultHNSWM              = 16
	defaultHNSWEfConstruction = 200
	defaultHNSWEfSearch       = 100
)

// effectiveM returns the M to use, falling back through caller > manifest > default.
func (vi *vectorIndex) effectiveM(man vectorManifest) int {
	if vi.wantM > 0 {
		return vi.wantM
	}
	if man.M > 0 {
		return int(man.M)
	}
	return defaultHNSWM
}

func (vi *vectorIndex) effectiveEfConstruction(man vectorManifest) int {
	if vi.wantEfConstruction > 0 {
		return vi.wantEfConstruction
	}
	if man.EfConstruction > 0 {
		return int(man.EfConstruction)
	}
	return defaultHNSWEfConstruction
}

// ---------------------------------------------------------------------------
// Manifest
// ---------------------------------------------------------------------------

// vectorManifest is the persisted per-(bucket,field) configuration.
// Encoded as concatenated uvarints so adding new fields is a
// non-breaking change: older manifests just leave the trailing fields
// at their zero value, which the reader treats as "use defaults".
//
// Field order is append-only:
//
//	Dim, Metric, Count, M, EfConstruction, MaxLevel
type vectorManifest struct {
	Dim            uint64
	Metric         uint64 // matches VectorMetric numeric values
	Count          uint64
	M              uint64 // graph degree (HNSW M); 0 -> default 16
	EfConstruction uint64 // HNSW efConstruction; 0 -> default 200
	MaxLevel       uint64 // highest level observed in the graph (entry point's level)
}

func encodeManifest(m vectorManifest) []byte {
	var buf [6 * binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], m.Dim)
	n += binary.PutUvarint(buf[n:], m.Metric)
	n += binary.PutUvarint(buf[n:], m.Count)
	n += binary.PutUvarint(buf[n:], m.M)
	n += binary.PutUvarint(buf[n:], m.EfConstruction)
	n += binary.PutUvarint(buf[n:], m.MaxLevel)
	return append([]byte(nil), buf[:n]...)
}

func decodeManifest(b []byte) (vectorManifest, error) {
	var m vectorManifest
	rest := b
	v, n := binary.Uvarint(rest)
	if n <= 0 {
		return m, fmt.Errorf("manifest dim varint")
	}
	m.Dim = v
	rest = rest[n:]
	v, n = binary.Uvarint(rest)
	if n <= 0 {
		return m, fmt.Errorf("manifest metric varint")
	}
	m.Metric = v
	rest = rest[n:]
	v, n = binary.Uvarint(rest)
	if n <= 0 {
		return m, nil
	}
	m.Count = v
	rest = rest[n:]
	v, n = binary.Uvarint(rest)
	if n <= 0 {
		return m, nil
	}
	m.M = v
	rest = rest[n:]
	v, n = binary.Uvarint(rest)
	if n <= 0 {
		return m, nil
	}
	m.EfConstruction = v
	rest = rest[n:]
	v, n = binary.Uvarint(rest)
	if n <= 0 {
		return m, nil
	}
	m.MaxLevel = v
	return m, nil
}

// readManifest loads the current manifest for vi inside the supplied
// txn. Returns a zero-valued manifest when the key is absent (first
// insert).
func (vi *vectorIndex) readManifest(btx *badger.Txn) (vectorManifest, error) {
	item, err := btx.Get(vecManifestKey(vi.bucket, vi.field.Name))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return vectorManifest{}, nil
	}
	if err != nil {
		return vectorManifest{}, err
	}
	var m vectorManifest
	err = item.Value(func(val []byte) error {
		var derr error
		m, derr = decodeManifest(val)
		return derr
	})
	return m, err
}

func (vi *vectorIndex) writeManifest(btx *badger.Txn, m vectorManifest) error {
	return btx.Set(vecManifestKey(vi.bucket, vi.field.Name), encodeManifest(m))
}

// ---------------------------------------------------------------------------
// Graph storage helpers
// ---------------------------------------------------------------------------

// readEntry returns the current entry-point (pk, level) or (nil, 0)
// when the graph is empty.
func (vi *vectorIndex) readEntry(btx *badger.Txn) ([]byte, uint8, error) {
	item, err := btx.Get(vecEntryKey(vi.bucket, vi.field.Name))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	var pk []byte
	var lvl uint8
	err = item.Value(func(val []byte) error {
		if len(val) < 1 {
			return fmt.Errorf("bw: vector entry value too short")
		}
		lvl = val[0]
		rest := val[1:]
		l, n := binary.Uvarint(rest)
		if n <= 0 || uint64(len(rest)-n) < l {
			return fmt.Errorf("bw: vector entry pk decode")
		}
		pk = append([]byte(nil), rest[n:n+int(l)]...)
		return nil
	})
	return pk, lvl, err
}

func (vi *vectorIndex) writeEntry(btx *badger.Txn, pk []byte, level uint8) error {
	val := make([]byte, 0, 1+binary.MaxVarintLen64+len(pk))
	val = append(val, level)
	val = appendLP(val, pk)
	return btx.Set(vecEntryKey(vi.bucket, vi.field.Name), val)
}

// readLevel returns the per-pk max level. Missing key -> 0 (level 0
// only); not an error since pre-Stage-B vectors carry no level marker.
func (vi *vectorIndex) readLevel(btx *badger.Txn, pk []byte) (uint8, error) {
	item, err := btx.Get(vecLevelKey(vi.bucket, vi.field.Name, pk))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var lvl uint8
	err = item.Value(func(val []byte) error {
		if len(val) < 1 {
			return fmt.Errorf("bw: vector level value too short")
		}
		lvl = val[0]
		return nil
	})
	return lvl, err
}

func (vi *vectorIndex) writeLevel(btx *badger.Txn, pk []byte, level uint8) error {
	return btx.Set(vecLevelKey(vi.bucket, vi.field.Name, pk), []byte{level})
}

// readNeighbours returns the neighbour pks of pk at the given level.
// Layout in value: varint(count), then count repetitions of lp(neighbourPk).
func (vi *vectorIndex) readNeighbours(btx *badger.Txn, pk []byte, level uint8) ([][]byte, error) {
	item, err := btx.Get(vecNeighKey(vi.bucket, vi.field.Name, level, pk))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out [][]byte
	err = item.Value(func(val []byte) error {
		count, n := binary.Uvarint(val)
		if n <= 0 {
			return fmt.Errorf("bw: vector neigh count varint")
		}
		rest := val[n:]
		out = make([][]byte, 0, count)
		for i := uint64(0); i < count; i++ {
			l, m := binary.Uvarint(rest)
			if m <= 0 || uint64(len(rest)-m) < l {
				return fmt.Errorf("bw: vector neigh pk decode")
			}
			out = append(out, append([]byte(nil), rest[m:m+int(l)]...))
			rest = rest[m+int(l):]
		}
		return nil
	})
	return out, err
}

func (vi *vectorIndex) writeNeighbours(btx *badger.Txn, pk []byte, level uint8, ns [][]byte) error {
	var buf []byte
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], uint64(len(ns)))
	buf = append(buf, hdr[:n]...)
	for _, p := range ns {
		buf = appendLP(buf, p)
	}
	return btx.Set(vecNeighKey(vi.bucket, vi.field.Name, level, pk), buf)
}

func (vi *vectorIndex) deleteNeighbours(btx *badger.Txn, pk []byte, level uint8) error {
	return btx.Delete(vecNeighKey(vi.bucket, vi.field.Name, level, pk))
}

// readVec loads the raw vector bytes for pk and decodes them.
func (vi *vectorIndex) readVec(btx *badger.Txn, pk []byte) ([]float32, error) {
	item, err := btx.Get(vecRawKey(vi.bucket, vi.field.Name, pk))
	if err != nil {
		return nil, err
	}
	var out []float32
	err = item.Value(func(val []byte) error {
		v, derr := decodeVector(val)
		if derr != nil {
			return derr
		}
		out = v
		return nil
	})
	return out, err
}

// isTombstoned reports whether pk has a soft-delete marker.
func (vi *vectorIndex) isTombstoned(btx *badger.Txn, pk []byte) (bool, error) {
	_, err := btx.Get(vecTombKey(vi.bucket, vi.field.Name, pk))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ---------------------------------------------------------------------------
// Vector encoding
// ---------------------------------------------------------------------------

// encodeVector serialises a []float32 to little-endian bytes. We use a
// fixed wire format rather than gob/msgpack so the bytes are stable
// across machines and Go releases.
func encodeVector(v []float32) []byte {
	out := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(f))
	}
	return out
}

// decodeVector parses a little-endian float32 sequence back into a
// freshly allocated slice.
func decodeVector(b []byte) ([]float32, error) {
	if len(b)%4 != 0 {
		return nil, fmt.Errorf("bw: vector bytes %% 4 != 0 (got %d)", len(b))
	}
	out := make([]float32, len(b)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Update path: writeVec / deleteVec — called from upsertTx / DeleteTx.
// ---------------------------------------------------------------------------

// writeVec stores v for pk inside btx and links the node into the
// HNSW graph (Stage B). Validates the dimension against the manifest
// (auto-locking it on the first write). Re-inserting the same pk
// removes the old node from the graph first so neighbour lists are
// not left pointing at stale geometry.
func (vi *vectorIndex) writeVec(btx *badger.Txn, pk []byte, v []float32) error {
	if len(v) == 0 {
		return ErrVectorEmpty
	}

	man, err := vi.readManifest(btx)
	if err != nil {
		return err
	}

	// First insert: lock the dim and HNSW knobs into the manifest.
	if man.Dim == 0 {
		if vi.field.VectorDim > 0 && vi.field.VectorDim != len(v) {
			return fmt.Errorf("%w: schema declares dim=%d, got %d",
				ErrDimMismatch, vi.field.VectorDim, len(v))
		}
		man.Dim = uint64(len(v))
		man.Metric = uint64(vi.defaultM)
		if vi.wantM > 0 {
			man.M = uint64(vi.wantM)
		}
		if vi.wantEfConstruction > 0 {
			man.EfConstruction = uint64(vi.wantEfConstruction)
		}
	} else if int(man.Dim) != len(v) {
		return fmt.Errorf("%w: bucket dim=%d, got %d", ErrDimMismatch, man.Dim, len(v))
	}

	rawKey := vecRawKey(vi.bucket, vi.field.Name, pk)

	// Detect overwrite-of-live so we can decommission the old graph
	// node before installing the new one. Tombstoned vectors carry
	// no graph state; treat them as "not present".
	hadOld := false
	if _, gErr := btx.Get(rawKey); gErr == nil {
		hadOld = true
	} else if !errors.Is(gErr, badger.ErrKeyNotFound) {
		return gErr
	}

	if hadOld {
		if err := vi.removeFromGraph(btx, pk, man); err != nil {
			return err
		}
		// removeFromGraph decremented Count; we increment again
		// below as if this were a fresh insert.
		man.Count--
	}

	// A reviving overwrite clears any stale tombstone.
	if err := btx.Delete(vecTombKey(vi.bucket, vi.field.Name, pk)); err != nil {
		return err
	}
	if err := btx.Set(rawKey, encodeVector(v)); err != nil {
		return err
	}

	// Insert into HNSW graph.
	newMan, err := vi.insertGraph(btx, pk, v, man)
	if err != nil {
		return err
	}
	man = newMan
	man.Count++

	if err := vi.writeManifest(btx, man); err != nil {
		return err
	}
	vi.mu.Lock()
	vi.dim = int(man.Dim)
	vi.mu.Unlock()
	return nil
}

// deleteVec drops the live vector and tombstones the pk so the
// remaining graph still resolves. Neighbour lists pointing at the
// tombstoned node stay intact: the search path skips tombstoned hits,
// so the graph topology degrades gracefully rather than tearing at
// the moment of deletion. A future compaction step (Bucket.CompactVectors)
// will trim those edges.
func (vi *vectorIndex) deleteVec(btx *badger.Txn, pk []byte) error {
	rawKey := vecRawKey(vi.bucket, vi.field.Name, pk)
	had := false
	if _, err := btx.Get(rawKey); err == nil {
		had = true
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}
	if !had {
		return nil
	}

	if err := btx.Delete(rawKey); err != nil {
		return err
	}
	if err := btx.Set(vecTombKey(vi.bucket, vi.field.Name, pk), []byte{1}); err != nil {
		return err
	}

	man, err := vi.readManifest(btx)
	if err != nil {
		return err
	}
	if man.Count > 0 {
		man.Count--
	}
	return vi.writeManifest(btx, man)
}

// removeFromGraph drops every neighbour-list entry and the level key
// for pk. It does not touch the entry-point key or count; callers
// adjust those as part of the larger update.
func (vi *vectorIndex) removeFromGraph(btx *badger.Txn, pk []byte, man vectorManifest) error {
	level, err := vi.readLevel(btx, pk)
	if err != nil {
		return err
	}
	for L := uint8(0); L <= level; L++ {
		neighs, err := vi.readNeighbours(btx, pk, L)
		if err != nil {
			return err
		}
		// Drop the back-edge from each neighbour pointing at us.
		for _, np := range neighs {
			rev, err := vi.readNeighbours(btx, np, L)
			if err != nil {
				return err
			}
			pruned := rev[:0]
			for _, x := range rev {
				if !vecBytesEqual(x, pk) {
					pruned = append(pruned, x)
				}
			}
			if len(pruned) == 0 {
				if err := vi.deleteNeighbours(btx, np, L); err != nil {
					return err
				}
			} else if err := vi.writeNeighbours(btx, np, L, pruned); err != nil {
				return err
			}
		}
		if err := vi.deleteNeighbours(btx, pk, L); err != nil {
			return err
		}
	}
	if err := btx.Delete(vecLevelKey(vi.bucket, vi.field.Name, pk)); err != nil {
		return err
	}

	// If we were the entry point, pick a survivor. We cheat: scan
	// for any pk with a level key set; that's "good enough" because
	// HNSW correctness only requires the entry point to be a real
	// node, not the highest-level one. The next insert that draws a
	// higher level will repair maxLevel.
	ep, _, err := vi.readEntry(btx)
	if err != nil {
		return err
	}
	if ep != nil && vecBytesEqual(ep, pk) {
		survivor, surLevel, err := vi.findSurvivor(btx)
		if err != nil {
			return err
		}
		if survivor == nil {
			// Graph is empty: drop the entry key.
			if err := btx.Delete(vecEntryKey(vi.bucket, vi.field.Name)); err != nil {
				return err
			}
			man.MaxLevel = 0
		} else {
			if err := vi.writeEntry(btx, survivor, surLevel); err != nil {
				return err
			}
			if uint64(surLevel) < man.MaxLevel {
				man.MaxLevel = uint64(surLevel)
			}
		}
	}
	return nil
}

// findSurvivor returns any (pk, level) with a level marker set. Used
// when the entry point is being deleted and we need a fallback.
func (vi *vectorIndex) findSurvivor(btx *badger.Txn) ([]byte, uint8, error) {
	pfx := append([]byte{}, vecFieldPrefix(vi.bucket, vi.field.Name)...)
	pfx = append(pfx, vecLevelMark...)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = pfx
	opts.PrefetchValues = true
	it := btx.NewIterator(opts)
	defer it.Close()
	for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
		item := it.Item()
		k := item.KeyCopy(nil)
		pk := append([]byte(nil), k[len(pfx):]...)
		// Skip tombstoned survivors.
		dead, err := vi.isTombstoned(btx, pk)
		if err != nil {
			return nil, 0, err
		}
		if dead {
			continue
		}
		var lvl uint8
		err = item.Value(func(val []byte) error {
			if len(val) < 1 {
				return fmt.Errorf("level value too short")
			}
			lvl = val[0]
			return nil
		})
		if err != nil {
			return nil, 0, err
		}
		return pk, lvl, nil
	}
	return nil, 0, nil
}

// bytesEqual is the obvious []byte comparator. Used in graph helpers
// where importing bytes for one call would be overkill (and where the
// FTS code already shows a similar local helper to avoid the import).
func vecBytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Distance
// ---------------------------------------------------------------------------

// score returns "higher = more similar" using the configured metric.
// All branches are inlined hot loops; this is the single biggest CPU
// cost of brute search so we keep it boring.
func score(metric VectorMetric, a, b []float32) float64 {
	switch metric {
	case DotProduct:
		return dot(a, b)
	case Euclidean:
		return -l2(a, b)
	default: // Cosine, MetricDefault treated as Cosine here
		return cosine(a, b)
	}
}

func dot(a, b []float32) float64 {
	var s float64
	for i := range a {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}

func cosine(a, b []float32) float64 {
	var dotV, na, nb float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dotV += x * y
		na += x * x
		nb += y * y
	}
	denom := math.Sqrt(na) * math.Sqrt(nb)
	if denom == 0 {
		return 0
	}
	return dotV / denom
}

func l2(a, b []float32) float64 {
	var s float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		s += d * d
	}
	return math.Sqrt(s)
}

// ---------------------------------------------------------------------------
// HNSW algorithms
// ---------------------------------------------------------------------------
//
// The graph is built bottom-up under the standard HNSW model:
//
//   - Each node carries a randomly drawn max level (geometric, p=1/M).
//   - Level 0 holds every node and is the densest.
//   - Higher levels are sparse "skip lists" that let search jump
//     toward the answer in log(N) hops.
//
// All graph mutations and reads run inside the Badger txn the caller
// supplied — so insert/search are consistent with the data write and
// crash recovery is just transaction rollback.
//
// Score semantics inside HNSW: we work with "higher = closer" because
// every metric (cosine/dot/-l2) reports that way. Candidate priority
// queues use this score directly.

// candDist pairs a pk with its score under the active metric.
type candDist struct {
	pk    []byte
	score float64
}

// greedySearchLevel walks from `entry` toward `q` greedily on `level`,
// returning the closest node it finds. The loop terminates when no
// neighbour beats the current best, which is the standard HNSW
// "navigation" step on upper levels.
//
// scoreOf maps a pk to its distance from q. We thread the function in
// so callers can cache vector reads (insert reads its own vector once,
// search reads the query once).
func (vi *vectorIndex) greedySearchLevel(btx *badger.Txn, entry []byte, q []float32, metric VectorMetric, level uint8) ([]byte, float64, error) {
	curr := entry
	currVec, err := vi.readVec(btx, curr)
	if err != nil {
		return nil, 0, err
	}
	currScore := score(metric, q, currVec)
	for {
		neighs, err := vi.readNeighbours(btx, curr, level)
		if err != nil {
			return nil, 0, err
		}
		improved := false
		for _, np := range neighs {
			nv, err := vi.readVec(btx, np)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					// Tombstoned/removed neighbour — skip.
					continue
				}
				return nil, 0, err
			}
			s := score(metric, q, nv)
			if s > currScore {
				curr = np
				currScore = s
				improved = true
			}
		}
		if !improved {
			return curr, currScore, nil
		}
	}
}

// efSearchLevel is the inner HNSW search routine on a single level.
// It maintains:
//
//   - a "candidates" max-heap by score (always pull the most
//     promising frontier next)
//   - a "results" min-heap of size <= ef tracking the best ef seen so
//     far (so we can compare any new candidate's score against the
//     current ef'th-best to decide whether to continue)
//
// Tombstoned nodes are skipped on the consume side: neighbour lists
// can still point at them (we don't tear edges on delete), so we
// filter at search time. allowed, when non-nil, narrows results to
// pks the caller pre-vetted via a query.Query filter.
//
// Returns the result heap as a slice of candDist, descending by score.
func (vi *vectorIndex) efSearchLevel(
	btx *badger.Txn,
	entries []candDist,
	q []float32,
	metric VectorMetric,
	level uint8,
	ef int,
	allowed map[string]struct{},
) ([]candDist, error) {
	visited := make(map[string]struct{}, ef*2)
	candidates := newScoreHeap(true) // max-heap on score
	results := newScoreHeap(false)   // min-heap on score, capped at ef

	for _, e := range entries {
		visited[string(e.pk)] = struct{}{}
		candidates.push(e)
		dead, err := vi.isTombstoned(btx, e.pk)
		if err != nil {
			return nil, err
		}
		if dead {
			continue
		}
		if allowed != nil {
			if _, ok := allowed[string(e.pk)]; !ok {
				continue
			}
		}
		results.push(e)
	}
	results.cap = ef
	results.trim()

	for candidates.len() > 0 {
		c := candidates.popMax()
		// If the best frontier candidate is worse than the worst
		// result already in our top-ef, we're done.
		if results.len() >= ef && c.score < results.peekMin().score {
			break
		}
		neighs, err := vi.readNeighbours(btx, c.pk, level)
		if err != nil {
			return nil, err
		}
		for _, np := range neighs {
			if _, seen := visited[string(np)]; seen {
				continue
			}
			visited[string(np)] = struct{}{}
			nv, err := vi.readVec(btx, np)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				return nil, err
			}
			s := score(metric, q, nv)
			cd := candDist{pk: np, score: s}

			// Always feed the frontier — the graph isn't
			// monotone, a worse-now neighbour might lead
			// somewhere better.
			candidates.push(cd)

			dead, err := vi.isTombstoned(btx, np)
			if err != nil {
				return nil, err
			}
			if dead {
				continue
			}
			if allowed != nil {
				if _, ok := allowed[string(np)]; !ok {
					continue
				}
			}
			if results.len() < ef || s > results.peekMin().score {
				results.push(cd)
				results.cap = ef
				results.trim()
			}
		}
	}

	out := results.drainSorted(true)
	return out, nil
}

// selectNeighborsHeuristic picks at most M neighbours for a new node
// from `cands`. Implements the HNSW paper's "heuristic 2": keep
// candidates whose closest already-selected neighbour is farther
// (under the same metric) than the candidate itself is from the new
// node. The diversification reduces clusters of mutually-close edges
// and consistently lifts recall by a few points.
//
// `cands` must be sorted descending by score (closest first).
func (vi *vectorIndex) selectNeighborsHeuristic(
	btx *badger.Txn,
	cands []candDist,
	M int,
	metric VectorMetric,
) ([]candDist, error) {
	if len(cands) <= M {
		return cands, nil
	}
	selected := make([]candDist, 0, M)
	// Precompute selected vectors so we can compare candidates
	// against them without re-reading from Badger every time.
	selectedVecs := make([][]float32, 0, M)
	for _, c := range cands {
		if len(selected) >= M {
			break
		}
		cv, err := vi.readVec(btx, c.pk)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			return nil, err
		}
		good := true
		for i, sv := range selectedVecs {
			// "candidate is closer to a selected neighbour
			// than to the new node" → reject.
			if score(metric, cv, sv) > c.score {
				_ = i
				good = false
				break
			}
		}
		if good {
			selected = append(selected, c)
			selectedVecs = append(selectedVecs, cv)
		}
	}
	// Pad with leftovers if heuristic was too strict.
	if len(selected) < M {
		for _, c := range cands {
			if len(selected) >= M {
				break
			}
			already := false
			for _, s := range selected {
				if vecBytesEqual(s.pk, c.pk) {
					already = true
					break
				}
			}
			if !already {
				selected = append(selected, c)
			}
		}
	}
	return selected, nil
}

// insertGraph wires pk (with vector v) into the HNSW graph. Returns the
// updated manifest (entry-point and max-level may change).
func (vi *vectorIndex) insertGraph(btx *badger.Txn, pk []byte, v []float32, man vectorManifest) (vectorManifest, error) {
	M := vi.effectiveM(man)
	efC := vi.effectiveEfConstruction(man)
	metric := VectorMetric(man.Metric)
	if metric == MetricDefault {
		metric = vi.defaultM
	}

	level := vi.randomLevel(M)
	if err := vi.writeLevel(btx, pk, level); err != nil {
		return man, err
	}

	// Empty graph — this node is the entry point.
	ep, epLevel, err := vi.readEntry(btx)
	if err != nil {
		return man, err
	}
	if ep == nil {
		if err := vi.writeEntry(btx, pk, level); err != nil {
			return man, err
		}
		man.MaxLevel = uint64(level)
		return man, nil
	}

	// Greedy descent on every level above the new node's max level.
	curr := ep
	currScore := score(metric, v, mustReadVec(btx, vi, ep))
	for L := int(epLevel); L > int(level); L-- {
		next, ns, err := vi.greedySearchLevel(btx, curr, v, metric, uint8(L))
		if err != nil {
			return man, err
		}
		curr = next
		currScore = ns
		_ = currScore
	}

	// At each level from min(level, epLevel) down to 0, run an
	// ef-search to find candidate neighbours, then heuristic-2 to
	// pick M, then bidirectional bond + prune.
	startL := int(level)
	if int(epLevel) < startL {
		startL = int(epLevel)
	}
	for L := startL; L >= 0; L-- {
		ent := []candDist{{pk: curr, score: currScore}}
		results, err := vi.efSearchLevel(btx, ent, v, metric, uint8(L), efC, nil)
		if err != nil {
			return man, err
		}
		// Heuristic-2 picks M from the ef candidates.
		mPick := M
		// Allow more degree at the dense bottom level (commonly
		// 2*M in HNSW papers); we keep it simple and use M
		// throughout in Stage B.
		neighbours, err := vi.selectNeighborsHeuristic(btx, results, mPick, metric)
		if err != nil {
			return man, err
		}

		// Write our own neighbour list.
		ourNeigh := make([][]byte, 0, len(neighbours))
		for _, n := range neighbours {
			ourNeigh = append(ourNeigh, n.pk)
		}
		if err := vi.writeNeighbours(btx, pk, uint8(L), ourNeigh); err != nil {
			return man, err
		}

		// Bidirectional bond + pruning of each neighbour.
		for _, n := range neighbours {
			rev, err := vi.readNeighbours(btx, n.pk, uint8(L))
			if err != nil {
				return man, err
			}
			rev = append(rev, append([]byte(nil), pk...))
			if len(rev) > M {
				// Re-rank rev under the neighbour's metric
				// view: distances to *the neighbour*, then
				// pick M.
				nv, err := vi.readVec(btx, n.pk)
				if err != nil {
					return man, err
				}
				ranked := make([]candDist, 0, len(rev))
				for _, rp := range rev {
					rv, err := vi.readVec(btx, rp)
					if err != nil {
						if errors.Is(err, badger.ErrKeyNotFound) {
							continue
						}
						return man, err
					}
					ranked = append(ranked, candDist{
						pk:    rp,
						score: score(metric, nv, rv),
					})
				}
				sort.Slice(ranked, func(i, j int) bool {
					return ranked[i].score > ranked[j].score
				})
				kept, err := vi.selectNeighborsHeuristic(btx, ranked, M, metric)
				if err != nil {
					return man, err
				}
				rev = rev[:0]
				for _, k := range kept {
					rev = append(rev, k.pk)
				}
			}
			if err := vi.writeNeighbours(btx, n.pk, uint8(L), rev); err != nil {
				return man, err
			}
		}

		// For the next level, take the closest neighbour as the
		// new curr to descend from.
		if len(neighbours) > 0 {
			curr = neighbours[0].pk
			currScore = neighbours[0].score
		}
	}

	// If our level beats the previous max, we become the new entry.
	if level > epLevel {
		if err := vi.writeEntry(btx, pk, level); err != nil {
			return man, err
		}
		if uint64(level) > man.MaxLevel {
			man.MaxLevel = uint64(level)
		}
	}
	return man, nil
}

// mustReadVec is a tiny helper for the tight greedy descent at insert
// time. It panics on error rather than threading errors through the
// loop because the caller already verified the entry-point exists in
// the same txn; a missing vector here is a real corruption.
func mustReadVec(btx *badger.Txn, vi *vectorIndex, pk []byte) []float32 {
	v, err := vi.readVec(btx, pk)
	if err != nil {
		// Treat as zero-vector so descent terminates rather than
		// panics; this can only happen on data corruption and
		// the caller will see scores of 0.
		return make([]float32, vi.dim)
	}
	return v
}

// ---------------------------------------------------------------------------
// scoreHeap: dual-purpose binary heap on candDist scores
// ---------------------------------------------------------------------------

// scoreHeap is a binary heap that doubles as a min-heap or max-heap
// on candDist.score, depending on the `max` flag set at construction.
// Used by ef-search to keep the candidate frontier (max-heap) and the
// running top-ef result set (min-heap, optionally capped).
type scoreHeap struct {
	items []candDist
	max   bool
	cap   int // 0 means unbounded
}

func newScoreHeap(max bool) *scoreHeap { return &scoreHeap{max: max} }

func (h *scoreHeap) less(i, j int) bool {
	if h.max {
		return h.items[i].score > h.items[j].score
	}
	return h.items[i].score < h.items[j].score
}

func (h *scoreHeap) len() int { return len(h.items) }

func (h *scoreHeap) push(x candDist) {
	h.items = append(h.items, x)
	h.siftUp(len(h.items) - 1)
}

func (h *scoreHeap) siftUp(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if h.less(i, p) {
			h.items[i], h.items[p] = h.items[p], h.items[i]
			i = p
			continue
		}
		break
	}
}

func (h *scoreHeap) siftDown(i int) {
	n := len(h.items)
	for {
		l, r := 2*i+1, 2*i+2
		best := i
		if l < n && h.less(l, best) {
			best = l
		}
		if r < n && h.less(r, best) {
			best = r
		}
		if best == i {
			return
		}
		h.items[i], h.items[best] = h.items[best], h.items[i]
		i = best
	}
}

// popMax returns the highest-scoring item from a max-heap; for
// min-heaps it returns the smallest. We name it "Max" because that's
// the most common ef-search use.
func (h *scoreHeap) popMax() candDist {
	top := h.items[0]
	last := len(h.items) - 1
	h.items[0] = h.items[last]
	h.items = h.items[:last]
	if last > 0 {
		h.siftDown(0)
	}
	return top
}

// peekMin returns the smallest item under the heap's ordering. For a
// min-heap this is items[0]; for a max-heap we have to scan because
// max-heaps don't track the min cheaply, so callers must use this on
// min-heaps only.
func (h *scoreHeap) peekMin() candDist {
	return h.items[0]
}

// trim removes elements until len <= cap. For a min-heap with
// cap=ef this evicts the worst-scoring item — exactly the bookkeeping
// we want for the running top-ef.
func (h *scoreHeap) trim() {
	if h.cap <= 0 {
		return
	}
	for len(h.items) > h.cap {
		h.popMax()
	}
}

// drainSorted returns the heap's contents sorted by score; descending
// when desc is true. The heap itself is left empty.
func (h *scoreHeap) drainSorted(desc bool) []candDist {
	out := append([]candDist(nil), h.items...)
	h.items = h.items[:0]
	if desc {
		sort.Slice(out, func(i, j int) bool { return out[i].score > out[j].score })
	} else {
		sort.Slice(out, func(i, j int) bool { return out[i].score < out[j].score })
	}
	return out
}

// ---------------------------------------------------------------------------
// Search path
// ---------------------------------------------------------------------------

// search returns the top-k records matching q under the configured
// metric. allowed, when non-nil, restricts the candidate set to the
// listed pks (caller-resolved from a query.Query in bucket.SearchVector).
// efSearch tunes the HNSW result-set size; values <= k are clamped to
// k (the standard HNSW guarantee that ef >= k for top-k queries).
//
// Routing is adaptive: very small graphs and very selective filters
// fall back to brute force, which is both faster and trivially exact
// at small N. For the rest, the HNSW algorithm runs.
func (vi *vectorIndex) search(
	db *DB,
	q []float32,
	k int,
	efSearch int,
	metric VectorMetric,
	allowed map[string]struct{},
	hydrateOne func(pk []byte, score float64) error,
) error {
	if len(q) == 0 {
		return ErrVectorEmpty
	}
	if metric == MetricDefault {
		metric = vi.defaultM
	}
	if k <= 0 {
		k = 10
	}
	if efSearch <= 0 {
		efSearch = defaultHNSWEfSearch
	}
	if efSearch < k {
		efSearch = k
	}

	type rankedHit struct {
		pk    []byte
		score float64
	}
	var hits []rankedHit

	err := db.bdb.View(func(btx *badger.Txn) error {
		man, err := vi.readManifest(btx)
		if err != nil {
			return err
		}
		if man.Count == 0 {
			return nil
		}
		if int(man.Dim) != len(q) {
			return fmt.Errorf("%w: bucket dim=%d, query dim=%d", ErrDimMismatch, man.Dim, len(q))
		}

		// Brute-force fallback: graphs below this size, or
		// filters with very few survivors, are faster (and
		// trivially exact) without HNSW.
		const bruteThreshold = 64
		filterIsTight := allowed != nil && len(allowed) <= bruteThreshold
		if int(man.Count) <= bruteThreshold || filterIsTight {
			h, err := vi.bruteSearch(btx, q, k, metric, allowed)
			if err != nil {
				return err
			}
			for _, x := range h {
				hits = append(hits, rankedHit{pk: x.pk, score: x.score})
			}
			return nil
		}

		// HNSW: greedy descent on upper levels, ef-search on
		// level 0, top-k by score (ties broken by pk for
		// determinism).
		ep, epLevel, err := vi.readEntry(btx)
		if err != nil {
			return err
		}
		if ep == nil {
			return nil
		}
		curr := ep
		currVec, err := vi.readVec(btx, curr)
		if err != nil {
			return err
		}
		currScore := score(metric, q, currVec)
		for L := int(epLevel); L >= 1; L-- {
			next, ns, err := vi.greedySearchLevel(btx, curr, q, metric, uint8(L))
			if err != nil {
				return err
			}
			curr = next
			currScore = ns
		}
		ent := []candDist{{pk: curr, score: currScore}}
		results, err := vi.efSearchLevel(btx, ent, q, metric, 0, efSearch, allowed)
		if err != nil {
			return err
		}
		// efSearchLevel already sorts descending; clamp to k.
		if len(results) > k {
			results = results[:k]
		}
		for _, r := range results {
			hits = append(hits, rankedHit{pk: r.pk, score: r.score})
		}
		return nil
	})
	if err != nil {
		return err
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].score != hits[j].score {
			return hits[i].score > hits[j].score
		}
		return string(hits[i].pk) < string(hits[j].pk)
	})

	for _, h := range hits {
		if err := hydrateOne(h.pk, h.score); err != nil {
			return err
		}
	}
	return nil
}

// bruteSearch is the small-N / tight-filter fast path: stream every
// (non-tombstoned, allowed) raw vector and run the same top-k heap as
// before. Always exact.
func (vi *vectorIndex) bruteSearch(
	btx *badger.Txn,
	q []float32,
	k int,
	metric VectorMetric,
	allowed map[string]struct{},
) ([]minHeapItem, error) {
	prefix := vecRawPrefix(vi.bucket, vi.field.Name)
	heap := newMinHeap(k)

	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchSize = 64
	it := btx.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		pk := pkFromVecRawKey(item.KeyCopy(nil), prefix)
		if pk == nil {
			continue
		}
		if allowed != nil {
			if _, ok := allowed[string(pk)]; !ok {
				continue
			}
		}
		dead, err := vi.isTombstoned(btx, pk)
		if err != nil {
			return nil, err
		}
		if dead {
			continue
		}
		var v []float32
		err = item.Value(func(val []byte) error {
			var derr error
			v, derr = decodeVector(val)
			return derr
		})
		if err != nil {
			return nil, err
		}
		s := score(metric, q, v)
		heap.push(minHeapItem{pk: pk, score: s})
	}
	hits := heap.drain()
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].score != hits[j].score {
			return hits[i].score > hits[j].score
		}
		return string(hits[i].pk) < string(hits[j].pk)
	})
	return hits, nil
}

// ---------------------------------------------------------------------------
// Top-k bounded min-heap
// ---------------------------------------------------------------------------

// minHeapItem is one element inside the bounded heap.
type minHeapItem struct {
	pk    []byte
	score float64
}

// minHeap is a fixed-capacity min-heap keyed on score, used to track
// the running top-k highest-scoring vectors. A standard min-heap is
// the right shape: when capacity is reached, anything ≤ the root is
// rejected for free, otherwise the root is evicted in O(log k).
type minHeap struct {
	data []minHeapItem
	cap  int
}

func newMinHeap(cap int) *minHeap {
	return &minHeap{data: make([]minHeapItem, 0, cap), cap: cap}
}

func (h *minHeap) push(x minHeapItem) {
	if len(h.data) < h.cap {
		h.data = append(h.data, x)
		h.siftUp(len(h.data) - 1)
		return
	}
	if x.score <= h.data[0].score {
		return
	}
	h.data[0] = x
	h.siftDown(0)
}

func (h *minHeap) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if h.data[i].score < h.data[parent].score {
			h.data[i], h.data[parent] = h.data[parent], h.data[i]
			i = parent
			continue
		}
		break
	}
}

func (h *minHeap) siftDown(i int) {
	n := len(h.data)
	for {
		l, r := 2*i+1, 2*i+2
		smallest := i
		if l < n && h.data[l].score < h.data[smallest].score {
			smallest = l
		}
		if r < n && h.data[r].score < h.data[smallest].score {
			smallest = r
		}
		if smallest == i {
			return
		}
		h.data[i], h.data[smallest] = h.data[smallest], h.data[i]
		i = smallest
	}
}

// drain returns a copy of the heap's contents in arbitrary order.
// Callers usually sort the result by score before presenting it.
func (h *minHeap) drain() []minHeapItem {
	out := make([]minHeapItem, len(h.data))
	copy(out, h.data)
	return out
}

// ---------------------------------------------------------------------------
// Maintenance
// ---------------------------------------------------------------------------

// wipeVectorBucket deletes every vector key for a bucket. Used by
// DB.Wipe via the registry's resetAll.
func wipeVectorBucket(db *DB, bucket string) error {
	prefix := vecBucketPrefix(bucket)
	const batchSize = 1024
	for {
		var keys [][]byte
		err := db.bdb.View(func(btx *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = false
			it := btx.NewIterator(opts)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix) && len(keys) < batchSize; it.Next() {
				keys = append(keys, it.Item().KeyCopy(nil))
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		err = db.bdb.Update(func(btx *badger.Txn) error {
			for _, k := range keys {
				if err := btx.Delete(k); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(keys) < batchSize {
			return nil
		}
	}
}

// resetAll wipes every vector key in the registry. Mirrors the FTS
// registry's reset hook so DB.Wipe drops both at once.
func (r *vectorRegistry) resetAll(db *DB) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for bucket := range r.indexes {
		if err := wipeVectorBucket(db, bucket); err != nil {
			return fmt.Errorf("bw: vector wipe %q: %w", bucket, err)
		}
	}
	return nil
}

// extractVector returns the []float32 stored in the vector field of
// record, or nil if the field is empty / record is a nil pointer.
func (vi *vectorIndex) extractVector(record any) []float32 {
	rv := reflect.ValueOf(record)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	fv := rv.FieldByIndex(vi.field.Index)
	if fv.Kind() != reflect.Slice {
		return nil
	}
	if fv.Len() == 0 {
		return nil
	}
	return fv.Interface().([]float32)
}
