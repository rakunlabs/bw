package bw

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/blevesearch/bleve/v2"
	"github.com/rakunlabs/bw/schema"
)

// ftsIndex wraps a Bleve index for a single bucket's full-text search
// fields. It is nil when the bucket has no FTS-tagged fields.
type ftsIndex struct {
	index  bleve.Index
	fields []*schema.Field // only FTS-tagged fields
}

// ftsRegistry holds per-bucket FTS indexes. It lives on the DB so closing
// the DB closes all Bleve indexes.
type ftsRegistry struct {
	mu      sync.RWMutex
	indexes map[string]*ftsIndex // keyed by bucket name
}

func newFTSRegistry() *ftsRegistry {
	return &ftsRegistry{indexes: make(map[string]*ftsIndex)}
}

func (r *ftsRegistry) get(bucket string) *ftsIndex {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.indexes[bucket]
}

func (r *ftsRegistry) set(bucket string, idx *ftsIndex) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.indexes[bucket] = idx
}

func (r *ftsRegistry) closeAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, fi := range r.indexes {
		if fi != nil && fi.index != nil {
			_ = fi.index.Close()
		}
	}
	r.indexes = make(map[string]*ftsIndex)
}

// openFTSIndex opens or creates a Bleve index for the given bucket. The
// index is stored at <db-dir>/_fts_<bucket>. For in-memory databases, a
// memory-only Bleve mapping is used.
func openFTSIndex(db *DB, bucket string, fields []*schema.Field) (*ftsIndex, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	mapping := bleve.NewIndexMapping()
	docMapping := bleve.NewDocumentMapping()

	for _, f := range fields {
		fm := bleve.NewTextFieldMapping()
		fm.Store = false
		fm.IncludeInAll = true
		docMapping.AddFieldMappingsAt(f.Name, fm)
	}

	mapping.DefaultMapping = docMapping

	var idx bleve.Index
	var err error

	if db.path == "" {
		// In-memory DB → in-memory Bleve index.
		idx, err = bleve.NewMemOnly(mapping)
		if err != nil {
			return nil, fmt.Errorf("bw: fts: %w", err)
		}
	} else {
		dir := filepath.Join(db.path, "_fts_"+bucket)
		if _, statErr := os.Stat(dir); os.IsNotExist(statErr) {
			idx, err = bleve.New(dir, mapping)
		} else {
			idx, err = bleve.Open(dir)
		}
		if err != nil {
			return nil, fmt.Errorf("bw: fts: %w", err)
		}
	}

	return &ftsIndex{index: idx, fields: fields}, nil
}

// indexDoc indexes a record into Bleve. The document ID is the hex-encoded
// primary key so we can correlate search hits back to BadgerDB records.
func (fi *ftsIndex) indexDoc(pkHex string, record any) error {
	doc := fi.buildDoc(record)
	if doc == nil {
		return nil
	}

	return fi.index.Index(pkHex, doc)
}

// deleteDoc removes a document from the Bleve index.
func (fi *ftsIndex) deleteDoc(pkHex string) error {
	return fi.index.Delete(pkHex)
}

// buildDoc extracts FTS field values from the record into a map suitable
// for Bleve indexing.
func (fi *ftsIndex) buildDoc(record any) map[string]any {
	rv := reflect.ValueOf(record)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}

	doc := make(map[string]any, len(fi.fields))
	for _, f := range fi.fields {
		fv := rv.FieldByIndex(f.Index)
		if fv.Kind() == reflect.String {
			doc[f.Name] = fv.String()
		}
	}

	return doc
}

// search performs a full-text search and returns the matching document IDs
// (hex-encoded primary keys) with scores.
func (fi *ftsIndex) search(queryStr string, limit, offset int) ([]SearchHit, uint64, error) {
	q := bleve.NewQueryStringQuery(queryStr)
	req := bleve.NewSearchRequestOptions(q, limit, offset, false)

	res, err := fi.index.Search(req)
	if err != nil {
		return nil, 0, err
	}

	hits := make([]SearchHit, 0, len(res.Hits))
	for _, h := range res.Hits {
		hits = append(hits, SearchHit{
			ID:    h.ID,
			Score: h.Score,
		})
	}

	return hits, res.Total, nil
}

// SearchHit represents a single full-text search result with the document
// ID (hex pk) and relevance score.
type SearchHit struct {
	ID    string
	Score float64
}
