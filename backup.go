package bw

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
	"google.golang.org/protobuf/proto"
)

var maxPendingWrites = 256

// Backup creates a backup of all entries with version > since.
//
// When deletedData is false, it uses Badger's built-in Backup which skips
// delete markers. When true, it walks all versions (including deletes) so the
// backup can later be used with BackupUntil for point-in-time restore.
func (db *DB) Backup(w io.Writer, since uint64, deletedData bool) (uint64, error) {
	db.accessMu.RLock()
	defer db.accessMu.RUnlock()
	slog.Info("backup database", slog.Uint64("since", since))

	if !deletedData {
		return db.bdb.Backup(w, since)
	}

	stream := db.bdb.NewStream()
	stream.LogPrefix = "DB.Backup"
	stream.SinceTs = since
	stream.KeyToList = allVersionsKeyToList(since, 0)

	return streamBackup(stream, w)
}

// BackupUntil creates a backup containing only entries with version <= until.
// It iterates through all versions of each key (including past delete markers)
// so that older live versions are included when their version falls within range.
func (db *DB) BackupUntil(w io.Writer, until uint64) (uint64, error) {
	db.accessMu.RLock()
	defer db.accessMu.RUnlock()
	slog.Info("backup database", slog.Uint64("until", until))

	stream := db.bdb.NewStream()
	stream.LogPrefix = "DB.BackupUntil"
	stream.KeyToList = allVersionsKeyToList(0, until)

	return streamBackup(stream, w)
}

// Restore loads a backup from the given reader, replacing the current
// database contents. The reader should contain data previously written
// by Backup or BackupUntil.
//
// Full-text search state is stored as ordinary Badger keys under the
// \x00fts\x00 namespace (see keys.go), so the backup stream carries
// FTS postings, doc lengths, and corpus stats alongside data. Restore
// is therefore portable to any directory: the receiving DB sees the
// same view of every Bucket[T].Search the source produced. No rebuild
// step is required.
func (db *DB) Restore(r io.Reader) error {
	db.schemaMu.Lock()
	defer db.schemaMu.Unlock()
	db.accessMu.Lock()
	defer db.accessMu.Unlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := db.bdb.DropAll(); err != nil {
		return err
	}
	db.invalidateBucketHandles()
	if db.fts != nil {
		db.fts.closeAll()
	}
	if db.vec != nil {
		db.vec.closeAll()
	}
	if err := db.bdb.Load(r, maxPendingWrites); err != nil {
		return err
	}
	slog.Info("restored database")
	return nil
}

// ApplyBackup merges a full or incremental backup into the current
// database. Unlike Restore, keys absent from the stream are retained.
func (db *DB) ApplyBackup(r io.Reader) error {
	db.schemaMu.Lock()
	defer db.schemaMu.Unlock()
	db.accessMu.Lock()
	defer db.accessMu.Unlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	before, err := db.captureBucketBackupState()
	if err != nil {
		return err
	}
	if err := db.bdb.Load(r, maxPendingWrites); err != nil {
		db.invalidateBucketHandles()
		if db.fts != nil {
			db.fts.closeAll()
		}
		if db.vec != nil {
			db.vec.closeAll()
		}
		return err
	}
	after, err := db.captureBucketBackupState()
	if err != nil {
		db.invalidateBucketHandles()
		return err
	}
	for bucket, oldState := range before {
		if after[bucket] != oldState {
			db.bumpBucketEpoch(bucket)
		}
	}
	slog.Info("applied database backup")
	return nil
}

type bucketBackupState struct {
	fingerprint string
	version     uint64
	dim         uint64
	metric      uint64
	m           uint64
	ef          uint64
}

func (db *DB) captureBucketBackupState() (map[string]bucketBackupState, error) {
	db.runtimeMu.RLock()
	buckets := make([]string, 0, len(db.epochs))
	for bucket := range db.epochs {
		buckets = append(buckets, bucket)
	}
	db.runtimeMu.RUnlock()

	states := make(map[string]bucketBackupState, len(buckets))
	for _, bucket := range buckets {
		fingerprint, err := db.readSchemaFingerprint(bucket)
		if err != nil {
			return nil, err
		}
		state := bucketBackupState{fingerprint: fingerprint}
		err = db.bdb.View(func(btx *badger.Txn) error {
			item, err := btx.Get(versionKey(bucket))
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			return item.Value(func(raw []byte) error {
				if len(raw) == 8 {
					state.version = binary.BigEndian.Uint64(raw)
				}
				return nil
			})
		})
		if err != nil {
			return nil, err
		}
		if vi := db.vec.get(bucket); vi != nil {
			man, err := readVectorManifestForField(db, bucket, vi.field.Name)
			if err != nil {
				return nil, err
			}
			state.dim = man.Dim
			state.metric = man.Metric
			state.m = man.M
			state.ef = man.EfConstruction
		}
		states[bucket] = state
	}
	return states, nil
}

// Version returns the maximum committed transaction version in the database.
// This is useful for incremental backups (pass the returned value as the
// "since" parameter to a subsequent Backup call).
func (db *DB) Version() uint64 {
	return db.bdb.MaxVersion()
}

// allVersionsKeyToList returns a KeyToList function that iterates through ALL
// versions of a key without stopping at delete markers.
//
//   - since > 0: skip versions < since  (incremental lower bound)
//   - until > 0: skip versions > until  (point-in-time upper bound)
//
// When both are 0, every version is included.
func allVersionsKeyToList(since, until uint64) func([]byte, *badger.Iterator) (*pb.KVList, error) {
	return func(key []byte, itr *badger.Iterator) (*pb.KVList, error) {
		list := &pb.KVList{}
		for ; itr.Valid(); itr.Next() {
			item := itr.Item()
			if !bytes.Equal(item.Key(), key) {
				return list, nil
			}

			v := item.Version()

			// Skip versions outside the requested range.
			if since > 0 && v < since {
				// All subsequent versions are even older; stop.
				return list, nil
			}
			if until > 0 && v > until {
				// Newer than requested; keep going to find older versions.
				continue
			}

			var valCopy []byte
			if !item.IsDeletedOrExpired() {
				if err := item.Value(func(val []byte) error {
					valCopy = make([]byte, len(val))
					copy(valCopy, val)
					return nil
				}); err != nil {
					return nil, err
				}
			}

			var meta byte
			if item.IsDeletedOrExpired() {
				meta = 1 // bitDelete
			}

			kv := &pb.KV{
				Key:       item.KeyCopy(nil),
				Value:     valCopy,
				UserMeta:  []byte{item.UserMeta()},
				Version:   v,
				ExpiresAt: item.ExpiresAt(),
				Meta:      []byte{meta},
			}
			list.Kv = append(list.Kv, kv)

			switch {
			case item.DiscardEarlierVersions():
				list.Kv = append(list.Kv, &pb.KV{
					Key:     item.KeyCopy(nil),
					Version: v - 1,
					Meta:    []byte{1}, // bitDelete
				})
				return list, nil
			case item.IsDeletedOrExpired():
				return list, nil
			}
		}
		return list, nil
	}
}

// streamBackup drives the given stream, writes each batch to w in the standard
// badger backup wire format, and returns the maximum version encountered.
func streamBackup(stream *badger.Stream, w io.Writer) (uint64, error) {
	var maxVersion uint64

	stream.Send = func(buf *z.Buffer) error {
		list, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}
		out := list.Kv[:0]
		for _, kv := range list.Kv {
			if kv.Version > maxVersion {
				maxVersion = kv.Version
			}
			if !kv.StreamDone {
				out = append(out, kv)
			}
		}
		list.Kv = out
		return writeBackupList(list, w)
	}

	if err := stream.Orchestrate(context.Background()); err != nil {
		return 0, err
	}
	return maxVersion, nil
}

func writeBackupList(list *pb.KVList, w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, uint64(proto.Size(list))); err != nil {
		return err
	}
	buf, err := proto.Marshal(list)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}
