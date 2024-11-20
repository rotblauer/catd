package state

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/jellydator/ttlcache/v3"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
	"go.etcd.io/bbolt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
)

const catDBName = "state.db"

var catStateBucket = []byte("state")

type Cat struct {
	CatID conceptual.CatID
	State *CatState
}

type CatState struct {
	CatID conceptual.CatID
	DB    *bbolt.DB
	Flat  *flat.Flat

	// FIXME: A TTL cache is definitely the wrong choice.
	// The TTL cache might be a server-scope cache, or otherwise
	// a global/API-facing-kind of cache. But a week-long TTL cache
	// on a state accessor instance?
	TTLCache *ttlcache.Cache[conceptual.CatID, *cattrack.CatTrack]
	Waiting  sync.WaitGroup
	rOnly    bool
}

// NewCatState defines data sources, caches, and encoding for a cat.
// It should be non-contentious. It must be blocking; it should not permit
// competing writes or reads to cat state. It must be the one true canonical cat.
func (c *Cat) NewCatState(readOnly bool) (*CatState, error) {
	flatCat := flat.NewFlatWithRoot(params.DatadirRoot).ForCat(c.CatID)
	if err := flatCat.Ensure(); err != nil {
		return nil, err
	}

	// Opening a writable DB conn will block all other cat writers and readers
	// with essentially a file lock/flock.
	db, err := bbolt.Open(filepath.Join(flatCat.Path(), catDBName),
		0600, &bbolt.Options{
			ReadOnly: readOnly,
		})
	if err != nil {
		return nil, err
	}

	s := &CatState{
		CatID: c.CatID,
		DB:    db,
		Flat:  flatCat,
		TTLCache: ttlcache.New[conceptual.CatID, *cattrack.CatTrack](
			ttlcache.WithTTL[conceptual.CatID, *cattrack.CatTrack](params.CacheLastKnownTTL)),
	}
	c.State = s
	return c.State, nil
}

func (s *CatState) Wait() {
	s.Waiting.Wait()
}

func (s *CatState) Close() error {
	if err := s.DB.Close(); err != nil {
		return err
	}
	return nil
}

func (s *CatState) WriteTrack(wr io.Writer, ct *cattrack.CatTrack) error {
	if err := json.NewEncoder(wr).Encode(ct); err != nil {
		return err
	}

	/*
			fatal error: concurrent map read and map write

			goroutine 1581 [running]:
			github.com/rotblauer/catd/types/cattrack.(*CatTrack).Time(0xc099f2c500?)
			        /home/ia/dev/rotblauer/catd/types/cattrack/cattrack.go:71 +0x70
			github.com/rotblauer/catd/types/cattrack.(*CatTrack).MustTime(...)
			        /home/ia/dev/rotblauer/catd/types/cattrack/cattrack.go:86
			github.com/rotblauer/catd/state.(*CatState).WriteTrack(0xc09a286400, {0xad6d80?, 0xc0907d6c60?}, 0xc09e0f94f0)
			        /home/ia/dev/rotblauer/catd/state/cat.go:89 +0xdb
			github.com/rotblauer/catd/api.(*Cat).Store.func1.3(0xc09e0f94f0)
			        /home/ia/dev/rotblauer/catd/api/store.go:50 +0x7e
			github.com/rotblauer/catd/stream.Transform[...].func1()
			        /home/ia/dev/rotblauer/catd/stream/stream.go:77 +0xe2
			created by github.com/rotblauer/catd/stream.Transform[...] in goroutine 1706
			        /home/ia/dev/rotblauer/catd/stream/stream.go:71 +0xcb

		When the cache looks up the LAST track, we now have the pointer to that track.
		So we're no longer handling tracks serially, and there are no guarantees that
		that last track isn't going to mutating in some "later" pipe function.
	*/

	// Cache as first or most recent track.
	//if res := s.TTLCache.Get("last"); res == nil || res.Value().MustTime().Before(ct.MustTime()) {
	s.TTLCache.Set("last", ct, ttlcache.DefaultTTL)
	//}
	return nil
}

func (s *CatState) TrackGZWriter() (io.WriteCloser, error) {
	gzf, err := s.Flat.TracksGZ()
	if err != nil {
		return nil, err
	}
	return gzf.Writer(), nil

}

func (s *CatState) CustomGZWriter(target string) (io.WriteCloser, error) {
	f, err := s.Flat.NamedGZ(target)
	if err != nil {
		return nil, err
	}
	wr := f.Writer()
	return wr, nil
}

func (s *CatState) storeKV(key []byte, data []byte) error {
	if key == nil {
		return fmt.Errorf("storeKV: nil key")
	}
	if data == nil {
		return fmt.Errorf("storeKV: nil data")
	}
	return s.DB.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(catStateBucket)
		if err != nil {
			return err
		}
		return bucket.Put(key, data)
	})
}

func (s *CatState) StoreLastTrack() error {
	track := s.TTLCache.Get("last")
	if track == nil {
		return fmt.Errorf("no last track (impossible if caller uses correctly)")
	}
	v := track.Value()
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	err = s.storeKV([]byte("last"), b)
	if err != nil {
		slog.Error("Failed to store last track", "error", err)
	} else {
		slog.Debug("Stored last track", "cat", s.CatID, "track", string(b))
	}
	return err
}

// StoreTracksAt stores tracks in a KV store.
// It is expected that this function can be used to cache
// partial naps or partial laps (i.e. their last, unfinished, incomplete nap or lap).
// Keep in mind that the tracks are buffered in memory, not streamed.
// For this reason, it may be prudent to limit the number of tracks stored (and read!) this way,
// and/or to limit the use of it.
// However, tracks are encoded in newline-delimited JSON to allow for streaming, someday, maybe.
func (s *CatState) StoreTracksAt(key []byte, tracks []*cattrack.CatTrack) error {
	buf := bytes.NewBuffer([]byte{})
	enc := json.NewEncoder(buf)
	for _, track := range tracks {
		if err := enc.Encode(track); err != nil {
			return err
		}
	}
	return s.storeKV(key, buf.Bytes())
}

func (s *CatState) WriteKV(key []byte, value []byte) error {
	return s.storeKV(key, value)
}

// WriteSnap is not implemented.
// TODO: Implement WriteSnap.
func (s *CatState) WriteSnap(ct *cattrack.CatTrack) error {
	return fmt.Errorf("not implemented")
}

func (w *CatState) readKV(key []byte) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	err := w.DB.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(catStateBucket)
		if bucket == nil {
			return fmt.Errorf("no state bucket")
		}

		// Gotcha! The value returned by Get is only valid in the scope of the transaction.
		got := bucket.Get(key)
		if got == nil {
			return nil
		}
		_, err := buf.Write(got)
		return err
	})
	return buf.Bytes(), err
}

func (w *CatState) ReadKV(key []byte) ([]byte, error) {
	return w.readKV(key)
}

func (w *CatState) ReadLastTrack() (*cattrack.CatTrack, error) {
	got, err := w.readKV([]byte("last"))
	if err != nil {
		return nil, err
	}
	if got == nil {
		return nil, fmt.Errorf("no last track")
	}
	track := &cattrack.CatTrack{}
	err = track.UnmarshalJSON(got)
	if err != nil {
		slog.Debug("Read last track", "error", err, "cat", w.CatID, "track", string(got))
		err = fmt.Errorf("%w: %q", err, string(got))
	} else {
		slog.Debug("Read last track", "cat", w.CatID, "track", track.StringPretty())
	}
	return track, err
}

func (w *CatState) ReadTracksAt(key []byte) ([]*cattrack.CatTrack, error) {
	got, err := w.readKV(key)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(got))
	var tracks []*cattrack.CatTrack
	for {
		track := &cattrack.CatTrack{}
		if err := dec.Decode(track); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		tracks = append(tracks, track)
	}
	return tracks, nil
}
