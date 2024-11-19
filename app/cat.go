package app

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

const catDBName = "app.db"

var catStateBucket = []byte("state")

type Cat struct {
	CatID conceptual.CatID
}

type CatWriter struct {
	CatID conceptual.CatID
	Flat  *flat.Flat
	Cache *ttlcache.Cache[conceptual.CatID, *cattrack.CatTrack]
	cmu   sync.Mutex
}

// NewCatWriter defines filepath and encoding for a cat.
// It should be non-contentious.
func (c *Cat) NewCatWriter() (*CatWriter, error) {
	flatCat := flat.NewFlatWithRoot(params.DatadirRoot).ForCat(c.CatID)
	if err := flatCat.Ensure(); err != nil {
		return nil, err
	}
	return &CatWriter{
		CatID: c.CatID,
		Flat:  flatCat,
		Cache: ttlcache.New[conceptual.CatID, *cattrack.CatTrack](
			ttlcache.WithTTL[conceptual.CatID, *cattrack.CatTrack](params.CacheLastKnownTTL)),
	}, nil
}

func (w *CatWriter) WriteTrack(wr io.Writer, ct *cattrack.CatTrack) error {
	if err := json.NewEncoder(wr).Encode(ct); err != nil {
		return err
	}
	w.cmu.Lock()
	w.Cache.Set(w.CatID, ct, ttlcache.DefaultTTL)
	w.cmu.Unlock()
	return nil
}

func (w *CatWriter) TrackWriter() (io.WriteCloser, error) {
	gzf, err := w.Flat.TracksGZ()
	if err != nil {
		return nil, err
	}
	return gzf.Writer(), nil

}

func (w *CatWriter) CustomWriter(target string) (io.WriteCloser, error) {
	f, err := w.Flat.NamedGZ(target)
	if err != nil {
		return nil, err
	}
	wr := f.Writer()
	return wr, nil
}

func (w *CatWriter) storeKV(key []byte, data []byte) error {
	catPath := w.Flat.Path()
	db, err := bbolt.Open(filepath.Join(catPath, catDBName), 0600, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(func(tx *bbolt.Tx) error {
		data := data
		bucket, err := tx.CreateBucketIfNotExists(catStateBucket)
		if err != nil {
			return err
		}
		return bucket.Put(key, data)
	})
}

func (w *CatWriter) StoreLastTrack() error {
	w.cmu.Lock()
	track := w.Cache.Get(w.CatID)
	defer w.cmu.Unlock()
	if track == nil {
		return fmt.Errorf("no last track (impossible if caller uses correctly)")
	}
	v := track.Value()
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(b)
	err = w.storeKV([]byte("last"), buf.Bytes())
	if err != nil {
		slog.Error("Failed to store last track", "error", err)
	} else {
		slog.Debug("Stored last track", "cat", w.CatID, "track", string(b))
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
func (w *CatWriter) StoreTracksAt(key []byte, tracks []*cattrack.CatTrack) error {
	buf := bytes.NewBuffer([]byte{})
	enc := json.NewEncoder(buf)
	for _, track := range tracks {
		if err := enc.Encode(track); err != nil {
			return err
		}
	}
	return w.storeKV(key, buf.Bytes())
}

func (w *CatWriter) WriteKV(key []byte, value []byte) error {
	return w.storeKV(key, value)
}

// WriteSnap is not implemented.
// TODO: Implement WriteSnap.
func (w *CatWriter) WriteSnap(ct *cattrack.CatTrack) error {
	return fmt.Errorf("not implemented")
}

func (w *CatWriter) Close() error {
	return nil
}

type CatReader struct {
	CatID conceptual.CatID
	Flat  *flat.Flat
}

func (c *Cat) NewCatReader() (*CatReader, error) {
	f := flat.NewFlatWithRoot(params.DatadirRoot).ForCat(c.CatID)
	if !f.Exists() {
		return nil, fmt.Errorf("cat not found")
	}
	return &CatReader{
		CatID: c.CatID,
		Flat:  f,
	}, nil
}

func (w *CatReader) readKV(key []byte) ([]byte, error) {
	catPath := w.Flat.Path()
	db, err := bbolt.Open(filepath.Join(catPath, catDBName), 0600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()
	buf := bytes.NewBuffer([]byte{})
	err = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(catStateBucket)
		if bucket == nil {
			return fmt.Errorf("no app bucket")
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

func (w *CatReader) ReadKV(key []byte) ([]byte, error) {
	return w.readKV(key)
}

func (w *CatReader) ReadLastTrack() (*cattrack.CatTrack, error) {
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

func (w *CatReader) ReadTracksAt(key []byte) ([]*cattrack.CatTrack, error) {
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
