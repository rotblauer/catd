package state

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/rotblauer/catd/types/cattrack"
	"os"
	"path/filepath"
)

// Snaps are stored in local cat state in two ways:
// 1. As a key-value pair in the state.db file.
//    The key is the snap key, and the value is the JSON cat track (with S3URL).
// 2. As two files in a subdirectory of the cat's 'snaps' directory;
//    1. The .jpeg image itself.
//    2. The cat track in JSON.
// (The KV value is redundant to the .json file in the subdir.)

func (s *CatState) snapFolderHolderPath(ct cattrack.CatTrack) string {
	t := ct.MustTime()
	return filepath.Join(s.Flat.Path(), "snaps",
		fmt.Sprintf("%d", t.Year()), fmt.Sprintf("%02d", t.Month()))
}

func (s *CatState) SnapImagePath(ct cattrack.CatTrack) string {
	return filepath.Join(s.snapFolderHolderPath(ct), ct.MustS3Key()+".jpg")
}

func (s *CatState) SnapTrackPath(ct cattrack.CatTrack) string {
	return filepath.Join(s.snapFolderHolderPath(ct), ct.MustS3Key()+".json")
}

// ValidateSnapLocalStore returns true if both the image and track exist
func (s *CatState) ValidateSnapLocalStore(ct cattrack.CatTrack) error {
	stat, err := os.Stat(s.SnapImagePath(ct))
	if err != nil {
		return err
	}
	if stat.Size() == 0 {
		return fmt.Errorf("empty image")
	}
	if stat.IsDir() {
		return fmt.Errorf("image is a directory")
	}
	stat, err = os.Stat(s.SnapTrackPath(ct))
	if err != nil {
		return err
	}
	if stat.Size() == 0 {
		return fmt.Errorf("empty track")
	}
	if stat.IsDir() {
		return fmt.Errorf("track is a directory")
	}
	return nil
}

func (s *CatState) storeSnapFlat(ct cattrack.CatTrack, jpegData []byte) error {
	// First, store the image, if any data provided.
	if jpegData != nil {
		target := s.SnapImagePath(ct)
		if err := os.MkdirAll(filepath.Dir(target), 0770); err != nil {
			return err
		}
		if err := os.WriteFile(target, jpegData, 0660); err != nil {
			return err
		}
	}

	// Second, store the track.
	return s.storeSnapJSON(ct)
}

func (s *CatState) storeSnapJSON(ct cattrack.CatTrack) error {
	// Second, store the track.
	target := s.SnapTrackPath(ct)
	if err := os.MkdirAll(filepath.Dir(target), 0770); err != nil {
		return err
	}
	buf := bytes.NewBuffer([]byte{})
	err := json.NewEncoder(buf).Encode(ct)
	if err != nil {
		return err
	}
	if err := os.WriteFile(target, buf.Bytes(), 0660); err != nil {
		return err
	}
	return nil
}

func (s *CatState) StoreSnapKV(ct cattrack.CatTrack) error {
	j, err := json.Marshal(ct)
	if err != nil {
		return err
	}
	return s.storeKV(CatSnapBucket, []byte(ct.MustS3Key()), j)
}

func (s *CatState) StoreSnapLocally(ct cattrack.CatTrack, jpegData []byte) error {
	if err := s.storeSnapFlat(ct, jpegData); err != nil {
		return err
	}
	return s.StoreSnapKV(ct)
}