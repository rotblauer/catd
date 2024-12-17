package api

import (
	"context"
	"fmt"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/stream"
	"github.com/rotblauer/catd/types/cattrack"
	"os"
	"sync"
)

var PropKeyInvalid = "invalid"

func (c *Cat) Validate(ctx context.Context, in <-chan cattrack.CatTrack) (valid chan cattrack.CatTrack, invalid chan cattrack.CatTrack) {
	valid = make(chan cattrack.CatTrack, params.DefaultBatchSize)
	invalid = make(chan cattrack.CatTrack, params.DefaultBatchSize)
	go func() {
		defer close(valid)
		defer close(invalid)
		for ct := range in {
			if ct.IsEmpty() {
				c.logger.Error("Invalid track: track is empty")
				ct.SetPropertySafe(PropKeyInvalid, "empty")
				invalid <- ct
				continue
			}
			if err := ct.Validate(); err != nil {
				c.logger.Error("Invalid track", "error", err)
				ct.SetPropertySafe(PropKeyInvalid, err.Error())
				invalid <- ct
				continue
			}
			if id := ct.CatID(); c.CatID != id {
				c.logger.Error("Invalid track, mismatched cat", "want", fmt.Sprintf("%q", c.CatID), "got", fmt.Sprintf("%q", id))
				ct.SetPropertySafe(PropKeyInvalid, "mismatched cat")
				invalid <- ct
				continue
			}
			valid <- ct
		}
	}()
	return valid, invalid
}

func (c *Cat) waitHandleInvalid(ctx context.Context, invalid chan cattrack.CatTrack, wg sync.WaitGroup) {
	wg.Add(1)
	go c.handleInvalid(ctx, invalid, wg)
}

func (c *Cat) handleInvalid(ctx context.Context, invalid chan cattrack.CatTrack, wg sync.WaitGroup) {
	defer wg.Done()
	invalidCollection := stream.Collect(ctx, invalid)
	if len(invalidCollection) > 0 {
		for _, track := range invalidCollection {
			c.logger.Error("Invalid track", "track", track.StringPretty())
		}
		wr, err := os.CreateTemp(params.DefaultDatadirRoot, "invalid-tracks-*.json.gz")
		if err != nil {
			c.logger.Error("Failed to create temporary file", "error", err)
			return
		}
		defer wr.Close()
		_, err = sinkStreamToJSONWriter(ctx, wr, stream.Slice(ctx, invalidCollection))
		if err != nil {
			c.logger.Error("Failed to write invalid tracks", "error", err)
			return
		}
	}
}

func (c *Cat) dedupe(ctx context.Context, size int, in <-chan cattrack.CatTrack) <-chan cattrack.CatTrack {
	dedupeCache := cattrack.NewDedupeLRUFunc(size)
	return stream.Filter(ctx, func(ct cattrack.CatTrack) bool {
		if !dedupeCache(ct) {
			c.logger.Warn("Deduped track", "track", ct.StringPretty())
			return false
		}
		return true
	}, in)
}
