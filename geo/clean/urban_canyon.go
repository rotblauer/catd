/*
20241205

Wang is on ice because it was wrongly filtering high speed points.

This was because the distance threshold was used as a constant,
and did not take track speed into account, so fast tracks were being filtered.

I made an attempt to correct it using the speed of the track,
but it still requires tuning.
And it looks a lot like the teleportation filter.

2024-09 cat=ia in SF has some good micro-canyoning during
walks down the street for an ultimate test.
*/

package clean

import (
	"context"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
	"github.com/paulmach/orb/planar"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
	"math"
)

type WangUrbanCanyonFilter struct {
	Filtered int
}

// Filter filters out spurious points which can occur in urban canyons.
// > Wang: Third, GPS points away from
// > the adjacent points due to the signal shift caused by
// > blocking or ‘‘urban canyon’’ effect are also deleted. As
// > is shown in Figure 2, GPS points away from both the
// > before and after 5 points center for more than 200 m
// > should be considered as shift points.
func (f *WangUrbanCanyonFilter) Filter(ctx context.Context, in <-chan cattrack.CatTrack) <-chan cattrack.CatTrack {
	out := make(chan cattrack.CatTrack)

	bufferFront, bufferBack := 5, 5
	bufferSize := bufferFront + 1 + bufferBack
	buffer := make([]*cattrack.CatTrack, 0, bufferSize)

	go func() {
		defer close(out)
		for track := range in {
			track := track

			buffer = append(buffer, &track)
			if len(buffer) < bufferSize {
				// The first points get automatically flushed without filtering
				// because there are no head points to compare them against.
				if len(buffer) < bufferFront+1 {
					out <- track
				}
				continue
			}
			for len(buffer) > bufferSize {
				buffer = buffer[1:]
			}

			head := buffer[:bufferFront]
			target := buffer[bufferFront]
			tail := buffer[bufferFront+1:]

			// Signal loss is not eligible for filtering.
			if tail[len(tail)-1].MustTime().Sub(head[0].MustTime()) > params.DefaultCleanConfig.WangUrbanCanyonWindow {
				select {
				case <-ctx.Done():
					return
				case out <- *target:
				}
				continue
			}

			// Find the centroid of the tail.
			tailCenter, _ := planar.CentroidArea(orb.MultiPoint{tail[0].Point(), tail[1].Point(), tail[2].Point(), tail[3].Point(), tail[4].Point()})
			// Find the centroid of the head.
			headCenter, _ := planar.CentroidArea(orb.MultiPoint{head[0].Point(), head[1].Point(), head[2].Point(), head[3].Point(), head[4].Point()})

			threshold := math.Max(target.Properties.MustFloat64("Speed", 0), 0) *
				params.DefaultCleanConfig.WangUrbanCanyonDistanceFromSpeedMul
			threshold = math.Max(threshold, params.DefaultCleanConfig.WangUrbanCanyonMinDistance)

			// If the distances from the target to the tail and head centroids are more than 200m, it's a shift point.
			if geo.Distance(tailCenter, target.Point()) > threshold &&
				geo.Distance(headCenter, target.Point()) > threshold {
				f.Filtered++
				continue
			}

			select {
			case <-ctx.Done():
				return
			case out <- *target:
			}
		}

		// Any and all tailing points get sent.
		if len(buffer) > bufferFront+1 {
			for _, track := range buffer[bufferFront:] {
				select {
				case <-ctx.Done():
					return
				case out <- *track:
				}
			}
		}
	}()

	return out
}
