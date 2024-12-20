package stream

import (
	"github.com/dustin/go-humanize"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/rotblauer/catd/common"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"
)

type tickScanMeter struct {
	label      time.Time // any value, eg track.time
	interval   time.Duration
	started    time.Time
	ticker     *time.Ticker
	nn         atomic.Uint64
	cats       []string
	reg        metrics.Registry
	count      metrics.Counter
	size       metrics.Counter
	countMeter metrics.Meter
	sizeMeter  metrics.Meter
}

func newTickScanMeter(interval time.Duration) *tickScanMeter {
	// Enable metrics package.
	// Won't work without this global setting.
	metrics.Enabled = true

	reg := metrics.NewRegistry()
	rl := &tickScanMeter{
		reg:        reg,
		interval:   interval,
		started:    time.Now(),
		nn:         atomic.Uint64{},
		cats:       []string{},
		count:      metrics.NewCounter(),
		size:       metrics.NewCounter(),
		countMeter: metrics.NewMeter(),
		sizeMeter:  metrics.NewMeter(),
	}

	if err := reg.Register("count.count", rl.count); err != nil {
		panic(err)
	}
	if err := reg.Register("size.count", rl.size); err != nil {
		panic(err)
	}
	if err := reg.Register("line.meter", rl.countMeter); err != nil {
		panic(err)
	}
	if err := reg.Register("size.meter", rl.sizeMeter); err != nil {
		panic(err)
	}
	rl.nn.Store(0)
	go rl.run()
	return rl
}

func (rl *tickScanMeter) mark(label time.Time, data []byte) {
	rl.label = label
	rl.nn.Add(1)
	rl.count.Inc(1)
	rl.size.Inc(int64(len(data)))
	rl.countMeter.Mark(1)
	rl.sizeMeter.Mark(int64(len(data)))
}

func (rl *tickScanMeter) addCat(cat string) {
	// add this cat to the slice and safegaurd bad coding dupe adds
	for _, c := range rl.cats {
		if c == cat {
			return
		}
	}
	rl.cats = append(rl.cats, cat)
}

func (rl *tickScanMeter) dropCat(cat string) {
	// delete this cat from the slice
	for i, c := range rl.cats {
		if c == cat {
			rl.cats = append(rl.cats[:i], rl.cats[i+1:]...)
			break
		}
	}
}

func (rl *tickScanMeter) run() {
	rl.ticker = time.NewTicker(rl.interval)
	for range rl.ticker.C {
		rl.log()
	}
}

func (rl *tickScanMeter) log() {

	countSnap := rl.countMeter.Snapshot()
	sizeSnap := rl.sizeMeter.Snapshot()

	slog.Info("Read tracks", "n", humanize.Comma(countSnap.Count()),
		"cats", strings.Join(rl.cats, ","),
		"read.last", rl.label.Format(time.DateTime),
		"tps", common.DecimalToFixed(countSnap.Rate1(), 0),
		"bps", humanize.Bytes(uint64(sizeSnap.Rate1())),
		"total.bytes", humanize.Bytes(uint64(sizeSnap.Count())),
		"running", time.Since(rl.started).Round(time.Second))
}

func (rl *tickScanMeter) stop() {
	if rl == nil || rl.ticker == nil {
		return
	}
	rl.ticker.Stop()
	rl.countMeter.Stop()
	rl.sizeMeter.Stop()
}
