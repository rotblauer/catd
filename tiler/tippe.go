package tiler

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/params"
	"io"
	"log"
	"os/exec"
	"strings"
)

func (d *Daemon) tip(args params.CLIFlagsT, sources ...string) error {
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		for _, source := range sources {
			d.logger.Info("Tipping...", "source", sources)
			reader, err := flat.NewFlatGZReader(source)
			if err != nil {
				d.logger.Error("Failed to open source file", "error", err)
				return
			}
			defer reader.Close()

			_, err = io.Copy(w, reader.Reader())
			if err != nil {
				d.logger.Error("Failed to copy source gz file", "error", err)
			}
			reader.Close() // Dupe calls to close, but OK. Safer.
		}
	}()

	return d.tipFromReader(r, args)
}

func (d *Daemon) tipFromReader(reader io.Reader, args params.CLIFlagsT) error {
	tippe := exec.Command(params.TippecanoeCommand, args...)
	stdin, err := tippe.StdinPipe()

	go func() {
		defer stdin.Close()
		n, err := io.Copy(stdin, reader)
		if err != nil {
			d.logger.Warn("Failed to copy reader to tippe", "error", err)
			return
		}
		d.logger.Info("Piped gz data to tippecanoe", "size", humanize.Bytes(uint64(n)))
	}()

	out, err := tippe.CombinedOutput()
	if out != nil {
		// Log output line by line
		for _, line := range strings.Split(string(out), "\n") {
			if line == "" {
				continue
			}
			log.Println(fmt.Sprintf("+ %s %s", tippe.Path, strings.Join(tippe.Args, " ")))
			log.Println(line)
		}
	}
	return err
}
