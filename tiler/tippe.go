package tiler

import (
	"github.com/rotblauer/catd/catdb/flat"
	"github.com/rotblauer/catd/params"
	"io"
	"log"
	"log/slog"
	"os/exec"
	"strings"
)

func (d *Daemon) tip(source string, args params.CLIFlagsT) {
	r, w := io.Pipe()

	go func() {
		defer w.Close()

		reader, err := flat.NewFlatGZReader(source)
		if err != nil {
			d.logger.Error("Failed to open source file", "error", err)
			return
		}
		defer reader.Close()

		_, err = io.Copy(w, reader.Reader())
		if err != nil {
			d.logger.Error("Failed to copy laps edge file", "error", err)
		}
	}()

	err := tipFromReader(r, args)
	if err != nil {
		d.logger.Error("Failed to run tippe laps", "error", err)
	}
}

func tipFromReader(reader io.Reader, args params.CLIFlagsT) error {
	tippe := exec.Command(params.TippecanoeCommand, args...)
	stdin, err := tippe.StdinPipe()

	go func() {
		defer stdin.Close()
		n, err := io.Copy(stdin, reader)
		if err != nil {
			slog.Warn("Failed to copy reader to tippe", "error", err)
			return
		}
		slog.Info("Copied tippe data", "bytes", n)
	}()

	out, err := tippe.CombinedOutput()
	if out != nil {
		// Log output line by line
		for _, line := range strings.Split(string(out), "\n") {
			log.Println("tippe |", line)
		}
	}
	return err
}
