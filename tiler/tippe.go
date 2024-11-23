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

	pipeErrs := make(chan error)
	go func() {
		defer w.Close()
		defer close(pipeErrs)

		d.logger.Info("Tipping...", "source", sources)

		// For each source, open the file and copy (pipe) it to the tippecanoe r/w.
		for _, source := range sources {
			reader, err := flat.NewFlatGZReader(source)
			if err != nil {
				d.logger.Error("tip open failed to open source file", "error", err)
				select {
				case pipeErrs <- err:
				default:
				}
				return
			}

			rr := reader.Reader()

			// Copy will not return an "expected" EOF.
			_, err = io.Copy(w, rr)

			// Close the reader before handling Copy errors.
			if err := rr.Close(); err != nil {
				d.logger.Error("tip failed to close source file reader", "source", source, "error", err)
				select {
				case pipeErrs <- err:
				default:
				}
				return
			}

			// Handle the copy errors.
			if err != nil {
				d.logger.Error("tip failed to pipe source gz file", "source", source, "error", err)
				select {
				case pipeErrs <- err:
				default:
				}
				return
			}
		}
	}()

	tipErrs := make(chan error)
	go func() {
		tipErrs <- d.tipFromReader(r, args)
	}()

	for {
		// Listen for tippe first.
		select {
		case err := <-tipErrs:
			if err != nil {
				return err
			}
		}

		// Then catch IO errors.
		select {
		case err := <-pipeErrs:
			return err
		}
	}
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
	log.Println(fmt.Sprintf("+ %s %s", tippe.Path, strings.Join(tippe.Args, " ")))
	if out != nil {
		// Log output line by line
		for _, line := range strings.Split(string(out), "\n") {
			if line == "" {
				continue
			}
			log.Println(line)
		}
	}
	return err
}
