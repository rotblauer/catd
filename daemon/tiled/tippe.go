package tiled

import (
	"bufio"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/rotblauer/catd/catz"
	"github.com/rotblauer/catd/params"
	"io"
	"log"
	"os/exec"
	"strings"
)

func (d *TileDaemon) tip(args *TilingRequestArgs, sources ...string) error {
	r, w := io.Pipe()

	tipErrs := make(chan error, 1)
	go func() {
		defer close(tipErrs)
		tipErrs <- d.tipFromReader(r, args)
	}()

	pipeErrs := make(chan error, 1)
	go func() {
		defer w.Close()
		defer close(pipeErrs)

		d.logger.Info("Tipping...", "source", sources)

		// For each source, open the file and copy (pipe) it to the tippecanoe r/w.
		for _, source := range sources {
			reader, err := catz.NewGZFileReader(source)
			if err != nil {
				d.logger.Error("tip open failed to open source file", "error", err)
				select {
				case pipeErrs <- err:
				default:
				}
				return
			}

			// Copy will not return an EOF as an error.
			_, err = io.Copy(w, reader)

			// Handle the copy errors.
			if err != nil {
				reader.MaybeClose()
				d.logger.Error("tip failed to pipe source gz file", "source", source, "error", err)
				select {
				case pipeErrs <- err:
				default:
				}
				return
			}

			// Close the reader before handling Copy errors.
			if err := reader.Close(); err != nil {
				d.logger.Error("tip failed to close source file reader", "source", source, "error", err)
				select {
				case pipeErrs <- err:
				default:
				}
				return
			}
		}
		pipeErrs <- nil
	}()

	for _, errCh := range []chan error{tipErrs, pipeErrs} {
		select {
		case err := <-errCh:
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// tipFromReader call the tippecanoe command with the given reader as input,
// piping the reader to the tippecanoe command stdin.
func (d *TileDaemon) tipFromReader(reader io.Reader, args *TilingRequestArgs) error {
	tippe := exec.Command(params.TippecanoeCommand, args.cliArgs...)
	stdin, err := tippe.StdinPipe()
	if err != nil {
		return err
	}

	go func() {
		defer stdin.Close()
		n, err := io.Copy(stdin, reader)
		if err != nil {
			d.logger.Warn("Failed to copy reader to tippe", "source", args.id(), "error", err)
			return
		}
		d.logger.Debug("Piped gz data to tippecanoe", "source", args.id(), "size", humanize.Bytes(uint64(n)))
	}()

	log.Println(fmt.Sprintf("+ %s %s", tippe.Path, strings.Join(tippe.Args, " ")))
	tippeStderr, _ := tippe.StderrPipe()

	tippeErr := make(chan error, 1)
	go func() {
		defer close(tippeErr)
		scanner := bufio.NewScanner(tippeStderr)
		for scanner.Scan() {
			log.Println(fmt.Sprintf("++ %s %s", scanner.Text(), args.id()))
		}
		tippeErr <- tippe.Wait()
	}()

	err = tippe.Start()
	if err != nil {
		return err
	}
	return <-tippeErr
}
