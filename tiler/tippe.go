package tiler

import (
	"github.com/rotblauer/catd/params"
	"io"
	"log"
	"log/slog"
	"os/exec"
	"strings"
)

func RunTippe(reader io.Reader, args params.CLIFlagsT) error {
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
