package common

import (
	"os"
	"os/signal"
	"syscall"
)

func Interrupted() <-chan os.Signal {
	interrupt := make(chan os.Signal, 2)
	signal.Notify(interrupt,
		os.Interrupt, os.Kill,
		syscall.SIGTERM, syscall.SIGQUIT,
	)
	return interrupt
}
