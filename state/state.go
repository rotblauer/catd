package state

import (
	"github.com/rotblauer/catd/catz"
	"go.etcd.io/bbolt"
	"sync"
)

type State struct {
	DB      *bbolt.DB
	Flat    *catz.Flat
	Waiting sync.WaitGroup
}
