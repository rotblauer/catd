package state

import (
	"github.com/rotblauer/catd/catz"
	"go.etcd.io/bbolt"
	"sync"
)

const appDBName = "app.db"

type State struct {
	DB      *bbolt.DB
	Flat    *catz.Flat
	Waiting sync.WaitGroup
	rOnly   bool
}
