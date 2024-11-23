package state

import (
	"github.com/rotblauer/catd/catdb/flat"
	"go.etcd.io/bbolt"
	"sync"
)

const appDBName = "app.db"

type State struct {
	DB      *bbolt.DB
	Flat    *flat.Flat
	Waiting sync.WaitGroup
	rOnly   bool
}
