package rgeo

import (
	"fmt"
	"github.com/sams96/rgeo"
	"log/slog"
	"slices"
)

var DBName = "rgeo.db"

//var R_Cities *rgeo.Rgeo
//var R_Countries *rgeo.Rgeo
//var R_Provinces *rgeo.Rgeo
//var R_US_Counties *rgeo.Rgeo

var r *rgeo.Rgeo

func R(dataset string) *rgeo.Rgeo {
	// if r == nil {
	// WARN: This takes forever. Init first.
	//
	// *Rgeo contains indices representing collection of datasets.
	// These datasets will be/can be reduced to a single Location value.
	// Also, the backing s2 calls Query function is not threadsafe.
	// Ye be warned.
	return r
}

func init() {}

var ErrAlreadyInitialized = fmt.Errorf("rgeo already initialized")

func DoInit() error {
	if r != nil {
		return ErrAlreadyInitialized
	}
	var err error
	slog.Info("Initializing rgeo...")
	r, err = rgeo.New(rgeo.Cities10, rgeo.Countries10, rgeo.Provinces10, rgeo.US_Counties10)
	if err != nil {
		return err
	}

	// Assert that exported DatasetNamesStable matches actual.
	names := r.DatasetNames()
	if !slices.Equal(DatasetNamesStable, names) {
		return fmt.Errorf("DatasetNamesStable does not match actual")
	}
	return nil
}
