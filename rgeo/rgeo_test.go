package rgeo

import (
	"encoding/json"
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/params"
	srgeo "github.com/sams96/rgeo"
	"log"
	"net"
	"net/rpc"
	"os"
	"slices"
	"testing"
)

func TestDatasetNamesStable(t *testing.T) {
	names := DatasetNamesStable
	if len(names) != len(defaultDatasets) {
		t.Errorf("Expected default-n names, got %d", len(names))
	}
	if !slices.IsSorted(names) {
		t.Errorf("Expected sorted names, got %v", names)
	}
}

func TestR(t *testing.T) {
	defer func() {
		r = nil
	}()
	t.Run("testRLoadBasic", testRLoadBasic)
	t.Run("testrRPCConf", testrRPCConf)
	//t.Run("testRLoadEmptyrRPC", testRLoadEmptyrRPC)
	//t.Run("testrRPC", testrRPC)
	t.Run("testRLoadBasic", testRLoadBasic)
}

func testRLoadBasic(t *testing.T) {
	defer func() {
		// Destroy global r, unload instance.
		r = nil
		_ = r
	}()

	// Test that R() returns a non-nil value, and is the primary fallback type:
	// a wrapped library instance rR aliasing srgeo.Rgeo.
	our := R()
	if our == nil {
		t.Fatal("Expected non-nil value from R()")
	}
	switch our.(type) {
	case *rR:
	default:
		t.Log("Is catd rgeod already running somewhere else?")
		t.Fatalf("Expected *rR, got %T", our)
	}
	if our != r {
		t.Fatalf("Expected %p, got %p", r, our)
	}

	// Unwrap instance and check lib methods.
	rr := our.(*rR)
	lib := (*srgeo.Rgeo)(rr)
	libNames := lib.DatasetNames()
	if !slices.Equal(DatasetNamesStable, libNames) {
		t.Fatalf("Expected/Got\n%v\n%v", DatasetNamesStable, libNames)
	}
}

func testrRPCConf(t *testing.T) {
	// Init a private instance for backing mock with real data.
	r1, err := srgeo.New(Provinces10)
	if err != nil {
		t.Fatal(err)
	}
	m := &MockRPCServer{r: (*rR)(r1)}
	server := rpc.NewServer()
	err = server.Register(m)
	if err != nil {
		t.Fatal(err)
	}
	//Start server on a temporary unix socket file.
	sock := "/tmp/rgeo.sock_tmp"
	a := &net.UnixAddr{
		Name: sock,
		Net:  "unix",
	}
	defer os.Remove(sock)
	os.Remove(sock)
	l, err := net.ListenUnix("unix", a)
	if err != nil {
		t.Fatal(err)
	}
	go server.Accept(l)

	// Configure global rRPC, causing R() to return this client.
	mockConf := func() *params.RgeoDaemonConfig {
		return &params.RgeoDaemonConfig{
			ListenerConfig: params.ListenerConfig{
				Network: "unix",
				Address: sock,
			},
			ServiceName: "MockRPCServer",
			RPCPath:     params.DefaultRgeoDaemonConfig().RPCPath,
		}
	}

	// THE TEST
	// Assign the global configuration value.
	// R() should attempt to create a client with this configuration
	// with NewRPCReverseGeocoderClient.
	// If it fails, it will slog a Warn and fall back to the library.
	rRPCConf = mockConf()
	//if rRPC != nil {
	//	// rRPC should be uninitialized; we're testing that R() will assign it.
	//	t.Fatal("Expected nil value for rRPC")
	//}

	// Call R() again.
	our := R()
	if our == nil {
		t.Fatal("Expected non-nil value from R()")
	}
	switch our.(type) {
	case *RPCReverseGeocoderClient:
	default:
		t.Fatalf("Expected *RPCReverseGeocoderClient, got %T", our)
	}
	// Test the methods.
	ourR, ok := our.(*RPCReverseGeocoderClient)
	if !ok {
		t.Fatalf("Expected *RPCReverseGeocoderClient, got %T", our)
	}

	// Check get reverse geocoded location.
	// Mock server has real data backing.
	// 47°24'20.2"N 105°35'24.9"W is 47.405611, -105.590250
	loc, err := ourR.GetLocation(Pt{-105.590250, 47.405611})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if loc.CountryCode3 != "USA" || loc.Province != "Montana" {
		t.Fatalf("Want country=%q province=%q, got: country=%q province=%q",
			"USA", "Montana", loc.CountryCode3, loc.Province)
	}

	// Test that the client has been closed.
	// RPC clients are single-use, disposable.
	// Slow, but mem safe that way.
	if err := ourR.Close(); err == nil || err.Error() != "connection is shut down" {
		t.Fatalf("Expected error 'connection is shut down', got %q", err)
	}

	// Get another one and use it without type assertion.
	ourR2 := R()
	switch ourR2.(type) {
	case *RPCReverseGeocoderClient:
	default:
		t.Fatalf("Expected *RPCReverseGeocoderClient, got %T", ourR2)
	}
	// Test the methods.
	ourR2T, ok := ourR2.(*RPCReverseGeocoderClient)
	if !ok {
		t.Fatalf("Expected *RPCReverseGeocoderClient, got %T", our)
	}

	// Check get geometry for some location/dataset.
	plat, err := ourR2T.GetGeometry(Pt{-105.590250, 47.405611}, "github.com/sams96/rgeo.Provinces10")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if plat != nil {
		if plat.Polygon == nil && plat.MultiPolygon == nil {
			t.Fatal("Expected non-nil Polygon or MultiPolygon")
		}
		if plat.Polygon == nil {
			t.Fatal("Expected non-nil Polygon")
		}
		if len(plat.Polygon) != 1 {
			t.Errorf("Expected 1 rings, got %d", len(plat.Polygon))
		}
		// Want: -116.048163,48.992515
		if plat.Polygon[0][0][0] != -116.048163 || plat.Polygon[0][0][1] != 48.992515 {
			t.Errorf("Expected 41,42, got %f,%f", plat.Polygon[0][0][0], plat.Polygon[0][0][1])
		}
	} else {
		t.Errorf("Expected non-nil geometry")
	}

	//err = rRPC.Close()
	if err != nil {
		t.Fatal(err)
	}

	l.Close()
	if t.Failed() {
		t.FailNow()
	}
	t.Log("Mock ReverseGeocode RPC server works. Truly amazing.")

	// De-configure and destroy.
	rRPCConf = nil
	//rRPC = nil
}

//func testRLoadEmptyrRPC(t *testing.T) {
//	defer func() {
//		rRPC = nil
//	}()
//
//	// Reconfigure with fake/zero value.
//	rRPC = &RPCReverseGeocoderClient{}
//	our := R()
//	if our == nil {
//		t.Fatal("Expected non-nil value from R()")
//	}
//	switch our.(type) {
//	case *RPCReverseGeocoderClient:
//	default:
//		t.Fatalf("Expected *RPCReverseGeocoderClient, got %T", our)
//	}
//	// our.(*RPCReverseGeocoderClient).Close()
//	// A zero instance will fail to close.
//}

//// testrRPC runs the following:
////
//// Implement an alias RPCServer satisfying the
//// ReverseGeocoder interface, then start
//// a server on a temporary unix socket file.
//// Assign rRPC with a working, adhoc RPC client (around the mock).
//// Call R() again, expecting to get this working server.
//// Test the methods to assert mock responses.
//// Close.
//func testrRPC(t *testing.T) {
//	old := rRPCConf
//	rRPCConf = nil
//	defer func() {
//		rRPC = nil
//		rRPCConf = old
//	}()
//
//	// Init a private instance for backing mock with real data.
//	r1, err := srgeo.New(Provinces10)
//	if err != nil {
//		t.Fatal(err)
//	}
//	m := &MockRPCServer{r: (*rR)(r1)}
//	server := rpc.NewServer()
//	err = server.Register(m)
//	if err != nil {
//		t.Fatal(err)
//	}
//	//Start server on a temporary unix socket file.
//	sock := "/tmp/rgeo.sock_tmp"
//	a := &net.UnixAddr{
//		Name: sock,
//		Net:  "unix",
//	}
//	defer os.Remove(sock)
//	os.Remove(sock)
//	l, err := net.ListenUnix("unix", a)
//	if err != nil {
//		t.Fatal(err)
//	}
//	go server.Accept(l)
//
//	// Configure global rRPC, causing R() to return this client.
//	mockConf := func() *params.RgeoDaemonConfig {
//		return &params.RgeoDaemonConfig{
//			ListenerConfig: params.ListenerConfig{
//				Network: "unix",
//				Address: sock,
//			},
//			Name:    "MockRPCServer",
//			RPCPath: params.DefaultRgeoDaemonConfig().RPCPath,
//		}
//	}
//
//	// THE TEST
//	// Assign the global value. R() should prioritize this, if it works.
//	// If assigned, it means there's already an RPC client ready for requests
//	// to a remote service.
//	// We're using a "custom"/non-R()'d RPC client.
//	if rRPCConf != nil {
//		t.Fatal("Expected nil value for rRPCConf")
//	}
//	rRPC, err = NewRPCReverseGeocoderClient(mockConf())
//
//	// Call R() again.
//	our := R()
//	if our == nil {
//		t.Fatal("Expected non-nil value from R()")
//	}
//	// our is == rRPC; compare pointers.
//	if our != rRPC {
//		t.Fatalf("Expected %p, got %p", rRPC, our)
//	}
//
//	switch our.(type) {
//	case *RPCReverseGeocoderClient:
//	default:
//		t.Fatalf("Expected *RPCReverseGeocoderClient, got %T", our)
//	}
//	// Test the methods.
//	ourR, ok := our.(*RPCReverseGeocoderClient)
//	if !ok {
//		t.Fatalf("Expected *RPCReverseGeocoderClient, got %T", our)
//	}
//
//	// Check get reverse geocoded location.
//	// Mock server has real data backing.
//	// 47°24'20.2"N 105°35'24.9"W is 47.405611, -105.590250
//	loc, err := ourR.GetLocation(Pt{-105.590250, 47.405611})
//	if err != nil {
//		t.Errorf("Unexpected error: %v", err)
//	}
//	if loc.CountryCode3 != "USA" || loc.Province != "Montana" {
//		t.Fatalf("Want country=%q province=%q, got: country=%q province=%q",
//			"USA", "Montana", loc.CountryCode3, loc.Province)
//	}
//
//	// Check get geometry for some location/dataset.
//	plat, err := ourR.GetGeometry(Pt{-105.590250, 47.405611}, "github.com/sams96/rgeo.Provinces10")
//	if err != nil {
//		t.Errorf("Unexpected error: %v", err)
//	}
//	if plat != nil {
//		if plat.Polygon == nil && plat.MultiPolygon == nil {
//			t.Fatal("Expected non-nil Polygon or MultiPolygon")
//		}
//		if plat.Polygon == nil {
//			t.Fatal("Expected non-nil Polygon")
//		}
//		if len(plat.Polygon) != 1 {
//			t.Errorf("Expected 1 rings, got %d", len(plat.Polygon))
//		}
//		// Want: -116.048163,48.992515
//		if plat.Polygon[0][0][0] != -116.048163 || plat.Polygon[0][0][1] != 48.992515 {
//			t.Errorf("Expected 41,42, got %f,%f", plat.Polygon[0][0][0], plat.Polygon[0][0][1])
//		}
//	} else {
//		t.Errorf("Expected non-nil geometry")
//	}
//
//	err = rRPC.Close()
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	l.Close()
//	if t.Failed() {
//		t.FailNow()
//	}
//	t.Log("Mock ReverseGeocode RPC server works. Truly amazing.")
//}

// MockRPCServer mocks daemon/rgeo/daemon.go#ReverseGeocoder.
type MockRPCServer struct {
	r *rR // cool but who cares
}

func (m *MockRPCServer) GetLocation(request *GetLocationRequest, response *GetLocationResponse) error {
	pt := orb.Point{0, 0}
	if request != nil {
		pt = orb.Point{request[0], request[1]}
	}
	if m.r != nil {
		p := Pt{pt[0], pt[1]}
		loc, err := m.r.GetLocation(p)
		if err != nil {
			if response == nil {
				return err
			}
			response.Location = loc
			response.Error = err.Error()
			return nil
		}
		response.Location = loc

		// Show our tester the call and response.
		reqj, _ := json.MarshalIndent(request, "", "  ")
		resj, _ := json.MarshalIndent(response, "", "  ")
		log.Printf("RPC reqj:\n%s\n", string(reqj))
		log.Printf("RPC response:\n%s\n", string(resj))
		return nil
	}
	r, err := m.fakeGetLocation(pt)
	if err != nil {
		return err
	}
	if response == nil {
		return err
	}
	response.Location = r
	return nil
}

func (m *MockRPCServer) fakeGetLocation(pt orb.Point) (srgeo.Location, error) {
	return srgeo.Location{
		Country:      "",
		CountryLong:  "",
		CountryCode2: "",
		CountryCode3: "JPN",
		Continent:    "",
		Region:       "",
		SubRegion:    "",
		Province:     "Hokkaido",
		ProvinceCode: "",
		County:       "",
		City:         "",
	}, nil
}

func (m MockRPCServer) GetGeometry(request *GetGeometryRequest, response *GetGeometryResponse) error {
	pt := orb.Point{0, 0}
	dataset := "Countries"
	if request != nil {
		pt = orb.Point{request.Pt[0], request.Pt[1]}
		dataset = request.Dataset
	}
	if m.r != nil {
		p := Pt{pt[0], pt[1]}
		plat, err := m.r.GetGeometry(p, dataset)
		if err != nil {
			if response == nil {
				return err
			}
			response.Plat = plat
			response.Error = err.Error()
			return nil
		}
		response.Plat = plat

		// Show our tester the call and response.
		reqj, _ := json.MarshalIndent(request, "", "  ")
		resj, _ := json.Marshal(response)
		log.Printf("RPC reqj:\n%s\n", string(reqj))
		log.Printf("RPC response:\n%s\n", string(resj[:100])+"...")
		return nil
	}
	r, err := m.fakeGetGeometry(pt, dataset)
	if err != nil {
		return err
	}
	if response == nil {
		return nil
	}
	response.Plat = &Plat{Polygon: r.(orb.Polygon)}
	return nil
}

func (m MockRPCServer) fakeGetGeometry(pt orb.Point, dataset string) (orb.Geometry, error) {
	return orb.Polygon{orb.Ring{orb.Point{42, 42}}}, nil
}
