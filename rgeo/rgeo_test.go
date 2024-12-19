package rgeo

import (
	"github.com/paulmach/orb"
	"github.com/rotblauer/catd/params"
	"github.com/sams96/rgeo"
	"net"
	"net/rpc"
	"os"
	"testing"
)

func TestDatasetNamesStable(t *testing.T) {
	names := DatasetNamesStable
	if len(names) != 4 {
		t.Errorf("Expected 4 names, got %d", len(names))
	}
	if want := "github.com/sams96/rgeo.Cities10"; names[0] != want {
		t.Errorf("Expected %q, got %s", want, names[0])
	}
	if want := "github.com/sams96/rgeo.Countries10"; names[1] != want {
		t.Errorf("Expected %q, got %s", want, names[1])
	}
	if want := "github.com/sams96/rgeo.Provinces10"; names[2] != want {
		t.Errorf("Expected %q, got %s", want, names[2])
	}
	if want := "github.com/sams96/rgeo.US_Counties10"; names[3] != want {
		t.Errorf("Expected %q, got %s", want, names[3])
	}
}

// MockRPCServer mocks daemon/rgeo/daemon.go#ReverseGeocoder.
type MockRPCServer struct{}

func (m MockRPCServer) GetLocation(request *GetLocationRequest, locationRequest *GetLocationResponse) error {
	pt := orb.Point{0, 0}
	if request != nil {
		pt = orb.Point{request[0], request[1]}
	}
	r, err := m.getLocation(pt)
	if err != nil {
		return err
	}
	if locationRequest == nil {
		return nil
	}
	locationRequest.Location = r
	return nil
}

func (m MockRPCServer) getLocation(pt orb.Point) (rgeo.Location, error) {
	return rgeo.Location{
		Country:      "USA",
		CountryLong:  "",
		CountryCode2: "",
		CountryCode3: "",
		Continent:    "",
		Region:       "",
		SubRegion:    "",
		Province:     "Montany",
		ProvinceCode: "",
		County:       "",
		City:         "",
	}, nil
}

func (m MockRPCServer) GetGeometry(request *GetGeometryRequest, geometryRequest *GetGeometryResponse) error {
	pt := orb.Point{0, 0}
	dataset := "Countries"
	if request != nil {
		pt = orb.Point{request.Pt[0], request.Pt[1]}
		dataset = request.Dataset
	}
	r, err := m.getGeometry(pt, dataset)
	if err != nil {
		return err
	}
	if geometryRequest == nil {
		return nil
	}
	geometryRequest.Plat = Geometry2Plat(r)
	return nil
}

func (m MockRPCServer) getGeometry(pt orb.Point, dataset string) (orb.Geometry, error) {
	return orb.Polygon{orb.Ring{orb.Point{42, 42}}}, nil
}

func TestR(t *testing.T) {
	defer func() {
		r = nil
	}()

	// Test that R() returns a non-nil value.
	//t.Skip("Takes 20 seconds")
	our := R()
	if our == nil {
		t.Fatal("Expected non-nil value from R()")
	}
	switch our.(type) {
	case *rR:
	default:
		t.Fatalf("Expected *rR, got %T", our)
		t.Log("Is catd rgeod already running?")
	}

	// Destroy global r, unload instance.
	r = nil
	_ = r

	// Reconfigure.
	rRPC = &RPCReverseGeocoderClient{}
	our = R()
	if our == nil {
		t.Fatal("Expected non-nil value from R()")
	}
	switch our.(type) {
	case *RPCReverseGeocoderClient:
	default:
		t.Fatalf("Expected *RPCReverseGeocoderClient, got %T", our)
	}
	// our.(*RPCReverseGeocoderClient).Close()
	// A zero instance will fail to close.

	// Destroy.
	rRPC = nil

	//Implement an alias RPCServer satisfying the
	//ReverseGeocoder interface, then start
	//a server on a temporary unix socket file.
	//Set rRPCConf with the test values, and
	//Call R() again, expecting a working server.
	//Test the methods to assert mock responses.
	//Close.
	m := &MockRPCServer{}
	server := rpc.NewServer()
	err := server.Register(m)
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

	// Configure global, causing R() to return a new client.
	rRPCConf = &params.RgeoDaemonConfig{
		ListenerConfig: params.ListenerConfig{
			Network: "unix",
			Address: sock,
		},
		Name:    "MockRPCServer",
		RPCPath: params.DefaultRgeoDaemonConfig().RPCPath,
	}

	// Call R() again.
	our = R()
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
	loc, err := ourR.GetLocation(Pt{0, 0})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if loc.Country != "USA" {
		t.Errorf("Expected USA, got %s", loc.Country)
	}
	geo, err := ourR.GetGeometry(Pt{0, 0}, "test")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if geo != nil {
		// Is polygon. Could check type.
	} else {
		t.Errorf("Expected non-nil geometry")
	}

	t.Log("Mock ReverseGeocode RPC server works.")
	l.Close()

	if t.Failed() {
		t.FailNow()
	}

	// De-configure and destroy.
	rRPCConf = nil
	rRPC = nil

	// Finally, call R() one last time and make
	// sure we spend another 20 seconds initializing
	// a dataset of our own.
	our = R()
	if our == nil {
		t.Fatal("Expected non-nil value from R()")
	}
	switch our.(type) {
	case *rR:
	default:
		t.Fatalf("Expected *rR, got %T", our)
	}
}
