module github.com/rotblauer/catd

go 1.22.2

require (
	github.com/aws/aws-sdk-go v1.55.5
	github.com/dustin/go-humanize v1.0.1
	github.com/ethereum/go-ethereum v1.14.11
	github.com/golang/geo v0.0.0-20230421003525-6adc56603217
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da
	github.com/gorilla/handlers v1.5.2
	github.com/gorilla/mux v1.8.1
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/influxdata/influxdb-client-go/v2 v2.4.0
	github.com/jellydator/ttlcache/v3 v3.3.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/hashstructure/v2 v2.0.2
	github.com/montanaflynn/stats v0.0.0-20171201202039-1bf9dbcd8cbe
	github.com/olahol/melody v1.2.1
	github.com/paulmach/orb v0.11.1
	github.com/regnull/kalman v0.0.0-20200908141424-10753ec93999
	github.com/sams96/rgeo v1.2.0
	github.com/shopspring/decimal v1.4.0
	github.com/spf13/cobra v1.8.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.19.0
	github.com/tidwall/gjson v1.18.0
	go.etcd.io/bbolt v1.3.11
)

replace github.com/sams96/rgeo => github.com/rotblauer/rgeo v0.0.0-20241229144427-38ee53b6fd46

//replace github.com/regnull/kalman => github.com/rotblauer/rkalman v0.0.0-20241229172425-96c538931353

//replace github.com/sams96/rgeo => /home/ia/dev/sams96/rgeo
replace github.com/regnull/kalman => /home/ia/dev/regnull/kalman

require (
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/deepmap/oapi-codegen v1.6.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/holiman/uint256 v1.3.1 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/influxdata/line-protocol v0.0.0-20200327222509-2487e7298839 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/pelletier/go-toml/v2 v2.2.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sagikazarmark/locafero v0.4.0 // indirect
	github.com/sagikazarmark/slog-shim v0.1.0 // indirect
	github.com/shirou/gopsutil v3.21.4-0.20210419000835-c7a38de76ee5+incompatible // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spf13/afero v1.11.0 // indirect
	github.com/spf13/cast v1.6.0 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	go.mongodb.org/mongo-driver v1.11.4 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
	golang.org/x/exp v0.0.0-20231110203233-9a3e6036ecaa // indirect
	golang.org/x/net v0.24.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/sys v0.22.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	gonum.org/v1/gonum v0.15.1 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
