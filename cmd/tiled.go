/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"fmt"
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/params"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"log"
	"log/slog"
	"os"
	"time"
)

// TODO Expose daemon config to flags.
// ... to RootCmd? cmd/populate, cmd/webd, cmd/tiled

var optTilingListenNetwork = params.DefaultTileDaemonConfig().Network
var optTilingListenAddress = params.DefaultTileDaemonConfig().Address
var optTilingSkipEdge bool
var optTilingPendingExpiry time.Duration
var optTilingAwaitPending bool

// tiledCmd represents the tiled command
var tiledCmd = &cobra.Command{
	Use:   "tiled",
	Short: "",
	Long: `TileD is the RPC tiling daemon.

It accepts data (source) pushes and tiling requests.
Tiling requests are processed in the background.
Pushes will trigger internal tiling requests. (FIXME: Configurable.) 
`,
	Run: func(cmd *cobra.Command, args []string) {
		setDefaultSlog(cmd, args)
		slog.Info("tiled.Run")

		config := params.DefaultTileDaemonConfig()
		config.ListenerConfig.Network = optTilingListenNetwork
		config.ListenerConfig.Address = optTilingListenAddress
		config.SkipEdge = optTilingSkipEdge
		config.TilingPendingExpiry = optTilingPendingExpiry
		config.AwaitPendingOnShutdown = optTilingAwaitPending

		d, err := tiled.NewDaemon(config)
		if err != nil {
			log.Fatalln(err)
		}
		if err := d.Start(); err != nil {
			log.Fatalln(err)
		}
		<-common.Interrupted()
		d.Interrupt()
		d.Wait()
	},
}

//var tiledRecoverCmd = &cobra.Command{
//	Use:   "recover",
//	Short: "Recover tiles from source",
//	Long:  ``,
//	Run: func(cmd *cobra.Command, args []string) {
//		setDefaultSlog(cmd, args)
//
//		config := params.DefaultTileDaemonConfig()
//		config.AwaitPendingOnShutdown = true
//		config.SkipEdge = true
//		d, err := tiled.NewDaemon(config)
//		if err != nil {
//			log.Fatalln(err)
//		}
//		if err := d.Start(); err != nil {
//			log.Fatalln(err)
//		}
//		if err := d.Recover(); err != nil {
//			log.Fatalln(err)
//		}
//		slog.Info("Stopping daemon")
//		d.Stop()
//		slog.Info("Waiting for daemon to stop")
//		d.Wait()
//	},
//}

var tiledListenerFlags = pflag.NewFlagSet("tiled.listen", pflag.ContinueOnError)

func init() {
	rootCmd.AddCommand(tiledCmd)

	flags := tiledCmd.Flags()

	tiledListenerFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", tiledCmd.CommandPath())
	}
	tiledListenerFlags.StringVar(&optTilingListenNetwork, "tiled.listen.network", optTilingListenNetwork, "Network to listen on")
	tiledListenerFlags.StringVar(&optTilingListenAddress, "tiled.listen.address", optTilingListenAddress, "Address to listen on")

	flags.AddFlagSet(tiledListenerFlags)

	flags.DurationVar(&optTilingPendingExpiry, "tiled.pending-after", 100*time.Hour,
		`Pending expiry interval for RequestTiling requests, aka "debounce" time.
A long interval will cause pending tiling requests
to (optionally) wait til daemon shutdown sequence.`)

	flags.BoolVar(&optTilingAwaitPending, "tiled.await-pending", true,
		`Await all pending tiling requests on shutdown.`)

	flags.BoolVar(&optTilingSkipEdge, "tiled.skip-edge", false,
		`Skip edge tiling.
All PushFeatures requests will have their version params overridden to Canonical. 
This is useful for development and initial runs, where long-deferred edge tiling 
could amass large amounts of edge data needlessly.`)

	// Both webd and populate commands can re-use the listener flags.
	// They want connections, and in the case of populate, to do an auto-start.
	webdCmd.Flags().AddFlagSet(tiledListenerFlags)
	populateCmd.Flags().AddFlagSet(tiledListenerFlags)
	populateCmd.Flags().AddFlagSet(flags)

	//tiledCmd.AddCommand(tiledRecoverCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// tiledCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// tiledCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
