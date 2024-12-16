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
	"github.com/rotblauer/catd/daemon/rgeod"
	"github.com/rotblauer/catd/params"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"log"
	"log/slog"
	"os"
)

var optRgeoDNetwork = params.DefaultRgeoDaemonConfig().Network
var optRgeoDAddress = params.DefaultRgeoDaemonConfig().Address

// rgeodCmd represents the rgeod command
var rgeodCmd = &cobra.Command{
	Use:   "rgeod",
	Short: "Run reverse geocode RPC daemon",
	Long: `RGeoD is the reverse-geocoder daemon.

It loads large datasets, and then looks places up for you.
`,
	Run: func(cmd *cobra.Command, args []string) {
		setDefaultSlog(cmd, args)
		slog.Info("rgeod.Run")

		config := params.DefaultRgeoDaemonConfig()
		config.ListenerConfig.Network = optRgeoDNetwork
		config.ListenerConfig.Address = optRgeoDAddress

		d, err := rgeod.NewDaemon(config)
		if err != nil {
			log.Fatalln(err)
		}
		if err := d.Start(); err != nil {
			log.Fatalln(err)
		}
		sig := <-common.Interrupted()
		slog.Info("rgeod interrupted", "signal", sig)
		d.Stop()
	},
}

var rgeodListenerFlags = pflag.NewFlagSet("rgeod.listen", pflag.ContinueOnError)

func init() {
	rootCmd.AddCommand(rgeodCmd)

	rgeodListenerFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", rgeodCmd.CommandPath())
	}
	rgeodListenerFlags.StringVar(&optRgeoDNetwork, "rgeod.listen.network", optRgeoDNetwork, "Network to listen on")
	rgeodListenerFlags.StringVar(&optRgeoDAddress, "rgeod.listen.address", optRgeoDAddress, "Address to listen on")
	rgeodCmd.Flags().AddFlagSet(rgeodListenerFlags)

	// Share this flagset with other commands.
	webdCmd.Flags().AddFlagSet(rgeodListenerFlags)
	populateCmd.Flags().AddFlagSet(rgeodListenerFlags)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// rgeodCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// rgeodCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
