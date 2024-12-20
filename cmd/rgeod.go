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

		config := params.InProcRgeoDaemonConfig

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

	// This flagset is shared with other commands,
	// and writes to the same configuration structures.
	rgeodListenerFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", rgeodCmd.CommandPath())
	}
	rgeodListenerFlags.StringVar(&params.InProcRgeoDaemonConfig.Network,
		"rgeod.listen.network", params.InProcRgeoDaemonConfig.Network,
		`Network to listen on
This flag configures a public inproc configuration structure instance.`)

	rgeodListenerFlags.StringVar(&params.InProcRgeoDaemonConfig.Address,
		"rgeod.listen.address", params.InProcRgeoDaemonConfig.Address,
		`Address to listen on
This flag configures a public inproc configuration structure instance.`)

	rgeodListenerFlags.StringVar(&params.InProcRgeoDaemonConfig.ServiceName,
		"rgeod.serviceName", params.InProcRgeoDaemonConfig.ServiceName,
		`RPC service name
This is used as MyServiceName.MethodName in RPC calls.
This flag configures a public inproc configuration structure instance.`)

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
