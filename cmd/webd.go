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
	"github.com/rotblauer/catd/daemon/webd"
	"github.com/rotblauer/catd/params"
	"github.com/spf13/pflag"
	"log"
	"log/slog"

	"github.com/spf13/cobra"
)

var optHTTPAddr string
var optHTTPPort int

// webdCmd represents the serve command
var webdCmd = &cobra.Command{
	Use:   "webd",
	Short: "Start the webserver",
	Long:  `Serves cats on the internet`,
	Run: func(cmd *cobra.Command, args []string) {
		setDefaultSlog(cmd, args)
		slog.Info("webd.Run")
		backend := params.DefaultCatBackendConfig()
		server, err := webd.NewWebDaemon(&params.WebDaemonConfig{
			DataDir: params.DefaultDatadirRoot,
			ListenerConfig: params.ListenerConfig{
				Address: optHTTPAddr,
				Network: "tcp",
			},
			CatBackendConfig: backend,
		})
		if err != nil {
			log.Fatalln(err)
		}

		if err := server.Run(); err != nil {
			log.Fatalln(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(webdCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// webdCmd.PersistentFlags().String("foo", "", "A help for foo")
	defaults := params.DefaultWebDaemonConfig()

	pFlags := webdCmd.PersistentFlags()
	pFlags.AddFlagSet(&pflag.FlagSet{})
	pFlags.StringVar(&optHTTPAddr, "address", defaults.Address, "HTTP address to listen on")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// webdCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
