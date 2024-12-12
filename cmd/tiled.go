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
	"github.com/rotblauer/catd/common"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/params"
	"github.com/spf13/cobra"
	"log"
	"log/slog"
)

// tiledCmd represents the tiled command
var tiledCmd = &cobra.Command{
	Use:   "tiled",
	Short: "Run the tiling daemon (HTTP RPC)",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		setDefaultSlog(cmd, args)

		config := params.DefaultTileDaemonConfig()
		d, err := tiled.NewDaemon(config)
		if err != nil {
			log.Fatalln(err)
		}
		if err := d.Run(); err != nil {
			log.Fatalln(err)
		}

		<-common.Interrupted()
		d.Stop()
		d.Wait()
	},
}

var tiledRecoverCmd = &cobra.Command{
	Use:   "recover",
	Short: "Recover tiles from source",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		setDefaultSlog(cmd, args)

		config := params.DefaultTileDaemonConfig()
		config.AwaitPendingOnShutdown = true
		config.SkipEdge = true
		d, err := tiled.NewDaemon(config)
		if err != nil {
			log.Fatalln(err)
		}
		if err := d.Run(); err != nil {
			log.Fatalln(err)
		}
		if err := d.Recover(); err != nil {
			log.Fatalln(err)
		}
		slog.Info("Stopping daemon")
		d.Stop()
		slog.Info("Waiting for daemon to stop")
		d.Wait()
	},
}

func init() {
	rootCmd.AddCommand(tiledCmd)
	tiledCmd.AddCommand(tiledRecoverCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// tiledCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// tiledCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
