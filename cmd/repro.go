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
	"github.com/rotblauer/catd/api"
	"github.com/rotblauer/catd/conceptual"
	"github.com/rotblauer/catd/daemon/tiled"
	"github.com/rotblauer/catd/params"
	"github.com/spf13/cobra"
	"log/slog"
	"time"
)

// reproCmd represents the repro command
var reproCmd = &cobra.Command{
	Use:   "repro",
	Short: "Reproduce cat producer pipelines",
	Long: `Regenerate synthetic data for a cat, using the cat's producer pipelines.

1. Remove cat's laps.geojson.gz and naps.geojson.gz files. 
2. Read tracks from cat/tracks.geojson.gz pipe them into 
   the producer pipeline. This will regenerate laps and naps.

Basically this is Populate, but skipping the filtering, validation, sanitization, 
sorting, and storage steps. Just the fun stuff.

`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			return
		}
		slog.Info("Reproducing pipelines for cats", "cats", args)
		dConfig := params.DefaultTileDaemonConfig()
		dConfig.TilingPendingExpiry = 1 * time.Hour
		dConfig.SkipEdge = true
		dConfig.AwaitPendingOnShutdown = true
		d := tiled.NewDaemon(dConfig)
		if err := d.Run(); err != nil {
			panic(err)
		}
		for i, kitty := range args {
			slog.Info("Reproducing pipelines for cat", "cat", kitty, "i", i, "n", len(args))
			cat, err := api.NewCat(conceptual.CatID(kitty), d.Config)
			if err != nil {
				panic(err)
			}
			if _, err := cat.WithState(false); err != nil {
				panic(err)
			}
			if err := cat.ReproducePipelines(); err != nil {
				panic(err)
			}
			cat.Close()
			slog.Info("Reproduced pipelines for cat", "cat", kitty, "i", i, "n", len(args))
		}
		d.Stop()
		slog.Info("Waiting for daemon to stop")
		d.Wait()
		slog.Info("Daemon stopped")
	},
}

func init() {
	rootCmd.AddCommand(reproCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// reproCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// reproCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
