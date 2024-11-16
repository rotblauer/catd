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
	"context"
	"encoding/json"
	"github.com/rotblauer/catd/api"
	"github.com/rotblauer/catd/types/cattrack"
	"github.com/spf13/cobra"
	"io"
	"log/slog"
	"os"
)

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import cat tracks from stdin",
	Long:  `Scans geojson.Feature lines from stdin and passes them to api.Populate.`,
	Run: func(cmd *cobra.Command, args []string) {

		ctx := context.Background()
		send := make(chan *cattrack.CatTrack)

		go func() {
			err := api.Populate(ctx, send)
			if err != nil {
				slog.Error("Failed to populate CatTracks", "error", err)
			}
		}()

		dec := json.NewDecoder(os.Stdin)
		for {
			ct := &cattrack.CatTrack{}
			if err := dec.Decode(ct); err == io.EOF {
				break
			} else if err != nil {
				slog.Error("Failed to decode CatTrack", "error", err)
				continue
			}
			select {
			case <-ctx.Done():
				return
			case send <- ct:
			}
		}

		close(send)
	},
}

func init() {
	rootCmd.AddCommand(importCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// importCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// importCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
