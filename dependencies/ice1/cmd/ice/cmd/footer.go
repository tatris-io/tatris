//  Copyright (c) 2020 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var footerCmd = &cobra.Command{
	Use:   "footer [path]",
	Short: "prints the contents of the footer",
	Long:  `The footer command will print the contents of the footer.`,
	RunE: func(cmd *cobra.Command, args []string) error {

		// fmt.Printf("Length: %d\n", len(data))
		fmt.Printf("CRC: %#x\n", seg.CRC())
		fmt.Printf("Version: %d\n", seg.Version())
		fmt.Printf("Chunk Mode: %d\n", seg.ChunkMode())
		fmt.Printf("Fields Idx: %d (%#x)\n", seg.FieldsIndexOffset(), seg.FieldsIndexOffset())
		fmt.Printf("Stored Idx: %d (%#x)\n", seg.StoredIndexOffset(), seg.StoredIndexOffset())
		fmt.Printf("DocValue Idx: %d (%#x)\n", seg.DocValueOffset(), seg.DocValueOffset())
		fmt.Printf("Num Docs: %d\n", seg.NumDocs())
		return nil
	},
}

func init() {
	RootCmd.AddCommand(footerCmd)
}
