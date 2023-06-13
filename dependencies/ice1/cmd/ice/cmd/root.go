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
	"os"
	"unicode"

	"github.com/blevesearch/mmap-go"
	segment "github.com/blugelabs/bluge_segment_api"

	"github.com/blugelabs/ice"
	"github.com/spf13/cobra"
)

var seg *ice.Segment

const rootArgFilename = 1

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "ice",
	Short: "command-line tool to interact with an ice file",
	Long:  `Ice is a command-line tool to interact with an ice file.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {

		if len(args) < rootArgFilename {
			return fmt.Errorf("must specify path to file")
		}

		segInt, _, err := openFromFile(args[0])
		if err != nil {
			return fmt.Errorf("error opening file: %v", err)
		}
		seg = segInt.(*ice.Segment)

		return nil
	},
	PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func printValue(val []byte) string {
	return printValueStr(string(val))
}

func printValueStr(str string) string {
	for _, r := range str {
		if !unicode.IsPrint(r) {
			return ""
		}
	}
	return str
}

type closeFunc func() error

var noCloseFunc = func() error {
	return nil
}

func openFromFile(path string) (segment.Segment, closeFunc, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, noCloseFunc, err
	}
	mm, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		// mmap failed, try to close the file
		_ = f.Close()
		return nil, noCloseFunc, err
	}

	closeF := func() error {
		err2 := mm.Unmap()
		// try to close file even if unmap failed
		err3 := f.Close()
		if err2 == nil {
			// try to return first error
			err2 = err3
		}
		return err2
	}

	data := segment.NewDataBytes(mm)

	seg, err := ice.Load(data)
	if err != nil {
		_ = closeF()
		return nil, noCloseFunc, fmt.Errorf("error loading segment: %v", err)
	}

	return seg, closeF, nil
}
