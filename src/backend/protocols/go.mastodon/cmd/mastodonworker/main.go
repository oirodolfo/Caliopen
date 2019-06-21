// Copyleft (ɔ) 2018 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package main

import (
	"fmt"
	"github.com/CaliOpen/Caliopen/src/backend/protocols/go.mastodon/cmd/mastodonworker/cli_cmds"
	"os"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
