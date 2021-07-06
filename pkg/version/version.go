package version

import (
	"fmt"
)

// NOTE: these variables are injected at build time

var (
	version, gitSHA, buildTime string
)

// Print prints the version, git sha and build time.
func Print() {
	fmt.Printf("version=%s\nsha=%s\ntime=%s\n", version, gitSHA, buildTime)
}
