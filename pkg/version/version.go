package version

import (
	"fmt"
	"runtime/debug"
)

// NOTE: these variables are injected at build time

var (
	version, gitSHA, buildTime string
)

const pvmModuleName = "github.com/replicatedhq/pvmigrate"

func init() {
	if version == "" {
		// attempt to get the version from runtime build info
		// go through all the dependencies to find the pvmigrate module version
		// failure to read buildinfo is ok, we just won't have a version set
		bi, ok := debug.ReadBuildInfo()
		if ok {
			for _, dep := range bi.Deps {
				if dep.Path == pvmModuleName {
					version = dep.Version
					if dep.Replace != nil {
						version = dep.Replace.Version
					}
					break
				}
			}
		}
	}
}

func Version() string {
	return version
}

// Print prints the version, git sha and build time.
func Print() {
	fmt.Printf("version=%s\nsha=%s\ntime=%s\n", version, gitSHA, buildTime)
}
