package main

import (
	"fmt"

	"github.com/replicatedhq/pvmigrate/pkg/migrate"
	"github.com/replicatedhq/pvmigrate/pkg/version"
)

func main() {
	fmt.Printf("Running pvmigrate build:\n")
	version.Print()

	migrate.Cli()
}
