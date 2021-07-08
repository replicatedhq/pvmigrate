package main

import (
	"fmt"

	"github.com/replicatedhq/pvmigrate/pkg/migrate"
	"github.com/replicatedhq/pvmigrate/pkg/version"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // this allows accessing a larger array of cloud providers
)

func main() {
	fmt.Printf("Running pvmigrate build:\n")
	version.Print()

	migrate.Cli()
}
