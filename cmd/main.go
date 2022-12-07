package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/replicatedhq/pvmigrate/pkg/migrate"
	"github.com/replicatedhq/pvmigrate/pkg/preflight"
	"github.com/replicatedhq/pvmigrate/pkg/version"
	k8sclient "k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // this allows accessing a larger array of cloud providers
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	fmt.Printf("Running pvmigrate build:\n")
	version.Print()

	// Cli uses CLI options to run Migrate
	var options migrate.Options
	var skipPreflightValidation bool
	var preflightValidationOnly bool
	flag.StringVar(&options.SourceSCName, "source-sc", "", "storage provider name to migrate from")
	flag.StringVar(&options.DestSCName, "dest-sc", "", "storage provider name to migrate to")
	flag.StringVar(&options.RsyncImage, "rsync-image", "eeacms/rsync:2.3", "the image to use to copy PVCs - must have 'rsync' on the path")
	flag.StringVar(&options.Namespace, "namespace", "", "only migrate PVCs within this namespace")
	flag.BoolVar(&options.SetDefaults, "set-defaults", false, "change default storage class from source to dest")
	flag.BoolVar(&options.VerboseCopy, "verbose-copy", false, "show output from the rsync command used to copy data between PVCs")
	flag.BoolVar(&options.SkipSourceValidation, "skip-source-validation", false, "migrate from PVCs using a particular StorageClass name, even if that StorageClass does not exist")
	flag.DurationVar(&options.PodReadyTimeout, "pod-ready-timeout", 60*time.Second, "length of time to wait (in seconds) for volume validation pod(s) to go into Ready phase")
	flag.BoolVar(&skipPreflightValidation, "skip-preflight-validation", false, "skip the volume access modes validation on the destination storage provider")
	flag.BoolVar(&preflightValidationOnly, "preflight-validation-only", false, "skip the migration and run preflight validation only")

	flag.Parse()

	// setup logger
	logger := log.New(os.Stderr, "", 0) // this has no time prefix etc

	// setup k8s
	cfg, err := config.GetConfig()
	if err != nil {
		logger.Printf("failed to get config: %s", err)
		os.Exit(1)
	}

	clientset, err := k8sclient.NewForConfig(cfg)
	if err != nil {
		logger.Printf("failed to create kubernetes clientset: %s", err)
		os.Exit(1)
	}

	if !skipPreflightValidation {
		failures, err := preflight.Validate(ctx, logger, clientset, options)
		if err != nil {
			logger.Printf("failed to run preflight validation checks")
			os.Exit(1)
		}

		if len(failures) != 0 {
			preflight.PrintValidationFailures(os.Stdout, failures)
			os.Exit(1)
		}
	}

	// start the migration
	if !preflightValidationOnly {
		err = migrate.Migrate(ctx, logger, clientset, options)
		if err != nil {
			logger.Printf("migration failed: %s", err)
			os.Exit(1)
		}
	}
}
