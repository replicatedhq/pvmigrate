package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
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
	var podReadyTimeout int
	var deletePVTimeout int
	var rsyncFlags string
	flag.StringVar(&options.SourceSCName, "source-sc", "", "storage provider name to migrate from")
	flag.StringVar(&options.DestSCName, "dest-sc", "", "storage provider name to migrate to")
	flag.StringVar(&options.RsyncImage, "rsync-image", "eeacms/rsync:2.3", "the image to use to copy PVCs - must have 'rsync' on the path")
	flag.StringVar(&rsyncFlags, "rsync-flags", "", "additional flags to pass to rsync command")
	flag.StringVar(&options.Namespace, "namespace", "", "only migrate PVCs within this namespace")
	flag.BoolVar(&options.SetDefaults, "set-defaults", false, "change default storage class from source to dest")
	flag.BoolVar(&options.VerboseCopy, "verbose-copy", false, "show output from the rsync command used to copy data between PVCs")
	flag.BoolVar(&options.PreSyncMode, "pre-sync-mode", false, "create the new PVC and copy the data, then scale down, run another copy and finally swap the PVCs")
	flag.BoolVar(&options.SkipSourceValidation, "skip-source-validation", false, "migrate from PVCs using a particular StorageClass name, even if that StorageClass does not exist")
	flag.IntVar(&options.MaxPVs, "max-pvs", 0, "maximum number of PVs to process. default to 0 (unlimited)")
	flag.IntVar(&podReadyTimeout, "pod-ready-timeout", 60, "length of time to wait (in seconds) for validation pod(s) to go into Ready phase")
	flag.IntVar(&deletePVTimeout, "delete-pv-timeout", 300, "length of time to wait (in seconds) for backing PV to be removed when temporary PVC is deleted")
	flag.BoolVar(&skipPreflightValidation, "skip-preflight-validation", false, "skip preflight migration validation on the destination storage provider")
	flag.BoolVar(&preflightValidationOnly, "preflight-validation-only", false, "skip the migration and run preflight validation only")

	flag.Parse()

	// update options with flag values
	options.PodReadyTimeout = time.Duration(podReadyTimeout) * time.Second
	options.DeletePVTimeout = time.Duration(deletePVTimeout) * time.Second

	if rsyncFlags != "" {
		rsyncFlagsSlice := strings.Split(rsyncFlags, ",")
		options.RsyncFlags = rsyncFlagsSlice
	}

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
