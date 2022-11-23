package migrate

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
	kurlutils "github.com/replicatedhq/kurl/pkg/k8sutil"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	k8spodutils "k8s.io/kubernetes/pkg/api/v1/pod"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

const (
	baseAnnotation           = "kurl.sh/pvcmigrate"
	scaleAnnotation          = baseAnnotation + "-scale"
	kindAnnotation           = baseAnnotation + "-kind"
	sourceNsAnnotation       = baseAnnotation + "-sourcens"
	sourcePvcAnnotation      = baseAnnotation + "-sourcepvc"
	desiredReclaimAnnotation = baseAnnotation + "-reclaim"
)

// IsDefaultStorageClassAnnotation - this is also exported by https://github.com/kubernetes/kubernetes/blob/v1.21.3/pkg/apis/storage/v1/util/helpers.go#L25
// but that would require adding the k8s import overrides to our go.mod
const IsDefaultStorageClassAnnotation = "storageclass.kubernetes.io/is-default-class"

const pvMigrateContainerName = "pvmigrate"

const openEBSLocalProvisioner = "openebs.io/local"

var isDestScLocalVolumeProvisioner bool

// Options is the set of options that should be provided to Migrate
type Options struct {
	SourceSCName               string
	DestSCName                 string
	RsyncImage                 string
	Namespace                  string
	SetDefaults                bool
	VerboseCopy                bool
	SkipSourceValidation       bool
	SkipPVAccessModeValidation bool
	PvcCopyTimeout             int
	PodReadyTimeout            int
}

// PVMigrator represents a migration context for migrating data from all srcSC volumes to
// dstSc volumes.
type PVMigrator struct {
	ctx             context.Context
	log             *log.Logger
	k8scli          k8sclient.Interface
	srcSc           string
	dstSc           string
	deletePVTimeout time.Duration
	podReadyTimeout time.Duration
	podNameOverride string
	pvcNameOverride string
}

// Cli uses CLI options to run Migrate
func Cli() {
	var options Options

	flag.StringVar(&options.SourceSCName, "source-sc", "", "storage provider name to migrate from")
	flag.StringVar(&options.DestSCName, "dest-sc", "", "storage provider name to migrate to")
	flag.StringVar(&options.RsyncImage, "rsync-image", "eeacms/rsync:2.3", "the image to use to copy PVCs - must have 'rsync' on the path")
	flag.StringVar(&options.Namespace, "namespace", "", "only migrate PVCs within this namespace")
	flag.BoolVar(&options.SetDefaults, "set-defaults", false, "change default storage class from source to dest")
	flag.BoolVar(&options.VerboseCopy, "verbose-copy", false, "show output from the rsync command used to copy data between PVCs")
	flag.BoolVar(&options.SkipSourceValidation, "skip-source-validation", false, "migrate from PVCs using a particular StorageClass name, even if that StorageClass does not exist")
	flag.BoolVar(&options.SkipPVAccessModeValidation, "skip-pv-access-mode-validation", false, "skip the volume access modes validation on the destination storage provider")
	flag.IntVar(&options.PvcCopyTimeout, "pvc-copy-timeout", 300, "length of time to wait (in seconds) when transferring data from the source to the destination storage volume")
	flag.IntVar(&options.PodReadyTimeout, "pod-ready-timeout", 60, "length of time to wait (in seconds) for volume validation pod(s) to go into Ready phase")

	flag.Parse()

	// setup k8s
	cfg, err := config.GetConfig()
	if err != nil {
		fmt.Printf("failed to get config: %s\n", err.Error())
		os.Exit(1)
	}

	clientset, err := k8sclient.NewForConfig(cfg)
	if err != nil {
		fmt.Printf("failed to create kubernetes clientset: %s\n", err.Error())
		os.Exit(1)
	}

	output := log.New(os.Stdout, "", 0) // this has no time prefix etc

	// escape hatch - for DEV/TEST ONLY
	if !options.SkipPVAccessModeValidation {
		srcPVs, err := kurlutils.PVSByStorageClass(context.TODO(), clientset, options.SourceSCName)
		if err != nil {
			fmt.Printf("failed to get volumes using storage class %s: %s", options.SourceSCName, err)
			os.Exit(1)
		}

		pvMigrator := PVMigrator{
			ctx:             context.TODO(),
			log:             output,
			k8scli:          clientset,
			srcSc:           options.SourceSCName,
			dstSc:           options.DestSCName,
			deletePVTimeout: 5 * time.Minute,
			podReadyTimeout: time.Duration(options.PodReadyTimeout) * time.Second,
		}
		unsupportedPVCs, err := pvMigrator.ValidateVolumeAccessModes(srcPVs)
		if err != nil {
			fmt.Printf("failed to validate volume access modes for destination storage class %s", options.DestSCName)
			os.Exit(1)
		}

		if len(unsupportedPVCs) != 0 {
			PrintPVAccessModeErrors(unsupportedPVCs)
			fmt.Printf("existing volumes have access modes not supported by the destination storage class %s", options.DestSCName)
			os.Exit(0)
		}
	}

	// start the migration
	err = Migrate(context.TODO(), output, clientset, options)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		os.Exit(1)
	}
}

// Migrate moves data and PVCs from one StorageClass to another
func Migrate(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, options Options) error {
	err := validateStorageClasses(ctx, w, clientset, options.SourceSCName, options.DestSCName, options.SkipSourceValidation)
	if err != nil {
		return err
	}

	matchingPVCs, namespaces, err := getPVCs(ctx, w, clientset, options.SourceSCName, options.DestSCName, options.Namespace)
	if err != nil {
		return err
	}

	updatedMatchingPVCs, err := scaleDownPods(ctx, w, clientset, matchingPVCs, time.Second*5)
	if err != nil {
		return fmt.Errorf("failed to scale down pods: %w", err)
	}

	err = copyAllPVCs(ctx, w, clientset, options.SourceSCName, options.DestSCName, options.RsyncImage, updatedMatchingPVCs, options.VerboseCopy, time.Duration(options.PvcCopyTimeout)*time.Second)
	if err != nil {
		return err
	}

	for ns, nsPVCs := range matchingPVCs {
		for _, nsPVC := range nsPVCs {
			err = swapPVs(ctx, w, clientset, ns, nsPVC.claim.Name)
			if err != nil {
				return fmt.Errorf("failed to swap PVs for PVC %s in %s: %w", nsPVC.claim.Name, ns, err)
			}
		}
	}

	// scale back deployments/daemonsets/statefulsets
	err = scaleUpPods(ctx, w, clientset, namespaces)
	if err != nil {
		return fmt.Errorf("failed to scale up pods: %w", err)
	}

	if options.SetDefaults {
		err = swapDefaultStorageClasses(ctx, w, clientset, options.SourceSCName, options.DestSCName)
		if err != nil {
			return fmt.Errorf("failed to change default StorageClass from %s to %s: %w", options.SourceSCName, options.DestSCName, err)
		}
	}

	w.Printf("\nSuccess!\n")
	return nil
}

// NewPVMigrator returns a PV migration context
func NewPVMigrator(cfg *rest.Config, log *log.Logger, srcSC, dstSC string, podReadyTimeout int) (*PVMigrator, error) {
	k8scli, err := k8sclient.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	if srcSC == "" {
		return nil, fmt.Errorf("empty source storage class")
	}
	if dstSC == "" {
		return nil, fmt.Errorf("empty destination storage class")
	}
	if log == nil {
		return nil, fmt.Errorf("no logger provided")
	}

	podReadinessTimeout := time.Duration(podReadyTimeout)
	if podReadinessTimeout == 0 {
		// default
		podReadinessTimeout = 60 * time.Second
	}

	return &PVMigrator{
		ctx:             context.Background(),
		log:             log,
		k8scli:          k8scli,
		srcSc:           srcSC,
		dstSc:           dstSC,
		deletePVTimeout: 5 * time.Minute,
		podReadyTimeout: podReadinessTimeout * time.Second,
	}, nil
}

// PrintPVAccessModeErrors prints and formats the volume access mode errors in pvcErrors
func PrintPVAccessModeErrors(pvcErrors map[string]map[string]PVCError) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 8, '\t', 0)
	fmt.Fprintf(tw, "The following persistent volume claims cannot be migrated:\n\n")
	fmt.Fprintln(tw, "NAMESPACE\tPVC\tSOURCE\tREASON\tMESSAGE")
	fmt.Fprintf(tw, "---------\t---\t------\t------\t-------\n")
	for ns, pvcErrs := range pvcErrors {
		for pvc, pvcErr := range pvcErrs {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", ns, pvc, pvcErr.from, pvcErr.reason, pvcErr.message)
		}
	}
	tw.Flush()
}

// ValidateVolumeAccessModes checks whether the provided persistent volumes support the access modes
// of the destination storage class.
// returns a map of pvc errors indexed by namespace
func (p *PVMigrator) ValidateVolumeAccessModes(pvs map[string]corev1.PersistentVolume) (map[string]map[string]PVCError, error) {
	validationErrors := make(map[string]map[string]PVCError)

	if _, err := p.k8scli.StorageV1().StorageClasses().Get(p.ctx, p.dstSc, metav1.GetOptions{}); err != nil {
		return nil, fmt.Errorf("failed to get destination storage class %s: %w", p.dstSc, err)
	}

	pvcs, err := kurlutils.PVCSForPVs(p.ctx, p.k8scli, pvs)
	if err != nil {
		return nil, fmt.Errorf("failed to get pv to pvc mapping: %w", err)
	}

	for pv, pvc := range pvcs {
		v, err := p.checkVolumeAccessModes(pvc)
		if err != nil {
			p.log.Printf("failed to check volume access mode for claim %s (%s): %s", pvc.Name, pv, err)
			continue
		}
		if v != (PVCError{}) { // test for empty struct
			validationErrors[pvc.Namespace] = map[string]PVCError{pvc.Name: v}
		}
	}
	return validationErrors, nil
}

type pvcCtx struct {
	claim     *corev1.PersistentVolumeClaim
	usedByPod *corev1.Pod
}

func (pvc pvcCtx) getNodeNameRef() string {
	if pvc.usedByPod == nil {
		return ""
	}
	return pvc.usedByPod.Spec.NodeName
}

type PVCError struct {
	reason  string
	from    string
	message string
}

func (e *PVCError) Error() string {
	return fmt.Sprintf("volume claim error from %s during %s: %s", e.from, e.reason, e.message)
}

// swapDefaultStorageClasses attempts to set newDefaultSC as the default StorageClass
// if oldDefaultSC was set as the default, then it will be unset first
// if another StorageClass besides these two is currently the default, it will return an error
func swapDefaultStorageClasses(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, oldDefaultSC string, newDefaultSC string) error {
	// check if any SC is currently set as default - if none is, skip the "remove existing default" step
	scs, err := clientset.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list storageclasses: %w", err)
	}

	isDefaultSet := false
	for _, sc := range scs.Items {
		if sc.Annotations == nil {
			continue
		}
		val, ok := sc.Annotations[IsDefaultStorageClassAnnotation]
		if !ok || val != "true" {
			continue
		}
		if sc.Name == newDefaultSC {
			return nil // the currently default StorageClass is the one we want to be set as default, nothing left to do
		}
		if sc.Name != oldDefaultSC {
			return fmt.Errorf("%s is not the default StorageClass", oldDefaultSC)
		}
		isDefaultSet = true
	}

	if isDefaultSet { // only unset the current default StorageClass if there is currently a default StorageClass
		w.Printf("\nChanging default StorageClass from %s to %s\n", oldDefaultSC, newDefaultSC)
		err = mutateSC(ctx, w, clientset, oldDefaultSC, func(sc *storagev1.StorageClass) *storagev1.StorageClass {
			delete(sc.Annotations, IsDefaultStorageClassAnnotation)
			return sc
		}, func(sc *storagev1.StorageClass) bool {
			_, ok := sc.Annotations[IsDefaultStorageClassAnnotation]
			return !ok
		})
		if err != nil {
			return fmt.Errorf("failed to unset StorageClass %s as default: %w", oldDefaultSC, err)
		}
	} else {
		w.Printf("\nSetting %s as the default StorageClass\n", newDefaultSC)
	}

	err = mutateSC(ctx, w, clientset, newDefaultSC, func(sc *storagev1.StorageClass) *storagev1.StorageClass {
		if sc.Annotations == nil {
			sc.Annotations = map[string]string{IsDefaultStorageClassAnnotation: "true"}
		} else {
			sc.Annotations[IsDefaultStorageClassAnnotation] = "true"
		}
		return sc
	}, func(sc *storagev1.StorageClass) bool {
		if sc.Annotations == nil {
			return false
		}
		val, ok := sc.Annotations[IsDefaultStorageClassAnnotation]
		return ok && val == "true"
	})
	if err != nil {
		return fmt.Errorf("failed to set StorageClass %s as default: %w", newDefaultSC, err)
	}

	w.Printf("Finished changing default StorageClass\n")
	return nil
}

func copyAllPVCs(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, sourceSCName string, destSCName string, rsyncImage string, matchingPVCs map[string][]pvcCtx, verboseCopy bool, timeout time.Duration) error {
	// create a pod for each PVC migration, and wait for it to finish
	w.Printf("\nCopying data from %s PVCs to %s PVCs\n", sourceSCName, destSCName)
	for ns, nsPvcs := range matchingPVCs {
		for _, nsPvc := range nsPvcs {
			sourcePvcName, destPvcName := nsPvc.claim.Name, newPvcName(nsPvc.claim.Name)
			w.Printf("Copying data from %s (%s) to %s in %s\n", sourcePvcName, nsPvc.claim.Spec.VolumeName, destPvcName, ns)

			// setup timeout
			timeoutCtx, cancelCtx := context.WithTimeout(ctx, timeout)
			defer cancelCtx()
			err := copyOnePVC(timeoutCtx, w, clientset, ns, sourcePvcName, destPvcName, rsyncImage, verboseCopy, nsPvc.getNodeNameRef(), timeout)
			if err != nil {
				return fmt.Errorf("failed to copy PVC %s in %s: %w", nsPvc.claim.Name, ns, err)
			}
		}
	}
	return nil
}

func copyOnePVC(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, ns string, sourcePvcName string, destPvcName string, rsyncImage string, verboseCopy bool, nodeName string, timeout time.Duration) error {
	w.Printf("Creating pvc migrator pod on node %s\n", nodeName)
	createdPod, err := createMigrationPod(ctx, clientset, ns, sourcePvcName, destPvcName, rsyncImage, nodeName)
	if err != nil {
		return err
	}
	w.Printf("waiting for pod %s to start in %s\n", createdPod.Name, createdPod.Namespace)

	// cleanup pod after completion
	defer func() {
		err = clientset.CoreV1().Pods(ns).Delete(context.TODO(), createdPod.Name, metav1.DeleteOptions{})
		if err != nil {
			w.Printf("failed to delete migration pod %s: %v", createdPod.Name, err)
		}
	}()

	migrationTimeout := time.NewTicker(timeout)
	waitInterval := time.NewTicker(1 * time.Second)
	defer migrationTimeout.Stop()
	defer waitInterval.Stop()
	for {
		gotPod, err := clientset.CoreV1().Pods(ns).Get(ctx, createdPod.Name, metav1.GetOptions{})
		if err != nil {
			w.Printf("failed to get newly created migration pod %s: %v\n", createdPod.Name, err)
		} else if gotPod.Status.Phase == corev1.PodRunning || gotPod.Status.Phase == corev1.PodSucceeded {
			// time to get logs
			break
		}

		w.Printf("got status %s for pod %s, this is likely an error\n", gotPod.Status.Phase, gotPod.Name)

		select {
		case <-waitInterval.C:
			continue
		case <-migrationTimeout.C:
			return fmt.Errorf("migration pod %s failed to go into Running phase: timedout", createdPod.Name)
		}
	}

	w.Printf("migrating PVC %s:\n", sourcePvcName)

	var podLogs io.ReadCloser
	for podLogs == nil {
		podLogsReq := clientset.CoreV1().Pods(ns).GetLogs(createdPod.Name, &corev1.PodLogOptions{
			Container: pvMigrateContainerName,
			Follow:    true,
		})
		podLogs, err = podLogsReq.Stream(ctx)
		if err != nil {
			// if there was an error trying to get the pod logs, check to make sure that the pod is actually running
			w.Printf("failed to get logs for migration pod %s, checking status: %v\n", createdPod.Name, err)

			gotPod, err := clientset.CoreV1().Pods(ns).Get(ctx, createdPod.Name, metav1.GetOptions{})
			if err != nil {
				w.Printf("failed to check status of newly created migration pod %s: %v\n", createdPod.Name, err)
			} else if gotPod.Status.Phase != corev1.PodRunning {
				// if the pod is not running, go to the "validate success" section
				break
			}
		}
		select {
		case <-waitInterval.C:
			continue
		case <-migrationTimeout.C:
			return fmt.Errorf("failed to get logs for migration container %s: timedout", pvMigrateContainerName)
		}
	}

	if podLogs != nil {
		for {
			bufPodLogs := bufio.NewReader(podLogs)
			if bufPodLogs == nil {
				continue
			}
			line, _, err := bufPodLogs.ReadLine()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				w.Printf("failed to read pod logs: %v\n", err)
				break
			}
			if verboseCopy {
				w.Printf("    %s\n", line)
			} else {
				_, _ = fmt.Fprintf(w.Writer(), ".") // one dot per line of output
			}
		}
		if !verboseCopy {
			_, _ = fmt.Fprintf(w.Writer(), "done!\n") // add a newline at the end of the dots if not showing pod logs
		}

		err = podLogs.Close()
		if err != nil {
			w.Printf("failed to close logs for migration pod %s: %v\n", createdPod.Name, err)
			// os.Exit(1) // TODO handle
		}
	}

	// validate that the migration actually completed successfully
	for {
		gotPod, err := clientset.CoreV1().Pods(ns).Get(ctx, createdPod.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get the migration pod %s in %s to confirm that it ran successfully: %w", createdPod.Name, ns, err)
		}
		if gotPod.Status.Phase == corev1.PodSucceeded {
			break
		}
		if gotPod.Status.Phase != corev1.PodRunning {
			return fmt.Errorf("logs for the migration pod %s in %s ended, but the status was %s and not succeeded", createdPod.Name, ns, gotPod.Status.Phase)
		}

		select {
		case <-waitInterval.C:
			continue
		case <-migrationTimeout.C:
			return fmt.Errorf("could not determine if migration pod %s succeeded: timedout", createdPod.Name)
		}
	}

	w.Printf("finished migrating PVC %s\n", sourcePvcName)
	return nil
}

func createMigrationPod(ctx context.Context, clientset k8sclient.Interface, ns string, sourcePvcName string, destPvcName string, rsyncImage string, nodeName string) (*corev1.Pod, error) {
	// apply nodeAffinity when migrating to a local volume provisioner
	var nodeAffinity *corev1.Affinity
	if isDestScLocalVolumeProvisioner && nodeName != "" {
		nodeAffinity = &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "kubernetes.io/hostname",
									Operator: corev1.NodeSelectorOperator("In"),
									Values:   []string{nodeName},
								},
							},
						},
					},
				},
			},
		}
	}

	createdPod, err := clientset.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "migrate-" + sourcePvcName,
			Namespace: ns,
			Labels: map[string]string{
				baseAnnotation: sourcePvcName,
			},
		},
		Spec: corev1.PodSpec{
			Affinity:      nodeAffinity,
			RestartPolicy: corev1.RestartPolicyNever,
			Volumes: []corev1.Volume{
				{
					Name: "source",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: sourcePvcName,
						},
					},
				},
				{
					Name: "dest",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: destPvcName,
						},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:  pvMigrateContainerName,
					Image: rsyncImage,
					Command: []string{
						"rsync",
					},
					Args: []string{
						"-a",       // use the "archive" method to copy files recursively with permissions/ownership/etc
						"-v",       // show verbose output
						"-P",       // show progress, and resume aborted/partial transfers
						"--delete", // delete files in dest that are not in source
						"/source/",
						"/dest",
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							MountPath: "/source",
							Name:      "source",
						},
						{
							MountPath: "/dest",
							Name:      "dest",
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create pod to migrate PVC %s to %s in %s: %w", sourcePvcName, destPvcName, ns, err)
	}
	return createdPod, nil
}

// getPVCs gets all of the PVCs and associated info using the given StorageClass, and creates PVCs to migrate to as needed
// returns:
// a map of namespaces to arrays of original PVCs
// an array of namespaces that the PVCs were found within
// an error, if one was encountered
func getPVCs(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, sourceSCName, destSCName string, Namespace string) (map[string][]pvcCtx, []string, error) {
	// get PVs using the specified storage provider
	pvs, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get persistent volumes: %w", err)
	}
	matchingPVs := []corev1.PersistentVolume{}
	pvsByName := map[string]corev1.PersistentVolume{}
	for _, pv := range pvs.Items {
		if pv.Spec.StorageClassName == sourceSCName {
			matchingPVs = append(matchingPVs, pv)
			pvsByName[pv.Name] = pv
		} else {
			w.Printf("PV %s does not match source SC %s, not migrating\n", pv.Name, sourceSCName)
		}
	}

	// get PVCs using specified PVs
	matchingPVCs := map[string][]pvcCtx{}
	var pvcInfo pvcCtx
	for _, pv := range matchingPVs {
		if pv.Spec.ClaimRef != nil {
			pvc, err := clientset.CoreV1().PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(ctx, pv.Spec.ClaimRef.Name, metav1.GetOptions{})
			if err != nil {
				return nil, nil, fmt.Errorf("failed to get PVC for PV %s in %s: %w", pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace, err)
			}
			pvcInfo.claim = pvc

			if pv.Spec.ClaimRef.Namespace == Namespace || Namespace == "" {
				matchingPVCs[pv.Spec.ClaimRef.Namespace] = append(matchingPVCs[pv.Spec.ClaimRef.Namespace], pvcInfo)
			}

		} else {
			return nil, nil, fmt.Errorf("PV %s does not have an associated PVC - resolve this before rerunning", pv.Name)
		}
	}

	pvcNamespaces := []string{}
	for idx := range matchingPVCs {
		pvcNamespaces = append(pvcNamespaces, idx)
	}

	w.Printf("\nFound %d matching PVCs to migrate across %d namespaces:\n", len(matchingPVCs), len(pvcNamespaces))
	tw := tabwriter.NewWriter(w.Writer(), 2, 2, 1, ' ', 0)
	_, _ = fmt.Fprintf(tw, "namespace:\tpvc:\tpv:\tsize:\t\n")
	for ns, nsPvcs := range matchingPVCs {
		for _, nsPvc := range nsPvcs {
			pvCap := pvsByName[nsPvc.claim.Spec.VolumeName].Spec.Capacity
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t\n", ns, nsPvc.claim.Name, nsPvc.claim.Spec.VolumeName, pvCap.Storage().String())
		}
	}
	err = tw.Flush()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to print PVCs: %w", err)
	}

	// create new PVCs for each matching PVC
	w.Printf("\nCreating new PVCs to migrate data to using the %s StorageClass\n", destSCName)
	for ns, nsPvcs := range matchingPVCs {
		for _, nsPvc := range nsPvcs {
			newName := newPvcName(nsPvc.claim.Name)

			desiredPV, ok := pvsByName[nsPvc.claim.Spec.VolumeName]
			if !ok {
				return nil, nil, fmt.Errorf("failed to find existing PV %s for PVC %s in %s", nsPvc.claim.Spec.VolumeName, nsPvc.claim.Name, ns)
			}

			desiredPvStorage, ok := desiredPV.Spec.Capacity[corev1.ResourceStorage]
			if !ok {
				return nil, nil, fmt.Errorf("failed to find storage capacity for PV %s for PVC %s in %s", nsPvc.claim.Spec.VolumeName, nsPvc.claim.Name, ns)
			}

			// check to see if the desired PVC name already exists (and is appropriate)
			existingPVC, err := clientset.CoreV1().PersistentVolumeClaims(ns).Get(ctx, newName, metav1.GetOptions{})
			if err != nil {
				if !k8serrors.IsNotFound(err) {
					return nil, nil, fmt.Errorf("failed to find existing PVC: %w", err)
				}
			} else if existingPVC != nil {
				if existingPVC.Spec.StorageClassName != nil && *existingPVC.Spec.StorageClassName == destSCName {
					existingSize := existingPVC.Spec.Resources.Requests.Storage().String()

					if existingSize == desiredPvStorage.String() {
						w.Printf("found existing PVC with name %s, not creating new one\n", newName)
						continue
					} else {
						return nil, nil, fmt.Errorf("PVC %s already exists in namespace %s but with size %s instead of %s, cannot create migration target from %s - please delete this to continue", newName, ns, existingSize, desiredPvStorage.String(), nsPvc.claim.Name)
					}
				} else {
					return nil, nil, fmt.Errorf("PVC %s already exists in namespace %s but with storage class %v, cannot create migration target from %s - please delete this to continue", newName, ns, existingPVC.Spec.StorageClassName, nsPvc.claim.Name)
				}
			}

			// if it doesn't already exist, create it
			newPVC, err := clientset.CoreV1().PersistentVolumeClaims(ns).Create(ctx, &corev1.PersistentVolumeClaim{
				TypeMeta: nsPvc.claim.TypeMeta,
				ObjectMeta: metav1.ObjectMeta{
					Name:      newName,
					Namespace: ns,
					Labels: map[string]string{
						baseAnnotation: nsPvc.claim.Name,
						kindAnnotation: "dest",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: &destSCName,
					Resources: corev1.ResourceRequirements{
						Requests: map[corev1.ResourceName]resource.Quantity{
							corev1.ResourceStorage: desiredPvStorage,
						},
					},
					AccessModes: nsPvc.claim.Spec.AccessModes,
				},
			}, metav1.CreateOptions{})
			if err != nil {
				return nil, nil, fmt.Errorf("failed to create new PVC %s in %s: %w", newName, ns, err)
			}
			w.Printf("created new PVC %s with size %v in %s\n", newName, newPVC.Spec.Resources.Requests.Storage().String(), ns)
		}
	}

	return matchingPVCs, pvcNamespaces, nil
}

func validateStorageClasses(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, sourceSCName string, destSCName string, skipSourceValidation bool) error {
	// get storage providers
	storageClasses, err := clientset.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to get StorageClasses: %w", err)
	}
	w.Printf("\nFound %d StorageClasses:\n", len(storageClasses.Items))
	sourceScFound, destScFound := false, false
	for _, sc := range storageClasses.Items {
		w.Printf("%s\n", sc.Name)
		if sc.Name == sourceSCName {
			sourceScFound = true
		}
		if sc.Name == destSCName {
			destScFound = true

			if sc.Provisioner == openEBSLocalProvisioner {
				isDestScLocalVolumeProvisioner = true
			}
		}
	}
	if !sourceScFound {
		if skipSourceValidation {
			w.Printf("\nWarning: unable to find source StorageClass %s, but continuing anyways\n", sourceSCName)
		} else {
			return fmt.Errorf("unable to find source StorageClass %s", sourceSCName)
		}
	}
	if !destScFound {
		return fmt.Errorf("unable to find dest StorageClass %s", destSCName)
	}
	w.Printf("\nMigrating data from %s to %s\n", sourceSCName, destSCName)
	return nil
}

const nameSuffix = "-pvcmigrate"

// if the length after adding the suffix is more than 63 characters, we need to reduce that to fit within k8s limits
// pruning from the end runs the risk of dropping the '0'/'1'/etc of a statefulset's PVC name
// pruning from the front runs the risk of making a-replica-... and b-replica-... collide
// so this removes characters from the middle of the string
func newPvcName(originalName string) string {
	candidate := originalName + nameSuffix
	if len(candidate) <= 63 {
		return candidate
	}

	// remove characters from the middle of the string to reduce the total length to 63 characters
	newCandidate := candidate[0:31] + candidate[len(candidate)-32:]
	return newCandidate
}

// get a PV, apply the selected mutator to the PV, update the PV, use the supplied validator to wait for the update to show up
func mutatePV(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, pvName string, mutator func(volume *corev1.PersistentVolume) *corev1.PersistentVolume, checker func(volume *corev1.PersistentVolume) bool) error {
	tries := 0
	for {
		pv, err := clientset.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get persistent volumes %s: %w", pvName, err)
		}

		pv = mutator(pv)

		_, err = clientset.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		if err != nil {
			if k8serrors.IsConflict(err) {
				if tries > 5 {
					return fmt.Errorf("failed to mutate PV %s: %w", pvName, err)
				}
				w.Printf("Got conflict updating PV %s, waiting 5s to retry\n", pvName)
				time.Sleep(time.Second * 5)
				tries++
				continue
			} else {
				return fmt.Errorf("failed to mutate PV %s: %w", pvName, err)
			}
		}
		break
	}

	for {
		pv, err := clientset.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get persistent volumes %s: %w", pvName, err)
		}

		if checker(pv) {
			return nil
		}
		time.Sleep(time.Second * 5)
	}
}

// get a SC, apply the selected mutator to the SC, update the SC, use the supplied validator to wait for the update to show up
func mutateSC(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, scName string, mutator func(sc *storagev1.StorageClass) *storagev1.StorageClass, checker func(sc *storagev1.StorageClass) bool) error {
	tries := 0
	for {
		sc, err := clientset.StorageV1().StorageClasses().Get(ctx, scName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get storage classes %s: %w", scName, err)
		}

		sc = mutator(sc)

		_, err = clientset.StorageV1().StorageClasses().Update(ctx, sc, metav1.UpdateOptions{})
		if err != nil {
			if k8serrors.IsConflict(err) {
				if tries > 5 {
					return fmt.Errorf("failed to mutate SC %s: %w", scName, err)
				}
				w.Printf("Got conflict updating SC %s, waiting 5s to retry\n", scName)
				time.Sleep(time.Second * 5)
				tries++
				continue
			} else {
				return fmt.Errorf("failed to mutate SC %s: %w", scName, err)
			}
		}
		break
	}

	for {
		sc, err := clientset.StorageV1().StorageClasses().Get(ctx, scName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get storage classes %s: %w", scName, err)
		}

		if checker(sc) {
			return nil
		}
		time.Sleep(time.Second * 5)
	}
}

// scaleDownPods scales down statefulsets & deployments controlling pods mounting PVCs in a supplied list
// it will also cleanup WIP migration pods it discovers that happen to be mounting a supplied PVC.
// if a pod is not created by pvmigrate, and is not controlled by a statefulset/deployment, this function will return an error.
// if waitForCleanup is true, after scaling down deployments/statefulsets it will wait for all pods to be deleted.
// It returns a map of namespace to PVCs and any errors encountered.
func scaleDownPods(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, matchingPVCs map[string][]pvcCtx, checkInterval time.Duration) (map[string][]pvcCtx, error) {
	// build new map with complete pvcCtx
	updatedPVCs := matchingPVCs

	// get pods using specified PVCs
	matchingPods := map[string][]corev1.Pod{}
	matchingPodsCount := 0
	for ns, nsPvcs := range matchingPVCs {
		nsPods, err := clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get pods in %s: %w", ns, err)
		}
		for _, nsPod := range nsPods.Items {
			nsPod := nsPod // need a new var that is per-iteration, NOT per-loop

		perPodLoop:
			for _, podVol := range nsPod.Spec.Volumes {
				if podVol.PersistentVolumeClaim != nil {
					for idx, nsPvClaim := range nsPvcs {
						nsPvClaim := nsPvClaim // need a new var that is per-iteration, NOT per-loop
						if podVol.PersistentVolumeClaim.ClaimName == nsPvClaim.claim.Name {
							matchingPods[ns] = append(matchingPods[ns], nsPod)
							matchingPodsCount++
							// when migrating the pvc data we'll use the nodeName in the podSpec to create the volume
							// on the node where the pod was originally scheduled on
							updatedPVCs[ns][idx] = pvcCtx{nsPvClaim.claim, &nsPod}
							break perPodLoop // exit the for _, podVol := range nsPod.Spec.Volumes loop, as we've already determined that this pod matches
						}
					}
				}
			}
		}
	}

	w.Printf("\nFound %d matching pods to migrate across %d namespaces:\n", matchingPodsCount, len(matchingPods))
	tw := tabwriter.NewWriter(w.Writer(), 2, 2, 1, ' ', 0)
	_, _ = fmt.Fprintf(tw, "namespace:\tpod:\t\n")
	for ns, nsPods := range matchingPods {
		for _, nsPod := range nsPods {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t\n", ns, nsPod.Name)
		}
	}
	err := tw.Flush()
	if err != nil {
		return nil, fmt.Errorf("failed to print Pods: %w", err)
	}

	// get owners controlling specified pods
	matchingOwners := map[string]map[string]map[string]struct{}{} // map of namespaces to ownertypes to ownernames
	for ns, nsPods := range matchingPods {
		for _, nsPod := range nsPods {
			for _, ownerReference := range nsPod.OwnerReferences {
				if _, ok := matchingOwners[ns]; !ok {
					matchingOwners[ns] = map[string]map[string]struct{}{}
				}
				if _, ok := matchingOwners[ns][ownerReference.Kind]; !ok {
					matchingOwners[ns][ownerReference.Kind] = map[string]struct{}{}
				}

				matchingOwners[ns][ownerReference.Kind][ownerReference.Name] = struct{}{}
			}
			if len(nsPod.OwnerReferences) == 0 {
				// if this was a migrate job that wasn't cleaned up properly, delete it
				// (if it's still running, rsync will happily resume when we get back to it)
				if _, ok := nsPod.Labels[baseAnnotation]; ok {
					// this pod was created by pvmigrate, so it can be deleted by pvmigrate
					err := clientset.CoreV1().Pods(ns).Delete(ctx, nsPod.Name, metav1.DeleteOptions{})
					if err != nil {
						return nil, fmt.Errorf("migration pod %s in %s leftover from a previous run failed to delete, please delete it before retrying: %w", nsPod.Name, ns, err)
					}
				} else {
					// TODO: handle properly
					return nil, fmt.Errorf("pod %s in %s did not have any owners!\nPlease delete it before retrying", nsPod.Name, ns)
				}
			}
		}
	}

	w.Printf("\nScaling down StatefulSets and Deployments with matching PVCs\n")
	migrationStartTime := time.Now()
	// record, log and scale things controlling specified pods
	for ns, nsOwners := range matchingOwners {
		for ownerKind, owner := range nsOwners {
			for ownerName := range owner {
				switch ownerKind {
				case "StatefulSet":
					ss, err := clientset.AppsV1().StatefulSets(ns).Get(ctx, ownerName, metav1.GetOptions{})
					if err != nil {
						return nil, fmt.Errorf("failed to get statefulset %s scale in %s: %w", ownerName, ns, err)
					}

					formerScale := int32(1)
					if ss.Spec.Replicas != nil {
						formerScale = *ss.Spec.Replicas
					}
					int320 := int32(0)
					ss.Spec.Replicas = &int320

					// add an annotation with the current scale (if it does not already exist)
					if ss.Annotations == nil {
						ss.Annotations = map[string]string{}
					}
					if _, ok := ss.Annotations[scaleAnnotation]; !ok {
						ss.Annotations[scaleAnnotation] = fmt.Sprintf("%d", formerScale)
					}

					w.Printf("scaling StatefulSet %s from %d to 0 in %s\n", ownerName, formerScale, ns)
					_, err = clientset.AppsV1().StatefulSets(ns).Update(ctx, ss, metav1.UpdateOptions{})
					if err != nil {
						return nil, fmt.Errorf("failed to scale statefulset %s to zero in %s: %w", ownerName, ns, err)
					}
				case "ReplicaSet":
					rs, err := clientset.AppsV1().ReplicaSets(ns).Get(ctx, ownerName, metav1.GetOptions{})
					if err != nil {
						return nil, fmt.Errorf("failed to get replicaset %s in %s: %w", ownerName, ns, err)
					}

					if len(rs.OwnerReferences) != 1 {
						return nil, fmt.Errorf("expected 1 owner for replicaset %s in %s, found %d instead", ownerName, ns, len(rs.OwnerReferences))
					}
					if rs.OwnerReferences[0].Kind != "Deployment" {
						return nil, fmt.Errorf("expected owner for replicaset %s in %s to be a deployment, found %s of kind %s instead", ownerName, ns, rs.OwnerReferences[0].Name, rs.OwnerReferences[0].Kind)
					}

					dep, err := clientset.AppsV1().Deployments(ns).Get(ctx, rs.OwnerReferences[0].Name, metav1.GetOptions{})
					if err != nil {
						return nil, fmt.Errorf("failed to get deployment %s scale in %s: %w", ownerName, ns, err)
					}

					formerScale := int32(1)
					if dep.Spec.Replicas != nil {
						formerScale = *dep.Spec.Replicas
					}
					int320 := int32(0)
					dep.Spec.Replicas = &int320

					// add an annotation with the current scale (if it does not already exist)
					if dep.Annotations == nil {
						dep.Annotations = map[string]string{}
					}
					if _, ok := dep.Annotations[scaleAnnotation]; !ok {
						dep.Annotations[scaleAnnotation] = fmt.Sprintf("%d", formerScale)
					}

					w.Printf("scaling Deployment %s from %d to 0 in %s\n", ownerName, formerScale, ns)
					_, err = clientset.AppsV1().Deployments(ns).Update(ctx, dep, metav1.UpdateOptions{})
					if err != nil {
						return nil, fmt.Errorf("failed to scale statefulset %s to zero in %s: %w", ownerName, ns, err)
					}
				default:
					return nil, fmt.Errorf("scaling pods controlled by a %s is not supported, please delete the pods controlled by %s in %s before retrying", ownerKind, ownerKind, ns)
				}
			}
		}
	}

	// wait for all pods to be deleted
	w.Printf("\nWaiting for pods with mounted PVCs to be cleaned up\n")
	time.Sleep(checkInterval / 16)
checkPvcPodLoop:
	for {
		for ns, nsPvcs := range matchingPVCs {
			nsPods, err := clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to get pods in %s: %w", ns, err)
			}
			for _, nsPod := range nsPods.Items {
				for _, podVol := range nsPod.Spec.Volumes {
					if podVol.PersistentVolumeClaim != nil {
						for _, nsClaim := range nsPvcs {
							if podVol.PersistentVolumeClaim.ClaimName == nsClaim.claim.Name {
								if nsPod.CreationTimestamp.After(migrationStartTime) {
									return nil, fmt.Errorf("pod %s in %s mounting %s was created at %s, after scale-down started at %s. It is likely that there is some other operator scaling this back up", nsPod.Name, ns, nsClaim.claim.Name, nsPod.CreationTimestamp.Format(time.RFC3339), migrationStartTime.Format(time.RFC3339))
								}

								w.Printf("Found pod %s in %s mounting to-be-migrated PVC %s, waiting\n", nsPod.Name, ns, nsClaim.claim.Name)
								time.Sleep(checkInterval) // don't check too often, as this loop is relatively expensive
								continue checkPvcPodLoop  // as soon as we find a matching pod, we know we need to wait another 30s
							}
						}
					}
				}
			}
		}

		break // no matching pods with PVCs, so good to continue
	}

	w.Printf("All pods removed successfully\n")
	return updatedPVCs, nil
}

func scaleUpPods(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, namespaces []string) error {
	w.Printf("\nScaling back StatefulSets and Deployments with matching PVCs\n")
	for _, ns := range namespaces {
		// get statefulsets and reset the scale (and remove the annotation in the process)
		sses, err := clientset.AppsV1().StatefulSets(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("failed to get statefulsets in %s: %w", ns, err)
		}
		for _, ss := range sses.Items {
			if desiredScale, ok := ss.Annotations[scaleAnnotation]; ok {
				desiredScaleInt, err := strconv.Atoi(desiredScale)
				if err != nil {
					return fmt.Errorf("failed to parse scale %q for StatefulSet %s in %s: %w", desiredScale, ss.Name, ns, err)
				}
				delete(ss.Annotations, scaleAnnotation)
				desiredScaleInt32 := int32(desiredScaleInt)
				ss.Spec.Replicas = &desiredScaleInt32

				w.Printf("scaling StatefulSet %s from 0 to %d in %s\n", ss.Name, desiredScaleInt32, ns)

				_, err = clientset.AppsV1().StatefulSets(ns).Update(ctx, &ss, metav1.UpdateOptions{})
				if err != nil {
					return fmt.Errorf("failed to update StatefulSet %s in %s: %w", ss.Name, ns, err)
				}
			}
		}

		// get deployments and reset the scale (and remove the annotation in the process)
		deps, err := clientset.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("failed to get deployments in %s: %w", ns, err)
		}
		for _, dep := range deps.Items {
			if desiredScale, ok := dep.Annotations[scaleAnnotation]; ok {
				desiredScaleInt, err := strconv.Atoi(desiredScale)
				if err != nil {
					return fmt.Errorf("failed to parse scale %q for Deployment %s in %s: %w", desiredScale, dep.Name, ns, err)
				}
				delete(dep.Annotations, scaleAnnotation)
				desiredScaleInt32 := int32(desiredScaleInt)
				dep.Spec.Replicas = &desiredScaleInt32

				w.Printf("scaling Deployment %s from 0 to %d in %s\n", dep.Name, desiredScaleInt32, ns)

				_, err = clientset.AppsV1().Deployments(ns).Update(ctx, &dep, metav1.UpdateOptions{})
				if err != nil {
					return fmt.Errorf("failed to update Deployment %s in %s: %w", dep.Name, ns, err)
				}
			}
		}

		// TODO: handle other owner kinds
	}
	return nil
}

// given the name of a PVC, swap the underlying PV with the one used by <pvcname>-pvcmigrate, delete the PVC <pvcname>-migrate, and delete the original PV
func swapPVs(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, ns string, pvcName string) error {
	w.Printf("\nSwapping PVC %s in %s to the new StorageClass", pvcName, ns)

	// get the two relevant PVCs - the (original) one that has the name we want, and the one that has the PV we want
	originalPVC, err := clientset.CoreV1().PersistentVolumeClaims(ns).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get original PVC %s in %s: %w", pvcName, ns, err)
	}
	migratedPVC, err := clientset.CoreV1().PersistentVolumeClaims(ns).Get(ctx, newPvcName(pvcName), metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get migrated PVC %s in %s: %w", newPvcName(pvcName), ns, err)
	}

	// mark PVs used by both originalPVC and migratedPVC as to-be-retained
	w.Printf("Marking original PV %s as to-be-retained\n", originalPVC.Spec.VolumeName)
	var originalReclaim corev1.PersistentVolumeReclaimPolicy
	err = mutatePV(ctx, w, clientset, originalPVC.Spec.VolumeName, func(volume *corev1.PersistentVolume) *corev1.PersistentVolume {
		originalReclaim = volume.Spec.PersistentVolumeReclaimPolicy
		volume.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
		return volume
	}, func(volume *corev1.PersistentVolume) bool {
		return volume.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain
	})
	if err != nil {
		return fmt.Errorf("failed to set original PV reclaim policy: %w", err)
	}

	w.Printf("Marking migrated-to PV %s as to-be-retained\n", migratedPVC.Spec.VolumeName)
	err = mutatePV(ctx, w, clientset, migratedPVC.Spec.VolumeName, func(volume *corev1.PersistentVolume) *corev1.PersistentVolume {
		// add annotations describing what PVC this data came from in case of a failure later
		volume.Annotations[sourceNsAnnotation] = ns
		volume.Annotations[sourcePvcAnnotation] = pvcName

		// this will be used to set the reclaim policy after attaching a new PVC
		volume.Annotations[desiredReclaimAnnotation] = string(originalReclaim)

		volume.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
		return volume
	}, func(volume *corev1.PersistentVolume) bool {
		return volume.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain
	})
	if err != nil {
		return fmt.Errorf("failed to set migratede-to PV reclaim policy: %w", err)
	}

	// delete both the original and migrated-to PVCs
	w.Printf("Deleting original PVC %s in %s to free up the name\n", pvcName, ns)
	err = clientset.CoreV1().PersistentVolumeClaims(ns).Delete(ctx, pvcName, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete original PVC %s in %s: %w", pvcName, ns, err)
	}
	w.Printf("Deleting migrated-to PVC %s in %s to release the PV\n", newPvcName(pvcName), ns)
	err = clientset.CoreV1().PersistentVolumeClaims(ns).Delete(ctx, newPvcName(pvcName), metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete migrated-to PVC %s in %s: %w", newPvcName(pvcName), ns, err)
	}

	// wait for the deleted PVCs to actually no longer exist
	w.Printf("Waiting for original PVC %s in %s to finish deleting\n", pvcName, ns)
	err = waitForDeletion(ctx, clientset, pvcName, ns)
	if err != nil {
		return fmt.Errorf("failed to ensure deletion of original PVC %s in %s: %w", pvcName, ns, err)
	}
	w.Printf("Waiting for migrated-to PVC %s in %s to finish deleting\n", newPvcName(pvcName), ns)
	err = waitForDeletion(ctx, clientset, newPvcName(pvcName), ns)
	if err != nil {
		return fmt.Errorf("failed to ensure deletion of migrated-to PVC %s in %s: %w", newPvcName(pvcName), ns, err)
	}

	// remove claimrefs from original and migrated-to PVs
	w.Printf("Removing claimref from original PV %s\n", originalPVC.Spec.VolumeName)
	err = mutatePV(ctx, w, clientset, originalPVC.Spec.VolumeName, func(volume *corev1.PersistentVolume) *corev1.PersistentVolume {
		volume.Spec.ClaimRef = nil
		return volume
	}, func(volume *corev1.PersistentVolume) bool {
		return volume.Spec.ClaimRef == nil
	})
	if err != nil {
		return fmt.Errorf("failed to remove claimrefs from PV %s: %w", originalPVC.Spec.VolumeName, err)
	}
	w.Printf("Removing claimref from migrated-to PV %s\n", migratedPVC.Spec.VolumeName)
	err = mutatePV(ctx, w, clientset, migratedPVC.Spec.VolumeName, func(volume *corev1.PersistentVolume) *corev1.PersistentVolume {
		volume.Spec.ClaimRef = nil
		return volume
	}, func(volume *corev1.PersistentVolume) bool {
		return volume.Spec.ClaimRef == nil
	})
	if err != nil {
		return fmt.Errorf("failed to remove claimrefs from PV %s: %w", migratedPVC.Spec.VolumeName, err)
	}

	// create new PVC with the old name/annotations/settings, and the new PV
	w.Printf("Creating new PVC %s with migrated-to PV %s\n", originalPVC.Name, migratedPVC.Spec.VolumeName)
	newPVC := corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{
			Kind:       originalPVC.Kind,
			APIVersion: originalPVC.APIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      originalPVC.Name,
			Namespace: originalPVC.Namespace,
			Labels:    originalPVC.Labels, // copy labels, don't copy annotations
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: originalPVC.Spec.AccessModes,
			Resources:   originalPVC.Spec.Resources,
			VolumeMode:  originalPVC.Spec.VolumeMode,

			// from migrated-to PVC
			StorageClassName: migratedPVC.Spec.StorageClassName,
			VolumeName:       migratedPVC.Spec.VolumeName,
		},
	}
	_, err = clientset.CoreV1().PersistentVolumeClaims(ns).Create(ctx, &newPVC, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create migrated-to PVC %s in %s: %w", pvcName, ns, err)
	}

	// return the PVs retain policy
	w.Printf("Resetting migrated-to PV %s reclaim policy\n", migratedPVC.Spec.VolumeName)
	err = resetReclaimPolicy(ctx, w, clientset, migratedPVC.Spec.VolumeName, &originalReclaim)
	if err != nil {
		return err
	}

	// delete the original PV
	w.Printf("Deleting original, now redundant, PV %s\n", originalPVC.Spec.VolumeName)
	err = clientset.CoreV1().PersistentVolumes().Delete(ctx, originalPVC.Spec.VolumeName, metav1.DeleteOptions{})
	if err != nil {
		w.Printf("Unable to cleanup redundant PV %s: %s\n", originalPVC.Spec.VolumeName, err.Error())
	}

	// success message
	w.Printf("Successfully migrated PVC %s in %s from PV %s to %s\n", pvcName, ns, originalPVC.Spec.VolumeName, migratedPVC.Spec.VolumeName)
	return nil
}

// waitForDeletion waits for the provided pvcName to not be found, and returns early if any error besides 'not found' is given
func waitForDeletion(ctx context.Context, clientset k8sclient.Interface, pvcName, ns string) error {
	for {
		_, err := clientset.CoreV1().PersistentVolumeClaims(ns).Get(ctx, pvcName, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) {
			break
		} else if err != nil {
			return err
		}

		select {
		case <-time.After(time.Second / 20):
			continue
		case <-ctx.Done():
			return fmt.Errorf("context ended waiting for PVC %s in %s to be deleted", pvcName, ns)
		}
	}
	return nil
}

// reset the reclaim policy of the specified PV.
// If 'reclaim' is non-nil, that value will be used, otherwise the value will be taken from the annotation.
// If 'reclaim' is not specified and the annotation does not exist, the reclaim policy will not be updated.
// in either case, the annotation will be removed.
func resetReclaimPolicy(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, pvName string, reclaim *corev1.PersistentVolumeReclaimPolicy) error {
	err := mutatePV(ctx, w, clientset, pvName, func(volume *corev1.PersistentVolume) *corev1.PersistentVolume {
		if reclaim != nil {
			volume.Spec.PersistentVolumeReclaimPolicy = *reclaim
		} else {
			if annotationVal, ok := volume.Annotations[desiredReclaimAnnotation]; ok {
				volume.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimPolicy(annotationVal)
			}
		}
		return volume
	}, func(volume *corev1.PersistentVolume) bool {
		if reclaim != nil {
			return volume.Spec.PersistentVolumeReclaimPolicy == *reclaim
		}

		if annotationVal, ok := volume.Annotations[desiredReclaimAnnotation]; ok {
			return volume.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimPolicy(annotationVal)
		}

		return true
	})
	if err != nil {
		return fmt.Errorf("failed to reset PV reclaim policy: %w", err)
	}

	return nil
}

// buildPVConsumerPod creates a pod spec for consuming a pvc
func (p *PVMigrator) buildPVConsumerPod(pvcName string) *corev1.Pod {
	podName := p.podNameOverride
	if podName == "" {
		tmp := uuid.New().String()[:5]
		podName = fmt.Sprintf("pvmigrate-vol-consumer-%s-%s", pvcName, tmp)
	}
	if len(podName) > 63 {
		podName = podName[0:31] + podName[len(podName)-32:]
	}
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Volumes: []corev1.Volume{
				{
					Name: "tmp",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcName,
						},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "busybox",
					Image: "busybox",
					Command: []string{
						"sleep",
						"3600",
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							MountPath: "/tmpmount",
							Name:      "tmp",
						},
					},
				},
			},
		},
	}
}

// buildPVC creates a temporary PVC requesting for 1Mi of storage for a provided storage class name.
func (p *PVMigrator) buildTmpPVC(pvc corev1.PersistentVolumeClaim, sc string) *corev1.PersistentVolumeClaim {
	pvcName := p.pvcNameOverride
	if pvcName == "" {
		tmp := uuid.New().String()[:5]
		pvcName = fmt.Sprintf("pvmigrate-claim-%s-%s", pvc.Name, tmp)
	}
	if len(pvcName) > 63 {
		pvcName = pvcName[0:31] + pvcName[len(pvcName)-32:]
	}

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &sc,
			AccessModes:      pvc.Spec.AccessModes,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Mi"),
				},
			},
		},
		// for testing/mocking
		// This fields gets set and updated by the kube api server
		Status: corev1.PersistentVolumeClaimStatus{Phase: pvc.Status.Phase},
	}
}

// checkVolumeAccessModeValid checks if the access modes of a pv are supported by the
// destination storage class.
func (p *PVMigrator) checkVolumeAccessModes(pvc corev1.PersistentVolumeClaim) (PVCError, error) {
	var err error

	// create temp pvc for storage class
	tmpPVC := p.buildTmpPVC(pvc, p.dstSc)
	if tmpPVC, err = p.k8scli.CoreV1().PersistentVolumeClaims("default").Create(
		p.ctx, tmpPVC, metav1.CreateOptions{},
	); err != nil {
		return PVCError{}, fmt.Errorf("failed to create temporary pvc: %w", err)
	}

	// consume pvc to determine any access mode errors
	pvConsumerPodSpec := p.buildPVConsumerPod(tmpPVC.Name)
	pvConsumerPod, err := p.k8scli.CoreV1().Pods(pvConsumerPodSpec.Namespace).Create(p.ctx, pvConsumerPodSpec, metav1.CreateOptions{})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return PVCError{}, err
	}

	// cleanup pvc and pod at the end
	defer func() {
		if err = p.deleteTmpPVC(tmpPVC); err != nil {
			p.log.Printf("failed to delete tmp claim: %s", err)
		}
	}()
	defer func() {
		if err = p.deletePVConsumerPod(pvConsumerPod); err != nil {
			p.log.Printf("failed to delete pv consumer pod %s: %s", pvConsumerPod.Name, err)
		}
	}()

	podReadyTimeoutEnd := time.Now().Add(p.podReadyTimeout)
	for {
		gotPod, err := p.k8scli.CoreV1().Pods(pvConsumerPodSpec.Namespace).Get(p.ctx, pvConsumerPodSpec.Name, metav1.GetOptions{})
		if err != nil {
			return PVCError{}, fmt.Errorf("failed getting pv consumer pod %s: %w", gotPod.Name, err)
		}

		switch {
		case k8spodutils.IsPodReady(gotPod):
			return PVCError{}, nil
		default:
			time.Sleep(time.Second)
		}

		if time.Now().After(podReadyTimeoutEnd) {
			// The volume consumer pod never went into running phase which means it's probably an error
			// with provisioning the volume.
			// A pod in Pending phase means the API Server has created the resource and stored it in etcd,
			// but the pod has not been scheduled yet, nor have container images been pulled from the registry.
			if gotPod.Status.Phase == corev1.PodPending {
				// check pvc status and get error
				pvcPendingError, err := p.getPvcError(tmpPVC)
				if err != nil {
					return PVCError{}, fmt.Errorf("failed to get PVC error: %s", err)
				}
				return pvcPendingError, nil
			}
			// pod failed for other reason(s)
			return PVCError{}, fmt.Errorf("unexpected status for pod %s: %s", gotPod.Name, gotPod.Status.Phase)
		}
	}
}

// deleteTmpPVC deletes the provided pvc from the default namespace and waits until the
// backing pv dissapear as well (this is mandatory so we don't leave any orphan pv as this would
// cause pvmigrate to fail). this function has a timeout of 5 minutes, after that an error is
// returned.
func (p *PVMigrator) deleteTmpPVC(pvc *corev1.PersistentVolumeClaim) error {
	// Cleanup should use background context so as not to fail if context has already been canceled
	ctx := context.Background()

	pvs, err := p.k8scli.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list persistent volumes: %w", err)
	}

	pvsByPVCName := map[string]corev1.PersistentVolume{}
	for _, pv := range pvs.Items {
		if pv.Spec.ClaimRef == nil {
			continue
		}
		pvsByPVCName[pv.Spec.ClaimRef.Name] = pv
	}

	var waitFor []string
	propagation := metav1.DeletePropagationForeground
	delopts := metav1.DeleteOptions{PropagationPolicy: &propagation}
	if err := p.k8scli.CoreV1().PersistentVolumeClaims("default").Delete(
		ctx, pvc.Name, delopts,
	); err != nil {
		if !k8serrors.IsNotFound(err) {
			p.log.Printf("failed to delete temp pvc %s: %s", pvc.Name, err)
			return err
		}
	}
	waitFor = append(waitFor, pvc.Name)

	timeout := time.NewTicker(5 * time.Minute)
	interval := time.NewTicker(5 * time.Second)
	defer timeout.Stop()
	defer interval.Stop()
	for _, pvc := range waitFor {
		pv, ok := pvsByPVCName[pvc]
		if !ok {
			p.log.Printf("failed to find pv for temp pvc %s", pvc)
			continue
		}

		for {
			// break the loop as soon as we can't find the pv anymore.
			if _, err := p.k8scli.CoreV1().PersistentVolumes().Get(
				ctx, pv.Name, metav1.GetOptions{},
			); err != nil && !k8serrors.IsNotFound(err) {
				p.log.Printf("failed to get pv for temp pvc %s: %s", pvc, err)
			} else if err != nil && k8serrors.IsNotFound(err) {
				break
			}

			select {
			case <-interval.C:
				continue
			case <-timeout.C:
				return fmt.Errorf("failed to delete pvs: timeout")
			}
		}
	}
	return nil
}

// deletePVConsumerPod removes the pod resource from the api servere
func (p *PVMigrator) deletePVConsumerPod(pod *corev1.Pod) error {
	propagation := metav1.DeletePropagationForeground
	delopts := metav1.DeleteOptions{PropagationPolicy: &propagation}
	if pod != nil {
		if err := p.k8scli.CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, delopts); err != nil {
			return err
		}
	}
	return nil
}

// getPvcError returns the error event for why a PVC is in Pending status
func (p *PVMigrator) getPvcError(pvc *corev1.PersistentVolumeClaim) (PVCError, error) {
	// no need to inspect pvc
	if pvc.Status.Phase != corev1.ClaimPending {
		return PVCError{}, fmt.Errorf("PVC %s is not in Pending status", pvc.Name)
	}

	pvcEvents, err := p.k8scli.CoreV1().Events(pvc.Namespace).Search(scheme.Scheme, pvc)
	if err != nil {
		return PVCError{}, fmt.Errorf("failed to list events for PVC %s", pvc.Name)
	}

	// get pending reason
	for _, event := range pvcEvents.Items {
		if event.Reason == "ProvisioningFailed" {
			return PVCError{event.Reason, event.Source.Component, event.Message}, nil
		}
	}
	return PVCError{}, fmt.Errorf("could not determine reason for why PVC %s is in Pending status", pvc.Name)
}
