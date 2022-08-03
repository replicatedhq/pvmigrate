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

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sclient "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

const baseAnnotation = "kurl.sh/pvcmigrate"
const scaleAnnotation = baseAnnotation + "-scale"
const kindAnnotation = baseAnnotation + "-kind"
const sourceNsAnnotation = baseAnnotation + "-sourcens"
const sourcePvcAnnotation = baseAnnotation + "-sourcepvc"
const desiredReclaimAnnotation = baseAnnotation + "-reclaim"

// IsDefaultStorageClassAnnotation - this is also exported by https://github.com/kubernetes/kubernetes/blob/v1.21.3/pkg/apis/storage/v1/util/helpers.go#L25
// but that would require adding the k8s import overrides to our go.mod
const IsDefaultStorageClassAnnotation = "storageclass.kubernetes.io/is-default-class"

const pvMigrateContainerName = "pvmigrate"

// Options is the set of options that should be provided to Migrate
type Options struct {
	SourceSCName         string
	DestSCName           string
	RsyncImage           string
	Namespace            string
	SetDefaults          bool
	VerboseCopy          bool
	SkipSourceValidation bool
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

	err = scaleDownPods(ctx, w, clientset, matchingPVCs, time.Second*5)
	if err != nil {
		return fmt.Errorf("failed to scale down pods: %w", err)
	}

	err = copyAllPVCs(ctx, w, clientset, options.SourceSCName, options.DestSCName, options.RsyncImage, matchingPVCs, options.VerboseCopy, time.Second)
	if err != nil {
		return err
	}

	for ns, nsPVCs := range matchingPVCs {
		for _, nsPVC := range nsPVCs {
			err = swapPVs(ctx, w, clientset, ns, nsPVC.Name)
			if err != nil {
				return fmt.Errorf("failed to swap PVs for PVC %s in %s: %w", nsPVC.Name, ns, err)
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

func copyAllPVCs(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, sourceSCName string, destSCName string, rsyncImage string, matchingPVCs map[string][]corev1.PersistentVolumeClaim, verboseCopy bool, waitTime time.Duration) error {
	// create a pod for each PVC migration, and wait for it to finish
	w.Printf("\nCopying data from %s PVCs to %s PVCs\n", sourceSCName, destSCName)
	for ns, nsPvcs := range matchingPVCs {
		for _, nsPvc := range nsPvcs {
			sourcePvcName, destPvcName := nsPvc.Name, newPvcName(nsPvc.Name)
			w.Printf("Copying data from %s (%s) to %s in %s\n", sourcePvcName, nsPvc.Spec.VolumeName, destPvcName, ns)

			err := copyOnePVC(ctx, w, clientset, ns, sourcePvcName, destPvcName, rsyncImage, verboseCopy, waitTime)
			if err != nil {
				return fmt.Errorf("failed to copy PVC %s in %s: %w", nsPvc.Name, ns, err)
			}
		}
	}
	return nil
}

func copyOnePVC(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, ns string, sourcePvcName string, destPvcName string, rsyncImage string, verboseCopy bool, waitTime time.Duration) error {
	createdPod, err := createMigrationPod(ctx, clientset, ns, sourcePvcName, destPvcName, rsyncImage)
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

	// wait for the pod to be created
	time.Sleep(waitTime)
	for {
		gotPod, err := clientset.CoreV1().Pods(ns).Get(ctx, createdPod.Name, metav1.GetOptions{})
		if err != nil {
			w.Printf("failed to get newly created migration pod %s: %v\n", createdPod.Name, err)
			continue
		}

		if gotPod.Status.Phase == corev1.PodPending {
			time.Sleep(waitTime)
			continue
		}

		if gotPod.Status.Phase == corev1.PodRunning || gotPod.Status.Phase == corev1.PodSucceeded {
			// time to get logs
			break
		}

		w.Printf("got status %s for pod %s, this is likely an error\n", gotPod.Status.Phase, gotPod.Name)
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
				continue
			}

			if gotPod.Status.Phase != corev1.PodRunning {
				// if the pod is not running, go to the "validate success" section
				break
			}

			//if the pod is running, wait to see if getting logs works in a few seconds
			time.Sleep(waitTime)
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
	for true {
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

		time.Sleep(waitTime)
	}

	w.Printf("finished migrating PVC %s\n", sourcePvcName)
	return nil
}

func createMigrationPod(ctx context.Context, clientset k8sclient.Interface, ns string, sourcePvcName string, destPvcName string, rsyncImage string) (*corev1.Pod, error) {
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
						"--no-o",   // no-owner
						"--no-g",   // no-group
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
func getPVCs(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, sourceSCName, destSCName string, Namespace string) (map[string][]corev1.PersistentVolumeClaim, []string, error) {
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
	matchingPVCs := map[string][]corev1.PersistentVolumeClaim{}
	for _, pv := range matchingPVs {
		if pv.Spec.ClaimRef != nil {
			pvc, err := clientset.CoreV1().PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(ctx, pv.Spec.ClaimRef.Name, metav1.GetOptions{})
			if err != nil {
				return nil, nil, fmt.Errorf("failed to get PVC for PV %s in %s: %w", pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace, err)
			}

			if pv.Spec.ClaimRef.Namespace == Namespace || Namespace == "" {
				matchingPVCs[pv.Spec.ClaimRef.Namespace] = append(matchingPVCs[pv.Spec.ClaimRef.Namespace], *pvc)
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
			pvCap := pvsByName[nsPvc.Spec.VolumeName].Spec.Capacity
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t\n", ns, nsPvc.Name, nsPvc.Spec.VolumeName, pvCap.Storage().String())
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
			newName := newPvcName(nsPvc.Name)

			desiredPV, ok := pvsByName[nsPvc.Spec.VolumeName]
			if !ok {
				return nil, nil, fmt.Errorf("failed to find existing PV %s for PVC %s in %s", nsPvc.Spec.VolumeName, nsPvc.Name, ns)
			}

			desiredPvStorage, ok := desiredPV.Spec.Capacity[corev1.ResourceStorage]
			if !ok {
				return nil, nil, fmt.Errorf("failed to find storage capacity for PV %s for PVC %s in %s", nsPvc.Spec.VolumeName, nsPvc.Name, ns)
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
						return nil, nil, fmt.Errorf("PVC %s already exists in namespace %s but with size %s instead of %s, cannot create migration target from %s - please delete this to continue", newName, ns, existingSize, desiredPvStorage.String(), nsPvc.Name)
					}
				} else {
					return nil, nil, fmt.Errorf("PVC %s already exists in namespace %s but with storage class %v, cannot create migration target from %s - please delete this to continue", newName, ns, existingPVC.Spec.StorageClassName, nsPvc.Name)
				}
			}

			// if it doesn't already exist, create it
			newPVC, err := clientset.CoreV1().PersistentVolumeClaims(ns).Create(ctx, &corev1.PersistentVolumeClaim{
				TypeMeta: nsPvc.TypeMeta,
				ObjectMeta: metav1.ObjectMeta{
					Name:      newName,
					Namespace: ns,
					Labels: map[string]string{
						baseAnnotation: nsPvc.Name,
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
					AccessModes: nsPvc.Spec.AccessModes,
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
func scaleDownPods(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, matchingPVCs map[string][]corev1.PersistentVolumeClaim, checkInterval time.Duration) error {
	// get pods using specified PVCs
	matchingPods := map[string][]corev1.Pod{}
	matchingPodsCount := 0
	for ns, nsPvcs := range matchingPVCs {
		nsPods, err := clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("failed to get pods in %s: %w", ns, err)
		}
		for _, nsPod := range nsPods.Items {

		perPodLoop:
			for _, podVol := range nsPod.Spec.Volumes {
				if podVol.PersistentVolumeClaim != nil {
					for _, nsClaim := range nsPvcs {
						if podVol.PersistentVolumeClaim.ClaimName == nsClaim.Name {
							matchingPods[ns] = append(matchingPods[ns], nsPod)
							matchingPodsCount++
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
		return fmt.Errorf("failed to print Pods: %w", err)
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
						return fmt.Errorf("migration pod %s in %s leftover from a previous run failed to delete, please delete it before retrying: %w", nsPod.Name, ns, err)
					}
				} else {
					// TODO: handle properly
					return fmt.Errorf("pod %s in %s did not have any owners!\nPlease delete it before retrying", nsPod.Name, ns)
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
						return fmt.Errorf("failed to get statefulset %s scale in %s: %w", ownerName, ns, err)
					}

					formerScale := int32(1)
					if ss.Spec.Replicas != nil {
						formerScale = *ss.Spec.Replicas
					}
					int320 := int32(0)
					ss.Spec.Replicas = &int320

					// add an annotation with the current scale (if it does not already exist)
					if ss.ObjectMeta.Annotations == nil {
						ss.ObjectMeta.Annotations = map[string]string{}
					}
					if _, ok := ss.ObjectMeta.Annotations[scaleAnnotation]; !ok {
						ss.ObjectMeta.Annotations[scaleAnnotation] = fmt.Sprintf("%d", formerScale)
					}

					w.Printf("scaling StatefulSet %s from %d to 0 in %s\n", ownerName, formerScale, ns)
					_, err = clientset.AppsV1().StatefulSets(ns).Update(ctx, ss, metav1.UpdateOptions{})
					if err != nil {
						return fmt.Errorf("failed to scale statefulset %s to zero in %s: %w", ownerName, ns, err)
					}
				case "ReplicaSet":
					rs, err := clientset.AppsV1().ReplicaSets(ns).Get(ctx, ownerName, metav1.GetOptions{})
					if err != nil {
						return fmt.Errorf("failed to get replicaset %s in %s: %w", ownerName, ns, err)
					}

					if len(rs.OwnerReferences) != 1 {
						return fmt.Errorf("expected 1 owner for replicaset %s in %s, found %d instead", ownerName, ns, len(rs.OwnerReferences))
					}
					if rs.OwnerReferences[0].Kind != "Deployment" {
						return fmt.Errorf("expected owner for replicaset %s in %s to be a deployment, found %s of kind %s instead", ownerName, ns, rs.OwnerReferences[0].Name, rs.OwnerReferences[0].Kind)
					}

					dep, err := clientset.AppsV1().Deployments(ns).Get(ctx, rs.OwnerReferences[0].Name, metav1.GetOptions{})
					if err != nil {
						return fmt.Errorf("failed to get deployment %s scale in %s: %w", ownerName, ns, err)
					}

					formerScale := int32(1)
					if dep.Spec.Replicas != nil {
						formerScale = *dep.Spec.Replicas
					}
					int320 := int32(0)
					dep.Spec.Replicas = &int320

					// add an annotation with the current scale (if it does not already exist)
					if dep.ObjectMeta.Annotations == nil {
						dep.ObjectMeta.Annotations = map[string]string{}
					}
					if _, ok := dep.ObjectMeta.Annotations[scaleAnnotation]; !ok {
						dep.ObjectMeta.Annotations[scaleAnnotation] = fmt.Sprintf("%d", formerScale)
					}

					w.Printf("scaling Deployment %s from %d to 0 in %s\n", ownerName, formerScale, ns)
					_, err = clientset.AppsV1().Deployments(ns).Update(ctx, dep, metav1.UpdateOptions{})
					if err != nil {
						return fmt.Errorf("failed to scale statefulset %s to zero in %s: %w", ownerName, ns, err)
					}
				default:
					return fmt.Errorf("scaling pods controlled by a %s is not supported, please delete the pods controlled by %s in %s before retrying", ownerKind, ownerKind, ns)
				}
			}
		}
	}

	// wait for all pods to be deleted
	w.Printf("\nWaiting for pods with mounted PVCs to be cleaned up\n")
	time.Sleep(checkInterval / 16)
checkPvcPodLoop:
	for true {
		for ns, nsPvcs := range matchingPVCs {
			nsPods, err := clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
			if err != nil {
				return fmt.Errorf("failed to get pods in %s: %w", ns, err)
			}
			for _, nsPod := range nsPods.Items {
				for _, podVol := range nsPod.Spec.Volumes {
					if podVol.PersistentVolumeClaim != nil {
						for _, nsClaim := range nsPvcs {
							if podVol.PersistentVolumeClaim.ClaimName == nsClaim.Name {
								if nsPod.CreationTimestamp.After(migrationStartTime) {
									return fmt.Errorf("pod %s in %s mounting %s was created at %s, after scale-down started at %s. It is likely that there is some other operator scaling this back up", nsPod.Name, ns, nsClaim.Name, nsPod.CreationTimestamp.Format(time.RFC3339), migrationStartTime.Format(time.RFC3339))
								}

								w.Printf("Found pod %s in %s mounting to-be-migrated PVC %s, waiting\n", nsPod.Name, ns, nsClaim.Name)
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
	return nil
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
			if desiredScale, ok := ss.ObjectMeta.Annotations[scaleAnnotation]; ok {
				desiredScaleInt, err := strconv.Atoi(desiredScale)
				if err != nil {
					return fmt.Errorf("failed to parse scale %q for StatefulSet %s in %s: %w", desiredScale, ss.Name, ns, err)
				}
				delete(ss.ObjectMeta.Annotations, scaleAnnotation)
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
			if desiredScale, ok := dep.ObjectMeta.Annotations[scaleAnnotation]; ok {
				desiredScaleInt, err := strconv.Atoi(desiredScale)
				if err != nil {
					return fmt.Errorf("failed to parse scale %q for Deployment %s in %s: %w", desiredScale, dep.Name, ns, err)
				}
				delete(dep.ObjectMeta.Annotations, scaleAnnotation)
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
		if volume.Spec.ClaimRef == nil {
			return true
		}
		return false
	})
	if err != nil {
		return fmt.Errorf("failed to remove claimrefs from PV %s: %w", originalPVC.Spec.VolumeName, err)
	}
	w.Printf("Removing claimref from migrated-to PV %s\n", migratedPVC.Spec.VolumeName)
	err = mutatePV(ctx, w, clientset, migratedPVC.Spec.VolumeName, func(volume *corev1.PersistentVolume) *corev1.PersistentVolume {
		volume.Spec.ClaimRef = nil
		return volume
	}, func(volume *corev1.PersistentVolume) bool {
		if volume.Spec.ClaimRef == nil {
			return true
		}
		return false
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
	for true {
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
