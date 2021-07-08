package migrate

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sclient "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

const baseAnnotation = "kurl.sh/pvcmigrate"
const scaleAnnotation = baseAnnotation + "-scale"
const kindAnnotation = baseAnnotation + "-kind"

// Cli uses CLI options to run Migrate
func Cli() {
	var sourceSCName string
	var destSCName string
	var rsyncImage string
	var setDefaults bool
	var verboseCopy bool

	flag.StringVar(&sourceSCName, "source-sc", "", "storage provider name to migrate from")
	flag.StringVar(&destSCName, "dest-sc", "", "storage provider name to migrate to")
	flag.StringVar(&rsyncImage, "rsync-image", "eeacms/rsync:2.3", "the image to use to copy PVCs - must have 'rsync' on the path")
	flag.BoolVar(&setDefaults, "set-defaults", true, "change default storage class from source to dest")
	flag.BoolVar(&verboseCopy, "verbose-copy", false, "show output from the rsync command used to copy data between PVCs")

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

	err = Migrate(context.TODO(), output, clientset, sourceSCName, destSCName, rsyncImage, setDefaults, verboseCopy)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		os.Exit(1)
	}
}

// Migrate moves data and PVCs from one StorageClass to another
func Migrate(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, sourceSCName, destSCName, rsyncImage string, setDefaults, verboseCopy bool) error {
	err := validateStorageClasses(ctx, w, clientset, sourceSCName, destSCName)
	if err != nil {
		return err
	}

	matchingPVCs, newPVCs, originalRetentionPolicies, originalPVNames, namespaces, err := getPVCs(ctx, w, clientset, sourceSCName, destSCName)
	if err != nil {
		return err
	}

	err = scaleDownPods(ctx, w, clientset, matchingPVCs)
	if err != nil {
		return fmt.Errorf("failed to scale down pods: %w", err)
	}

	err = copyAllPVCs(ctx, w, clientset, sourceSCName, destSCName, rsyncImage, matchingPVCs, verboseCopy)
	if err != nil {
		return err
	}

	// mark previously existing PVs as 'retain' so that when we delete the PVC it does not take the PV with it (in case things go wrong)
	w.Printf("\nMarking previously existing PVs as to-be-retained\n")
	for _, pvname := range originalPVNames {
		w.Printf("Marking PV %s as to-be-retained\n", pvname)
		err = mutatePV(ctx, w, clientset, pvname, func(volume *corev1.PersistentVolume) *corev1.PersistentVolume {
			volume.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
			return volume
		}, func(volume *corev1.PersistentVolume) bool {
			return volume.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain
		})
		if err != nil {
			return fmt.Errorf("failed to set PV reclaim policy: %w", err)
		}
	}

	// mark newly created PVs as 'retain'
	w.Printf("\nMarking newly-created existing PVs as to-be-retained\n")
	allPVs, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to get persistent volumes: %w", err)
	}
	desiredPVRetentions := map[string]corev1.PersistentVolumeReclaimPolicy{}
	for _, pv := range allPVs.Items {
		if pv.Spec.StorageClassName == destSCName && pv.Spec.ClaimRef != nil {
			if newNsPVCs, ok := newPVCs[pv.Spec.ClaimRef.Namespace]; ok {
				isMatch := false
				for idx, newNsPvc := range newNsPVCs {
					if newNsPvc.Name == pv.Spec.ClaimRef.Name {
						isMatch = true
						newPVCs[pv.Spec.ClaimRef.Namespace][idx].Spec.VolumeName = pv.Name // ensure that the PVC has a PV name associated with it, as we use that again later
						break
					}
				}

				// if this PV corresponds to one of the ones we created to migrate to, then set the reclaim policy
				if isMatch {
					w.Printf("Marking PV %s (PVC %s in %s) as to-be-retained\n", pv.Name, pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace)
					err = mutatePV(ctx, w, clientset, pv.Name, func(volume *corev1.PersistentVolume) *corev1.PersistentVolume {
						volume.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
						return volume
					}, func(volume *corev1.PersistentVolume) bool {
						return volume.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain
					})
					if err != nil {
						return fmt.Errorf("failed to set PV reclaim policy: %w", err)
					}

					desiredPVRetentions[pv.Name] = originalRetentionPolicies[pv.Spec.ClaimRef.Namespace][originalPvcName(pv.Spec.ClaimRef.Name)]
				}
			}
		}
	}

	// delete all the original PVCs to free up names
	w.Printf("\nDeleting original PVCs to free up names\n")
	for ns, nsPVCs := range matchingPVCs {
		for _, nsPVC := range nsPVCs {
			w.Printf("Deleting original PVC %s in %s\n", nsPVC.Name, ns)

			// delete the PVC so that we can create a new PVC with the original name
			err = clientset.CoreV1().PersistentVolumeClaims(ns).Delete(ctx, nsPVC.Name, metav1.DeleteOptions{})
			if err != nil {
				return fmt.Errorf("failed to delete PVC %s of ns %s to allow a new one to be created: %w", nsPVC.Name, ns, err)
			}
		}
	}

	// delete migrated-to PVCs to free up PVs
	w.Printf("\nDeleting migrated-to PVCs to free up PVs\n")
	for ns, nsPVCs := range newPVCs {
		for _, nsPVC := range nsPVCs {
			w.Printf("Deleting migrated PVC %s in %s\n", nsPVC.Name, ns)

			// delete the PVC so that we can create a new PVC with the original name
			err = clientset.CoreV1().PersistentVolumeClaims(ns).Delete(ctx, nsPVC.Name, metav1.DeleteOptions{})
			if err != nil {
				return fmt.Errorf("failed to delete PVC %s of ns %s to allow a new one to be created: %w", nsPVC.Name, ns, err)
			}
		}
	}

	w.Printf("\nRemoving claimrefs from PVs to attach new PVCs\n")
	// original PVs
	for _, pvname := range originalPVNames {
		w.Printf("Removing claimrefs from PV %s\n", pvname)
		err = mutatePV(ctx, w, clientset, pvname, func(volume *corev1.PersistentVolume) *corev1.PersistentVolume {
			volume.Spec.ClaimRef = nil
			return volume
		}, func(volume *corev1.PersistentVolume) bool {
			if volume.Spec.ClaimRef == nil {
				return true
			}
			w.Printf("claimref for %s: %+v\n", pvname, volume.Spec.ClaimRef)
			return false
		})
		if err != nil {
			return fmt.Errorf("failed to remove claimrefs from PV %s: %w", pvname, err)
		}
	}

	// new PVs
	for pvname := range desiredPVRetentions {
		w.Printf("Removing claimrefs from PV %s\n", pvname)
		err = mutatePV(ctx, w, clientset, pvname, func(volume *corev1.PersistentVolume) *corev1.PersistentVolume {
			volume.Spec.ClaimRef = nil
			return volume
		}, func(volume *corev1.PersistentVolume) bool {
			if volume.Spec.ClaimRef == nil {
				return true
			}
			w.Printf("claimref for %s: %+v\n", pvname, volume.Spec.ClaimRef)
			return false
		})
		if err != nil {
			return fmt.Errorf("failed to remove claimrefs from PV %s: %w", pvname, err)
		}
	}

	w.Printf("\nCreating new PVCs with the original names\n")
	// make a new PVC with the updated name that owns the PV in question
	for ns, nsPVCs := range newPVCs { // TODO: change this to be for each old PVC, so that we retain annotations/labels/access modes
		for _, newPVC := range nsPVCs {
			originalName := originalPvcName(newPVC.Name)
			w.Printf("Creating PVC %s in %s using PV %s\n", originalName, ns, newPVC.Spec.VolumeName)

			// create a new PVC referencing the PV we copied data to, but with the original name
			newPVC.Status = corev1.PersistentVolumeClaimStatus{}
			newPVC.ObjectMeta = metav1.ObjectMeta{
				Name:      originalName,
				Namespace: ns,
			}
			newPVC.Spec.DataSource = nil
			_, err = clientset.CoreV1().PersistentVolumeClaims(ns).Create(ctx, &newPVC, metav1.CreateOptions{})
			if err != nil {
				w.Printf("failed to create PVC %s of ns %s with intended PV %s, you will likely need to do this manually: %v\n", originalName, ns, newPVC.Spec.VolumeName, err)
				delete(desiredPVRetentions, newPVC.Spec.VolumeName) // don't reset retention policies if the PVC wasn't able to be created
			}
		}
	}

	w.Printf("\nResetting PV retention policies\n")
	for pvname, desired := range desiredPVRetentions {
		err = mutatePV(ctx, w, clientset, pvname, func(volume *corev1.PersistentVolume) *corev1.PersistentVolume {
			volume.Spec.PersistentVolumeReclaimPolicy = desired
			return volume
		}, func(volume *corev1.PersistentVolume) bool {
			return volume.Spec.PersistentVolumeReclaimPolicy == desired
		})
		if err != nil {
			return fmt.Errorf("failed to return PV reclaim policy: %w", err)
		}
	}

	// scale back deployments/daemonsets/statefulsets
	err = scaleUpPods(ctx, w, clientset, namespaces)
	if err != nil {
		return fmt.Errorf("failed to scale up pods: %w", err)
	}

	// delete migrated PVs
	w.Printf("\nDeleting original PVs\n")
	for _, pvname := range originalPVNames {
		err = mutatePV(ctx, w, clientset, pvname, func(volume *corev1.PersistentVolume) *corev1.PersistentVolume {
			volume.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
			return volume
		}, func(volume *corev1.PersistentVolume) bool {
			return volume.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimDelete
		})
		if err != nil {
			w.Printf("failed to mark old and redundant PV %s as having a reclaim policy of 'Delete': %v\n", pvname, err)
		}

		err = clientset.CoreV1().PersistentVolumes().Delete(ctx, pvname, metav1.DeleteOptions{})
		if err != nil {
			w.Printf("failed to remove old and redundant PV %s: %v\n", pvname, err)
		}
	}

	w.Printf("\nSuccess!\n")
	return nil
}

func copyAllPVCs(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, sourceSCName string, destSCName string, rsyncImage string, matchingPVCs map[string][]corev1.PersistentVolumeClaim, verboseCopy bool) error {
	// create a pod for each PVC migration, and wait for it to finish
	w.Printf("\nCopying data from %s PVCs to %s PVCs\n", sourceSCName, destSCName)
	for ns, nsPvcs := range matchingPVCs {
		for _, nsPvc := range nsPvcs {
			sourcePvcName, destPvcName := nsPvc.Name, newPvcName(nsPvc.Name)
			w.Printf("Copying data from %s (%s) to %s in %s\n", sourcePvcName, nsPvc.Spec.VolumeName, destPvcName, ns)

			err := copyOnePVC(ctx, w, clientset, ns, sourcePvcName, destPvcName, rsyncImage, verboseCopy)
			if err != nil {
				return fmt.Errorf("failed to copy PVC %s in %s: %w", nsPvc.Name, ns, err)
			}
		}
	}
	return nil
}

func copyOnePVC(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, ns string, sourcePvcName string, destPvcName string, rsyncImage string, verboseCopy bool) error {
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
	time.Sleep(time.Second * 1)
	for {
		gotPod, err := clientset.CoreV1().Pods(ns).Get(ctx, createdPod.Name, metav1.GetOptions{})
		if err != nil {
			w.Printf("failed to get newly created migration pod %s: %v\n", createdPod.Name, err)
			continue
		}

		if gotPod.Status.Phase == corev1.PodPending {
			time.Sleep(time.Second * 1)
			continue
		}

		if gotPod.Status.Phase == corev1.PodRunning || gotPod.Status.Phase == corev1.PodSucceeded {
			// time to get logs
			break
		}

		w.Printf("got status %s for pod %s, this is likely an error\n", gotPod.Status.Phase, gotPod.Name)
	}

	podLogsReq := clientset.CoreV1().Pods(ns).GetLogs(createdPod.Name, &corev1.PodLogOptions{
		Follow: true,
	})
	podLogs, err := podLogsReq.Stream(ctx)
	if err != nil {
		w.Printf("failed to get logs for migration pod %s: %v\n", createdPod.Name, err)
		// os.Exit(1) // TODO handle
	}

	w.Printf("migrating PVC %s:\n", sourcePvcName)
	for {
		bufPodLogs := bufio.NewReader(podLogs)
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
			w.Print(fmt.Sprintf(".")) // one dot per line of output
		}
	}
	if !verboseCopy {
		w.Print(fmt.Sprintf("done!\n")) // add a newline at the end of the dots if not showing pod logs
	}

	err = podLogs.Close()
	if err != nil {
		w.Printf("failed to close logs for migration pod %s: %v\n", createdPod.Name, err)
		// os.Exit(1) // TODO handle
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

		time.Sleep(time.Second * 5)
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
					Name:  "pvmigrate-" + sourcePvcName,
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
// a map of namespaces to arrays of to-be-migrated-to PVCs
// a map of namespaces to PVC names to PV reclaim policies
// an array of the original PV names being migrated
// an array of namespaces that the PVCs were found within
func getPVCs(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, sourceSCName, destSCName string) (map[string][]corev1.PersistentVolumeClaim, map[string][]corev1.PersistentVolumeClaim, map[string]map[string]corev1.PersistentVolumeReclaimPolicy, []string, []string, error) {
	// get PVs using the specified storage provider
	pvs, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("failed to get persistent volumes: %w", err)
	}
	matchingPVs := []corev1.PersistentVolume{}
	originalPVNames := []string{}
	for _, pv := range pvs.Items {
		if pv.Spec.StorageClassName == sourceSCName {
			matchingPVs = append(matchingPVs, pv)
			originalPVNames = append(originalPVNames, pv.Name)
		} else {
			w.Printf("PV %s does not match source SC %s, not migrating\n", pv.Name, sourceSCName)
		}
	}

	// get PVCs (and namespaces) using specified PVs
	matchingPVCs := map[string][]corev1.PersistentVolumeClaim{}
	originalRetentionPolicies := map[string]map[string]corev1.PersistentVolumeReclaimPolicy{}
	namespaces := []string{}
	for _, pv := range matchingPVs {
		if pv.Spec.ClaimRef != nil {
			pvc, err := clientset.CoreV1().PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(ctx, pv.Spec.ClaimRef.Name, metav1.GetOptions{})
			if err != nil {
				return nil, nil, nil, nil, nil, fmt.Errorf("failed to get PVC for PV %s in %s: %w", pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace, err)
			}
			matchingPVCs[pv.Spec.ClaimRef.Namespace] = append(matchingPVCs[pv.Spec.ClaimRef.Namespace], *pvc)

			// save the original retention policy so we can reset it later
			if _, ok := originalRetentionPolicies[pv.Spec.ClaimRef.Namespace]; !ok {
				originalRetentionPolicies[pv.Spec.ClaimRef.Namespace] = map[string]corev1.PersistentVolumeReclaimPolicy{}
			}
			originalRetentionPolicies[pv.Spec.ClaimRef.Namespace][pv.Spec.ClaimRef.Name] = pv.Spec.PersistentVolumeReclaimPolicy
			namespaces = append(namespaces, pv.Spec.ClaimRef.Namespace)
		} else {
			return nil, nil, nil, nil, nil, fmt.Errorf("PV %s does not have an associated PVC - resolve this before rerunning", pv.Name)
		}
	}

	w.Printf("\nFound %d matching PVCs to migrate across %d namespaces:\n", len(originalPVNames), len(matchingPVCs))
	tw := tabwriter.NewWriter(w.Writer(), 2, 2, 1, ' ', 0)
	_, _ = fmt.Fprintf(tw, "namespace:\tpvc:\tpv:\t\n")
	for ns, nsPvcs := range matchingPVCs {
		for _, nsPvc := range nsPvcs {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t\n", ns, nsPvc.Name, nsPvc.Spec.VolumeName)
		}
	}
	err = tw.Flush()
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("failed to print PVCs: %w", err)
	}

	// create new PVCs for each matching PVC
	w.Printf("\nCreating new PVCs to migrate data to using the %s StorageClass\n", destSCName)
	newPVCs := map[string][]corev1.PersistentVolumeClaim{}
	for ns, nsPvcs := range matchingPVCs {
		for _, nsPvc := range nsPvcs {
			newName := newPvcName(nsPvc.Name)

			// check to see if the desired PVC name already exists (and is appropriate)
			existingPVC, err := clientset.CoreV1().PersistentVolumeClaims(ns).Get(ctx, newName, metav1.GetOptions{})
			if err != nil {
				if !k8serrors.IsNotFound(err) {
					return nil, nil, nil, nil, nil, fmt.Errorf("failed to find existing PVC: %w", err)
				}
			} else if existingPVC != nil {
				if existingPVC.Spec.StorageClassName != nil && *existingPVC.Spec.StorageClassName == destSCName {
					existingSize := existingPVC.Spec.Resources.Requests.Storage().String()
					desiredSize := nsPvc.Spec.Resources.Requests.Storage().String() // TODO: this should use the size of the PV, not the size of the PVC
					if existingSize == desiredSize {
						w.Printf("found existing PVC with name %s, not creating new one\n", newName)
						newPVCs[ns] = append(newPVCs[ns], *existingPVC)
						continue
					} else {
						return nil, nil, nil, nil, nil, fmt.Errorf("storage class %s already exists in namespace %s but with size %s instead of %s, cannot create migration target from %s - please delete this to continue", newName, ns, existingSize, desiredSize, nsPvc.Name)
					}
				} else {
					return nil, nil, nil, nil, nil, fmt.Errorf("storage class %s already exists in namespace %s but with storage class %v, cannot create migration target from %s - please delete this to continue", newName, ns, existingPVC.Spec.StorageClassName, nsPvc.Name)
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
					Resources:        nsPvc.Spec.Resources,
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
				},
			}, metav1.CreateOptions{})
			if err != nil {
				return nil, nil, nil, nil, nil, fmt.Errorf("failed to create new PVC %s in %s: %w", newName, ns, err)
			}
			w.Printf("created new PVC %s with size %v in %s\n", newName, newPVC.Spec.Resources.Requests.Storage().String(), ns)
			newPVCs[ns] = append(newPVCs[ns], *newPVC)
		}
	}

	return matchingPVCs, newPVCs, originalRetentionPolicies, originalPVNames, namespaces, nil
}

func validateStorageClasses(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, sourceSCName string, destSCName string) error {
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
		return fmt.Errorf("unable to find source StorageClass %s", sourceSCName)
	}
	if !destScFound {
		return fmt.Errorf("unable to find dest StorageClass %s", destSCName)
	}
	w.Printf("\nMigrating data from %s to %s\n", sourceSCName, destSCName)
	return nil
}

func newPvcName(originalName string) string {
	return originalName + "-pvcmigrate"
}

func originalPvcName(newName string) string {
	return strings.TrimSuffix(newName, "-pvcmigrate")
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

// TODO: add waitForCleanup param to allow testing this
func scaleDownPods(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, matchingPVCs map[string][]corev1.PersistentVolumeClaim) error {
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
				// TODO: handle properly
				return fmt.Errorf("pod %s in %s did not have any owners!\nPlease delete it before retrying", nsPod.Name, ns)
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
				case "Deployment":
					dep, err := clientset.AppsV1().Deployments(ns).Get(ctx, ownerName, metav1.GetOptions{})
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
checkPvcPodLoop:
	for true {
		time.Sleep(time.Second * 5) // don't check too often, as this loop is relatively expensive

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

								w.Printf("Found pod %s in %s mounting to-be-migrated PVC %s, sleeping again\n", nsPod.Name, ns, nsClaim.Name)
								continue checkPvcPodLoop // as soon as we find a matching pod, we know we need to wait another 30s
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
