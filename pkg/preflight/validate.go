package preflight

import (
	"context"
	"fmt"
	"io"
	"log"
	"text/tabwriter"
	"time"

	"github.com/replicatedhq/pvmigrate/pkg/k8sutil"
	"github.com/replicatedhq/pvmigrate/pkg/migrate"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	k8spodutils "k8s.io/kubernetes/pkg/api/v1/pod"

	"k8s.io/utils/pointer"
)

const (
	pvcNamePrefix = "pf-pvc"
	podNamePrefix = "pvmigrate-pf-pod"
)

type pvcFailure struct {
	reason  string
	from    string
	message string
}

type ValidationFailure struct {
	Namespace string
	Resource  string
	Source    string
	Message   string
}

// Validate runs preflight check on storage volumes returning a list of failures
func Validate(ctx context.Context, w *log.Logger, clientset k8sclient.Interface, options migrate.Options) ([]ValidationFailure, error) {
	// validate storage classes
	scFailures, err := validateStorageClasses(ctx, w, clientset, options.SourceSCName, options.DestSCName)
	if err != nil {
		return nil, fmt.Errorf("failed to validate storage classes: %w", err)
	}

	// if there are storage class validation failures it doesn't make sense to proceed
	if scFailures != nil {
		return scFailures, nil
	}

	// validate access modes for all PVCs using the source storage class
	pvcs, err := pvcsForStorageClass(ctx, w, clientset, options.SourceSCName, options.Namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get PVCs for storage %s: %w", options.SourceSCName, err)
	}
	pvcAccesModeFailures, err := validateVolumeAccessModes(ctx, w, clientset, options.DestSCName, options.RsyncImage, options.PodReadyTimeout, pvcs)
	if err != nil {
		return nil, fmt.Errorf("failed to validate PVC access modes: %w", err)
	}
	return toValidationFailures(pvcAccesModeFailures), nil
}

// PrintPVAccessModeErrors prints and formats the volume access mode errors in pvcErrors
func PrintValidationFailures(stream io.Writer, failures []ValidationFailure) {
	tw := tabwriter.NewWriter(stream, 0, 8, 8, '\t', 0)
	fmt.Fprintf(tw, "The following resources failed validation:\n")
	fmt.Fprintln(tw, "NAMESPACE\tRESOURCE\tMESSAGE")
	fmt.Fprintf(tw, "---------\t--------\t-------\n")
	for _, failure := range failures {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", failure.Namespace, failure.Resource, failure.Message)
	}
	tw.Flush()
}

func toValidationFailures(pvcFailures map[string]map[string]pvcFailure) []ValidationFailure {
	var vFailures []ValidationFailure
	for ns, failures := range pvcFailures {
		for name, pvcFailure := range failures {
			vFailures = append(vFailures, ValidationFailure{ns, "pvc/" + name, pvcFailure.from, pvcFailure.message})
		}
	}
	return vFailures
}

// validateVolumeAccessModes checks whether the provided persistent volumes support the access modes
// of the destination storage class.
// returns a map of pvc validation failures indexed by namespace
func validateVolumeAccessModes(ctx context.Context, l *log.Logger, client k8sclient.Interface, dstSC string, tmpPodImage string, podReadyTimeout time.Duration, pvcs map[string]corev1.PersistentVolumeClaim) (map[string]map[string]pvcFailure, error) {
	volAccessModeFailures := make(map[string]map[string]pvcFailure)

	if _, err := client.StorageV1().StorageClasses().Get(ctx, dstSC, metav1.GetOptions{}); err != nil {
		return nil, fmt.Errorf("failed to get destination storage class %s: %w", dstSC, err)
	}

	for _, pvc := range pvcs {
		v, err := checkVolumeAccessModes(ctx, l, client, dstSC, pvc, podReadyTimeout, tmpPodImage)
		if err != nil {
			l.Printf("failed to check volume access mode for PVC %s: %s", pvc.Name, err)
			continue
		}
		if v != nil {
			volAccessModeFailures[pvc.Namespace] = map[string]pvcFailure{pvc.Name: *v}
		}
	}
	return volAccessModeFailures, nil
}

// validateStorageClasses returns any failures encountered when discovering the source and destination
// storage classes
func validateStorageClasses(ctx context.Context, l *log.Logger, clientset k8sclient.Interface, sourceSCName, destSCName string) ([]ValidationFailure, error) {
	// get storage providers
	storageClasses, err := clientset.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list storage classes: %w", err)
	}

	var sourceScFound, destScFound bool
	var scFailures []ValidationFailure
	for _, sc := range storageClasses.Items {
		if sc.Name == sourceSCName {
			sourceScFound = true
		}
		if sc.Name == destSCName {
			destScFound = true
		}
	}
	if !sourceScFound {
		scFailures = append(scFailures, ValidationFailure{Resource: "sc/" + sourceSCName, Message: "Resource not found"})
	}
	if !destScFound {
		scFailures = append(scFailures, ValidationFailure{Resource: "sc/" + destSCName, Message: "Resource not found"})
	}
	return scFailures, nil
}

// buildTmpPVCConsumerPod creates a pod spec for consuming a pvc
func buildTmpPVCConsumerPod(pvcName, namespace, image string) *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      k8sutil.NewPrefixedName(podNamePrefix, pvcName),
			Namespace: namespace,
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
					Name:  "sleep",
					Image: image,
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

// buildTmpPVC creates a temporary PVC requesting for 1Mi of storage for a provided storage class name.
func buildTmpPVC(pvc corev1.PersistentVolumeClaim, sc string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k8sutil.NewPrefixedName(pvcNamePrefix, pvc.Name),
			Namespace: pvc.Namespace,
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
	}
}

// checkVolumeAccessModes checks if the access modes of a pv are supported by the
// destination storage class.
func checkVolumeAccessModes(ctx context.Context, l *log.Logger, client k8sclient.Interface, dstSC string, pvc corev1.PersistentVolumeClaim, timeout time.Duration, tmpPodImage string) (*pvcFailure, error) {
	var err error

	// create temp pvc for storage class
	tmpPVCSpec := buildTmpPVC(pvc, dstSC)
	tmpPVC, err := client.CoreV1().PersistentVolumeClaims(tmpPVCSpec.Namespace).Create(
		ctx, tmpPVCSpec, metav1.CreateOptions{})
	if err != nil {
		if !k8serrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("failed to create temporary pvc %s: %w", tmpPVCSpec.Name, err)
		}

		// PVC exist, get it
		tmpPVC, err = client.CoreV1().PersistentVolumeClaims(tmpPVCSpec.Namespace).Get(ctx, tmpPVCSpec.Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get existing temp PVC %s: %w", tmpPVCSpec.Name, err)
		}
	}

	// consume pvc to determine any access mode errors
	pvcConsumerPodSpec := buildTmpPVCConsumerPod(tmpPVC.Name, tmpPVC.Namespace, tmpPodImage)
	pvcConsumerPod, err := client.CoreV1().Pods(tmpPVC.Namespace).Create(ctx, pvcConsumerPodSpec, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create validation pod %s: %w", pvcConsumerPodSpec.Name, err)
	}

	// cleanup pvc and pod at the end
	defer func() {
		// pod must be deleted first then the pvc
		if err = deletePVConsumerPod(client, pvcConsumerPod); err != nil {
			l.Printf("failed to cleanup pv consumer pod %s: %s", pvcConsumerPod.Name, err)
		}

		if err = deleteTmpPVC(l, client, tmpPVC); err != nil {
			l.Printf("failed to cleanup tmp claim: %s", err)
		}
	}()

	podReadyTimeoutEnd := time.Now().Add(timeout)
	for {
		gotPod, err := client.CoreV1().Pods(pvcConsumerPodSpec.Namespace).Get(ctx, pvcConsumerPodSpec.Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get pv consumer pod %s: %w", gotPod.Name, err)
		}

		switch {
		case k8spodutils.IsPodReady(gotPod):
			return nil, nil
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
				pvcPendingError, err := getPVCError(client, tmpPVC)
				if err != nil {
					return nil, fmt.Errorf("failed to get pvc failure: %w", err)
				}
				return pvcPendingError, nil
			}
			// pod failed for other reason(s)
			return nil, fmt.Errorf("unexpected status for pod %s: %s", gotPod.Name, gotPod.Status.Phase)
		}
	}
}

// deleteTmpPVC deletes the provided pvc from the default namespace and waits until the
// backing pv dissapear as well (this is mandatory so we don't leave any orphan pv as this would
// cause pvmigrate to fail). this function has a timeout of 5 minutes, after that an error is
// returned.
func deleteTmpPVC(l *log.Logger, client k8sclient.Interface, pvc *corev1.PersistentVolumeClaim) error {
	// Cleanup should use background context so as not to fail if context has already been canceled
	ctx := context.Background()

	pvs, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
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
	if err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(
		ctx, pvc.Name, delopts,
	); err != nil {
		if !k8serrors.IsNotFound(err) {
			l.Printf("failed to delete temp pvc %s: %s", pvc.Name, err)
			return err
		}
	}
	waitFor = append(waitFor, pvc.Name)

	timeout := time.After(5 * time.Minute)
	interval := time.NewTicker(5 * time.Second)
	defer interval.Stop()
	for _, pvc := range waitFor {
		pv, ok := pvsByPVCName[pvc]
		if !ok {
			l.Printf("failed to find pv for temp pvc %s", pvc)
			continue
		}

		for {
			// break the loop as soon as we can't find the pv anymore.
			if _, err := client.CoreV1().PersistentVolumes().Get(
				ctx, pv.Name, metav1.GetOptions{},
			); err != nil && !k8serrors.IsNotFound(err) {
				l.Printf("failed to get pv for temp pvc %s: %s", pvc, err)
			} else if err != nil && k8serrors.IsNotFound(err) {
				break
			}

			select {
			case <-interval.C:
				continue
			case <-timeout:
				return fmt.Errorf("failed to delete pvs: timeout")
			}
		}
	}
	return nil
}

// deletePVConsumerPod removes the pod resource from the api servere
func deletePVConsumerPod(client k8sclient.Interface, pod *corev1.Pod) error {
	propagation := metav1.DeletePropagationForeground
	delopts := metav1.DeleteOptions{GracePeriodSeconds: pointer.Int64(0), PropagationPolicy: &propagation}
	if pod != nil {
		if err := client.CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, delopts); err != nil {
			return err
		}
	}
	return nil
}

// getPVCError returns the failure event for why a PVC is in Pending status
func getPVCError(client k8sclient.Interface, pvc *corev1.PersistentVolumeClaim) (*pvcFailure, error) {
	// no need to inspect pvc if it's NOT in Pending phase
	if pvc.Status.Phase != corev1.ClaimPending {
		return nil, fmt.Errorf("PVC %s is not in Pending status", pvc.Name)
	}

	pvcEvents, err := client.CoreV1().Events(pvc.Namespace).Search(scheme.Scheme, pvc)
	if err != nil {
		return nil, fmt.Errorf("failed to list events for PVC %s: %w", pvc.Name, err)
	}

	// get pending reason
	for _, event := range pvcEvents.Items {
		if event.Reason == "ProvisioningFailed" {
			return &pvcFailure{event.Reason, event.Source.Component, event.Message}, nil
		}
	}
	return nil, fmt.Errorf("could not determine reason for why PVC %s is in Pending status", pvc.Name)
}

// pvcsForStorageClass returns all PersistentVolumeClaims, filtered by namespace, for a given
// storage class
func pvcsForStorageClass(ctx context.Context, l *log.Logger, client k8sclient.Interface, srcSC, namespace string) (map[string]corev1.PersistentVolumeClaim, error) {
	srcPVs, err := k8sutil.PVsByStorageClass(ctx, client, srcSC)
	if err != nil {
		return nil, fmt.Errorf("failed to get PVs for storage class %s: %w", srcSC, err)
	}

	// get PVCs using specified PVs
	srcPVCs := map[string]corev1.PersistentVolumeClaim{}
	for _, pv := range srcPVs {
		if pv.Spec.ClaimRef != nil {
			pvc, err := client.CoreV1().PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(ctx, pv.Spec.ClaimRef.Name, metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to get PVC for PV %s in %s: %w", pv.Spec.ClaimRef.Name, pv.Spec.ClaimRef.Namespace, err)
			}
			if pv.Spec.ClaimRef.Namespace == namespace || namespace == "" {
				srcPVCs[pv.Spec.ClaimRef.Name] = *pvc.DeepCopy()
			}
		} else {
			return nil, fmt.Errorf("PV %s does not have an associated PVC", pv.Name)
		}
	}
	return srcPVCs, nil
}
