package k8sutil

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// PVSByStorageClass returns a map of persistent volumes using the provided storage class name.
// returned pvs map is indexed by pv's name.
func PVsByStorageClass(ctx context.Context, cli kubernetes.Interface, scname string) (map[string]corev1.PersistentVolume, error) {
	if _, err := cli.StorageV1().StorageClasses().Get(ctx, scname, metav1.GetOptions{}); err != nil {
		return nil, fmt.Errorf("failed to get storage class %s: %w", scname, err)
	}

	allpvs, err := cli.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get persistent volumes: %w", err)
	}

	pvs := map[string]corev1.PersistentVolume{}
	for _, pv := range allpvs.Items {
		if pv.Spec.StorageClassName != scname {
			continue
		}
		pvs[pv.Name] = *pv.DeepCopy()
	}
	return pvs, nil
}
