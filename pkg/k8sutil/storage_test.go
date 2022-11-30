package k8sutil

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPVCSForPVs(t *testing.T) {
	for _, tt := range []struct {
		name     string
		err      string
		input    map[string]corev1.PersistentVolume
		expected map[string]corev1.PersistentVolumeClaim
		objs     []runtime.Object
	}{
		{
			name: "should fail if pvc does not havel a claimref",
			err:  "pv pv0 without associated PVC",
			input: map[string]corev1.PersistentVolume{
				"pv0": {
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv0",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "default",
					},
				},
			},
		},
		{
			name: "should fail if pvc is not found",
			err:  "failed to get pvc do-not-exist for pv pv0",
			input: map[string]corev1.PersistentVolume{
				"pv0": {
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv0",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "default",
						ClaimRef: &corev1.ObjectReference{
							Name: "do-not-exist",
						},
					},
				},
			},
		},
		{
			name: "should be able to find space in detached pvc",
			input: map[string]corev1.PersistentVolume{
				"pv0": {
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv0",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "default",
						ClaimRef: &corev1.ObjectReference{
							Name: "pvc",
						},
					},
				},
			},
			objs: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pvc",
					},
				},
			},
			expected: map[string]corev1.PersistentVolumeClaim{
				"pv0": {
					ObjectMeta: metav1.ObjectMeta{
						Name: "pvc",
					},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			kcli := fake.NewSimpleClientset(tt.objs...)
			result, err := PVCSForPVs(context.Background(), kcli, tt.input)
			if err != nil {
				if len(tt.err) == 0 {
					t.Errorf("unexpected error: %s", err)
				} else if !strings.Contains(err.Error(), tt.err) {
					t.Errorf("expecting %q, %q received instead", tt.err, err)
				}
				return
			}

			if len(tt.err) > 0 {
				t.Errorf("expecting error %q, nil received instead", tt.err)
			}

			if diff := cmp.Diff(tt.expected, result); diff != "" {
				t.Errorf("unexpected return: %s", diff)
			}
		})
	}
}

func TestPVSByStorageClass(t *testing.T) {
	for _, tt := range []struct {
		name     string
		err      string
		scname   string
		expected map[string]corev1.PersistentVolume
		objs     []runtime.Object
	}{
		{
			name:   "should fail if storage class was not found",
			scname: "not-found",
			err:    "failed to get storage class",
		},
		{
			name:   "should pass when multiple volumes are present",
			scname: "default",
			expected: map[string]corev1.PersistentVolume{
				"pv0": {
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv0",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "default",
					},
				},
				"pv1": {
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv1",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "default",
					},
				},
			},
			objs: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "default",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv0",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "default",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv1",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "default",
					},
				},
			},
		},
		{
			name:   "should pass when multiple volumes of different classes are present",
			scname: "default",
			expected: map[string]corev1.PersistentVolume{
				"pv0": {
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv0",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "default",
					},
				},
				"pv1": {
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv1",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "default",
					},
				},
			},
			objs: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "default",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv0",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "default",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv1",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "default",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv2",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "another",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv3",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "yet-other-class",
					},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			kcli := fake.NewSimpleClientset(tt.objs...)
			result, err := PVSByStorageClass(context.Background(), kcli, tt.scname)
			if err != nil {
				if len(tt.err) == 0 {
					t.Errorf("unexpected error: %s", err)
				} else if !strings.Contains(err.Error(), tt.err) {
					t.Errorf("expecting %q, %q received instead", tt.err, err)
				}
				return
			}

			if len(tt.err) > 0 {
				t.Errorf("expecting error %q, nil received instead", tt.err)
			}

			if diff := cmp.Diff(tt.expected, result); diff != "" {
				t.Errorf("unexpected return: %s", diff)
			}
		})
	}
}
