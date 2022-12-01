package preflight

import (
	"context"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/pointer"
)

func Test_validateVolumeAccessModes(t *testing.T) {
	for _, tt := range []struct {
		name            string
		dstSC           string
		podReadyTimeout time.Duration
		wantErr         bool
		resources       []runtime.Object
		input           map[string]corev1.PersistentVolumeClaim
		expected        map[string]map[string]pvcFailure
	}{
		{
			name: "With compatible access modes, expect no validation failures",
			input: map[string]corev1.PersistentVolumeClaim{
				"pvc0": {
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc0",
						Namespace: "default",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.String("default"),
						AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteMany"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Mi"),
							},
						},
					},
				},
				"pvc1": {
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc1",
						Namespace: "default",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.String("default"),
						AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteMany"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Mi"),
							},
						},
					},
				},
			},
			expected: make(map[string]map[string]pvcFailure),
			dstSC:    "dstSc",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "srcSc",
					},
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "dstSc",
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pvc",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv0",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "srcSc",
						ClaimRef: &corev1.ObjectReference{
							Name: "pvc",
						},
					},
				},
			},
		},
		{
			name:    "When destination storage class is not found, expect error",
			wantErr: true,
			dstSC:   "dstSc",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "srcSc",
					},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			kcli := fake.NewSimpleClientset(tt.resources...)
			logger := log.New(io.Discard, "", 0)
			result, err := validateVolumeAccessModes(context.Background(), logger, kcli, tt.dstSC, tt.podReadyTimeout, tt.input)
			if err != nil {
				if tt.wantErr {
					req.Error(err)
				} else {
					req.NoError(err)
				}
			}
			req.Equal(result, tt.expected)
		})
	}
}

func Test_getPvcError(t *testing.T) {
	for _, tt := range []struct {
		name      string
		wantErr   bool
		resources []runtime.Object
		input     *corev1.PersistentVolumeClaim
		expected  *pvcFailure
	}{
		{
			name: "When there is a PVC failure, expect ProvisioningFailed event",
			input: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc",
					Namespace: "default",
					UID:       "12345",
				},
			},
			expected: &pvcFailure{
				reason:  "ProvisioningFailed",
				from:    "kubernetes.io/no-provisioner",
				message: "Only support ReadWriteOnce access mode",
			},
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "srcSc",
					},
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "dstSc",
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc",
						Namespace: "default",
						UID:       "12345",
					},
					Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
				},
				&corev1.EventList{
					Items: []corev1.Event{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "pvc-error-event",
								Namespace: "default",
							},
							InvolvedObject: corev1.ObjectReference{
								Kind:      "PersistentVolumeClaim",
								Namespace: "default",
								Name:      "pvc",
								UID:       "12345",
							},
							Source:  corev1.EventSource{Component: "kubernetes.io/no-provisioner"},
							Reason:  "ProvisioningFailed",
							Message: "Only support ReadWriteOnce access mode",
						},
					},
				},
			},
		},
		{
			name: "When PVC event failure reason is not ProvisioningFailed, expect error",
			input: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc",
					Namespace: "default",
					UID:       "12345",
				},
			},
			expected: &pvcFailure{},
			wantErr:  true,
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "srcSc",
					},
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "dstSc",
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc",
						Namespace: "default",
						UID:       "12345",
					},
					Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
				},
				&corev1.EventList{
					Items: []corev1.Event{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "pvc-error-event",
								Namespace: "default",
							},
							InvolvedObject: corev1.ObjectReference{
								Kind:      "PersistentVolumeClaim",
								Namespace: "default",
								Name:      "pvc",
								UID:       "12345",
							},
							Source:  corev1.EventSource{Component: "kubernetes.io/no-provisioner"},
							Reason:  "Provisioning",
							Message: "External provisioner is provisiong volume for claim pvc",
						},
					},
				},
			},
		},
		{
			name: "When PVC is pending due to a failure but there are no events for it, expect error",
			input: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc",
					Namespace: "default",
					UID:       "12345",
				},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
			},
			expected: &pvcFailure{},
			wantErr:  true,
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "srcSc",
					},
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "dstSc",
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc",
						Namespace: "default",
						UID:       "12345",
					},
					Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
				},
			},
		},
		{
			name: "When PVC is not in Pending status, expect error",
			input: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc",
					Namespace: "default",
					UID:       "12345",
				},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
			},
			expected:  &pvcFailure{},
			wantErr:   true,
			resources: []runtime.Object{},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			kcli := fake.NewSimpleClientset(tt.resources...)
			result, err := getPvcError(kcli, tt.input)
			if err != nil {
				if tt.wantErr {
					req.Error(err)
				} else {
					req.NoError(err)
				}
			}
			req.Equal(tt.expected, result)
		})
	}
}

func Test_checkVolumeAccessModes(t *testing.T) {
	for _, tt := range []struct {
		name            string
		srcStorageClass string
		dstStorageClass string
		deletePVTimeout time.Duration
		podTimeout      time.Duration
		wantErr         bool
		resources       []runtime.Object
		input           *corev1.PersistentVolumeClaim
		expected        pvcFailure
	}{
		{
			name: "When the PVC access mode is not supported by destination storage provider, expect PVCError",
			input: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc",
					Namespace: "default",
					UID:       "12345",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: pointer.String("srcSc"),
					AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteMany"},
				},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
			},
			expected: pvcFailure{
				reason:  "ProvisioningFailed",
				from:    "kubernetes.io/no-provisioner",
				message: "Only support ReadWriteOnce access mode",
			},
			srcStorageClass: "srcSc",
			dstStorageClass: "dstSc",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "srcSc",
					},
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "dstSc",
					},
				},
				&corev1.EventList{
					Items: []corev1.Event{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "pvc-error-event",
								Namespace: "default",
							},
							InvolvedObject: corev1.ObjectReference{
								Kind:      "PersistentVolumeClaim",
								Namespace: "default",
								UID:       "12345",
							},
							Source:  corev1.EventSource{Component: "kubernetes.io/no-provisioner"},
							Reason:  "ProvisioningFailed",
							Message: "Only support ReadWriteOnce access mode",
						},
					},
				},
				&corev1.Pod{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "tmpPod",
						Namespace: "default",
					},
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers: []corev1.Container{
							{
								Name:  "busybox",
								Image: "busybox",
								Command: []string{
									"sleep",
									"3600",
								},
							},
						},
					},
					Status: corev1.PodStatus{Phase: corev1.PodPending},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			kcli := fake.NewSimpleClientset(tt.resources...)
			logger := log.New(io.Discard, "", 0)
			result, err := checkVolumeAccessModes(context.Background(), logger, kcli, tt.dstStorageClass, *tt.input, tt.podTimeout)
			if err != nil {
				if tt.wantErr {
					req.Error(err)
				} else {
					req.NoError(err)
				}
			}
			req.Equal(tt.expected, result)
		})
	}
}

func Test_buildTmpPVC(t *testing.T) {
	for _, tt := range []struct {
		name            string
		pvcNameOverride string
		dstStorageClass string
		input           *corev1.PersistentVolumeClaim
		expectedPVC     *corev1.PersistentVolumeClaim
		expectedName    string
	}{
		{
			name: "When PVC name is not overridden, expect unique temp pvc name",
			input: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "default",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: pointer.String("dstSc"),
					AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteOnce"},
				},
			},
			expectedPVC: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: pointer.String("dstSc"),
					AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteOnce"},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Mi"),
						},
					},
				},
			},
			expectedName:    "pvmigrate-preflight-test-pvc",
			dstStorageClass: "dstSc",
		},
		{
			name: "When PVC name is longer than 63 chars, expect name to be trimmed",
			input: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "really-long-pvc-name-that-should-be-trimmed-to-avoid-an-error",
					Namespace: "default",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: pointer.String("dstSc"),
					AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteOnce"},
				},
			},
			expectedPVC: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: pointer.String("dstSc"),
					AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteOnce"},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Mi"),
						},
					},
				},
			},
			expectedName:    "pvmigrate-claim-really-long-pvc-trimmed-to-avoid-an-error-",
			dstStorageClass: "dstSc",
		},
		{
			name: "When PVC name is overriden, expect non-UID generated name",
			input: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "my-test-pvc",
					Namespace: "default",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: pointer.String("dstSc"),
					AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteOnce"},
				},
			},
			expectedPVC: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: pointer.String("dstSc"),
					AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteOnce"},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Mi"),
						},
					},
				},
			},
			pvcNameOverride: "pvc-name-override",
			expectedName:    "pvc-name-override",
			dstStorageClass: "dstSc",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			pvc := buildTmpPVC(*tt.input, tt.dstStorageClass)
			req.True(strings.HasPrefix(pvc.Name, tt.expectedName))
			req.Equal(tt.expectedPVC.Spec, pvc.Spec)
		})
	}
}

func Test_buildPVCConsumerPod(t *testing.T) {
	for _, tt := range []struct {
		name            string
		namespace       string
		podNameOverride string
		pvcName         string
		expectedPod     *corev1.Pod
		expectedName    string
	}{
		{
			name:    "When pod name not overriden, expect unique pod name",
			pvcName: "test-pvc",
			expectedPod: &corev1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Volumes: []corev1.Volume{
						{
							Name: "tmp",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "test-pvc",
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
			},
			expectedName: "pvmigrate-vol-consumer-test-pvc-",
		},
		{
			name:    "When pod name is longer than 63 chars, expect pod name to be trimmed",
			pvcName: "pvc-name-that-should-be-trimmed-because-it-will-cause-an-err",
			expectedPod: &corev1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Volumes: []corev1.Volume{
						{
							Name: "tmp",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "pvc-name-that-should-be-trimmed-because-it-will-cause-an-err",
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
			},
			expectedName: "pvmigrate-vol-consumer-pvc-namecause-it-will-cause-an-err-",
		},
		{
			name:    "When pod name is overriden, expect non-UID name",
			pvcName: "test-pvc",
			expectedPod: &corev1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Volumes: []corev1.Volume{
						{
							Name: "tmp",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "test-pvc",
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
			},
			podNameOverride: "my pod name override",
			expectedName:    "my pod name override",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			pod := buildPVCConsumerPod(tt.pvcName, tt.namespace)
			req.True(strings.HasPrefix(pod.Name, tt.expectedName))
			req.Equal(tt.expectedPod.Spec, pod.Spec)
		})
	}
}

func Test_pvcsForStorageClass(t *testing.T) {
	for _, tt := range []struct {
		name      string
		scname    string
		namespace string
		wantErr   bool
		resources []runtime.Object
		expected  map[string]corev1.PersistentVolumeClaim
	}{
		{
			name:      "When storage class is not found, expect error",
			scname:    "i-dont-exit",
			namespace: "default",
			wantErr:   true,
		},
		{
			name:      "When volumes and storage classes exist and namespace is set, expect pvcs for that particular namespace only",
			scname:    "default",
			namespace: "default",
			expected: map[string]corev1.PersistentVolumeClaim{
				"pvc0": {
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc0",
						Namespace: "default",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.String("default"),
						AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteOnce"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Mi"),
							},
						},
					},
				},
			},
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "default",
					},
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "rook",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv0",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "default",
						ClaimRef: &corev1.ObjectReference{
							Name:      "pvc0",
							Namespace: "default",
						},
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv1",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "rook",
						ClaimRef: &corev1.ObjectReference{
							Name:      "pvc1",
							Namespace: "test",
						},
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc0",
						Namespace: "default",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.String("default"),
						AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteOnce"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Mi"),
							},
						},
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc1",
						Namespace: "test",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.String("rook"),
						AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteMany"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Mi"),
							},
						},
					},
				},
			},
		},
		{
			name:      "When volumes and storage classes exist and namespace is NOT set, expect pvcs for all namespaces",
			scname:    "default",
			namespace: "",
			expected: map[string]corev1.PersistentVolumeClaim{
				"pvc0": {
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc0",
						Namespace: "default",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.String("default"),
						AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteOnce"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Mi"),
							},
						},
					},
				},
				"pvc1": {
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc1",
						Namespace: "test",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.String("rook"),
						AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteMany"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Mi"),
							},
						},
					},
				},
			},
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "default",
					},
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "rook",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv0",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "default",
						ClaimRef: &corev1.ObjectReference{
							Name:      "pvc0",
							Namespace: "default",
						},
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv1",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "rook",
						ClaimRef: &corev1.ObjectReference{
							Name:      "pvc1",
							Namespace: "test",
						},
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc0",
						Namespace: "default",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.String("default"),
						AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteOnce"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Mi"),
							},
						},
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc1",
						Namespace: "test",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.String("rook"),
						AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteMany"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Mi"),
							},
						},
					},
				},
			},
		},
		{
			name:      "When PV does not have an associated PVC, expect error",
			scname:    "default",
			namespace: "default",
			expected: map[string]corev1.PersistentVolumeClaim{
				"pvc0": {
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc0",
						Namespace: "default",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.String("default"),
						AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteOnce"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Mi"),
							},
						},
					},
				},
			},
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "default",
					},
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "rook",
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
						StorageClassName: "rook",
						ClaimRef: &corev1.ObjectReference{
							Name:      "pvc1",
							Namespace: "test",
						},
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc0",
						Namespace: "default",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.String("default"),
						AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteOnce"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Mi"),
							},
						},
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc1",
						Namespace: "test",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.String("rook"),
						AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteMany"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Mi"),
							},
						},
					},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			kcli := fake.NewSimpleClientset(tt.resources...)
			logger := log.New(io.Discard, "", 0)
			result, err := pvcsForStorageClass(context.Background(), logger, kcli, tt.scname, tt.namespace)
			if err != nil {
				if tt.wantErr {
					req.Error(err)
				} else {
					req.NoError(err)
				}
			}
			req.Equal(tt.expected, result)
		})
	}
}
