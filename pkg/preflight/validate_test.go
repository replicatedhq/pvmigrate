package preflight

import (
	"context"
	"io"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/pointer"
)

func Test_validateVolumeAccessModes(t *testing.T) {
	for _, tt := range []struct {
		name            string
		dstSC           string
		podReadyTimeout time.Duration
		deletePVTimeout time.Duration
		wantErr         bool
		resources       []runtime.Object
		input           map[string]corev1.PersistentVolumeClaim
		expected        map[string]map[string]pvcFailure
	}{
		{
			name:            "With compatible access modes, expect no validation failures",
			deletePVTimeout: time.Second,
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
			name:            "When destination storage class is not found, expect error",
			deletePVTimeout: time.Second,
			wantErr:         true,
			dstSC:           "dstSc",
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
			result, err := validateVolumeAccessModes(context.Background(), logger, kcli, tt.dstSC, "eeacms/rsync:2.3", tt.podReadyTimeout, tt.deletePVTimeout, tt.input)
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
			name: "When there is a PVC failure expect ProvisioningFailed event",
			input: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc",
					Namespace: "default",
					UID:       "12345",
				},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
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
			name: "When PVC event failure reason is not ProvisioningFailed expect error",
			input: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc",
					Namespace: "default",
					UID:       "12345",
				},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
			},
			expected: nil,
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
			name: "When PVC is pending due to a failure but there are no events for it expect error",
			input: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc",
					Namespace: "default",
					UID:       "12345",
				},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
			},
			expected: nil,
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
			name: "When PVC is not in Pending status expect error",
			input: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc",
					Namespace: "default",
					UID:       "12345",
				},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
			},
			expected:  nil,
			wantErr:   true,
			resources: []runtime.Object{},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			kcli := fake.NewSimpleClientset(tt.resources...)
			result, err := getPVCError(kcli, tt.input)
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
		expected        *pvcFailure
		tmpPodName      string
		backgroundFunc  func(context.Context, *log.Logger, k8sclient.Interface, string, string, string)
	}{
		{
			name:            "When the PVC access mode is not supported by destination storage provider expect PVC failure",
			deletePVTimeout: time.Second,
			input: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "testpvc",
					Namespace: "default",
					UID:       "12345",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: pointer.String("srcSc"),
					AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteMany"},
				},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
			},
			expected: &pvcFailure{
				reason:  "ProvisioningFailed",
				from:    "kubernetes.io/no-provisioner",
				message: "Only support ReadWriteOnce access mode",
			},
			srcStorageClass: "srcSc",
			dstStorageClass: "dstSc",
			tmpPodName:      podNamePrefix + "-pf-pvc-testpvc",
			// make the timeout for the function under test take a little longer so that that
			// backgroundFunc can update the pod phase to Pending
			podTimeout: 2 * time.Second,
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
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv-for-pf-pvc-testpvc",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "dstSc",
						ClaimRef: &corev1.ObjectReference{
							Name:      pvcNamePrefix + "-testpvc",
							Namespace: "default",
						},
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      pvcNamePrefix + "-testpvc",
						Namespace: "default",
						UID:       "12345",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.String("dstSc"),
						AccessModes:      []corev1.PersistentVolumeAccessMode{"ReadWriteMany"},
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
								UID:       "12345",
							},
							Source:  corev1.EventSource{Component: "kubernetes.io/no-provisioner"},
							Reason:  "ProvisioningFailed",
							Message: "Only support ReadWriteOnce access mode",
						},
					},
				},
			},
			backgroundFunc: func(ctx context.Context, logger *log.Logger, k k8sclient.Interface, tmpPod, ns, pv string) {
				for {
					pod, err := k.CoreV1().Pods(ns).Get(ctx, tmpPod, metav1.GetOptions{})
					if err != nil {
						continue
					}

					// update status of the pod to Pending
					pendingPod := pod.DeepCopy()
					pendingPod.Status = corev1.PodStatus{Phase: corev1.PodPending}
					if _, err = k.CoreV1().Pods(pendingPod.Namespace).Update(ctx, pendingPod, metav1.UpdateOptions{}); err != nil {
						logger.Printf("backgroundFunc: failed to update pod %s with status Pending", pendingPod.Name)
						return
					}

					// now wait for a bit until defer functions run
					// this needs to > tt.podTimeout
					time.Sleep(3 * time.Second)

					// delete PV in resources
					if err = k.CoreV1().PersistentVolumes().Delete(ctx, pv, metav1.DeleteOptions{}); err != nil {
						logger.Print("backgroundFunc: could not delete PV: ", pv)
					}
					break
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			testCtx, cancelfunc := context.WithTimeout(context.Background(), time.Minute) // if your test takes more than 1m, there are issues
			defer cancelfunc()
			kcli := fake.NewSimpleClientset(tt.resources...)
			logger := log.New(io.Discard, "", 0)
			if tt.backgroundFunc != nil {
				go tt.backgroundFunc(testCtx, logger, kcli, tt.tmpPodName, "default", "pv-for-pf-pvc-testpvc")
			}
			result, err := checkVolumeAccessModes(context.Background(), logger, kcli, tt.dstStorageClass, *tt.input, tt.podTimeout, tt.deletePVTimeout, "eeacms/rsync:2.3")
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
		dstStorageClass string
		input           *corev1.PersistentVolumeClaim
		expectedPVC     *corev1.PersistentVolumeClaim
	}{
		{
			name: "When PVC name is longer than 63 chars expect name to be trimmed",
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
					Name:      "pf-pvc-really-long-pvc-name-thauld-be-trimmed-to-avoid-an-error",
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
			dstStorageClass: "dstSc",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			pvc, err := buildTmpPVC(*tt.input, tt.dstStorageClass)
			if err != nil {
				req.NoError(err)
			}
			req.Equal(tt.expectedPVC, pvc)
		})
	}
}

func Test_buildPVCConsumerPod(t *testing.T) {
	for _, tt := range []struct {
		name        string
		namespace   string
		pvcName     string
		podImage    string
		expectedPod *corev1.Pod
	}{
		{
			name:      "When pod name is longer than 63 chars expect pod name to be trimmed",
			pvcName:   "pf-pvc-this-pvc-name-will-cause-the-temp-pod-name-to-be-trimmed",
			namespace: "default",
			podImage:  "eeacms/rsync:2.3",
			expectedPod: &corev1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvmigrate-pf-pod-pf-pvc-this-pv-the-temp-pod-name-to-be-trimmed",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Volumes: []corev1.Volume{
						{
							Name: "tmp",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "pf-pvc-this-pvc-name-will-cause-the-temp-pod-name-to-be-trimmed",
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "sleep",
							Image: "eeacms/rsync:2.3",
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
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			pod := buildTmpPVCConsumerPod(tt.pvcName, tt.namespace, tt.podImage)
			req.Equal(tt.expectedPod, pod)
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
			name:      "When storage class is not found expect error",
			scname:    "i-dont-exist",
			namespace: "default",
			wantErr:   true,
		},
		{
			name:      "When volumes and storage classes exist and namespace is set expect pvcs for that particular namespace only",
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
							Name:      "pvc2",
							Namespace: "test",
						},
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv2",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "default",
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
						Name:      "pvc2",
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
			name:      "When volumes and storage classes exist and namespace is NOT set expect pvcs for all namespaces",
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
						StorageClassName: "default",
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
		},
		{
			name:      "When PV does not have an associated PVC expect error",
			scname:    "default",
			namespace: "default",
			wantErr:   true,
			expected:  nil,
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

func Test_validateStorageClasses(t *testing.T) {
	for _, tt := range []struct {
		name                   string
		resources              []runtime.Object
		sourceSC               string
		destSC                 string
		wantErr                bool
		expected               []ValidationFailure
		skipSourceSCValidation bool
	}{
		{
			name:     "When both StorageClasses exist and are distinct expect no failures",
			sourceSC: "sourcesc",
			destSC:   "destsc",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "sourcesc",
					},
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "destsc",
					},
				},
			},
		},
		{
			name:     "When source storage class does not exist expect validation failure",
			sourceSC: "sourcesc",
			destSC:   "destsc",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "destsc",
					},
				},
			},
			expected: []ValidationFailure{
				{
					Resource: "sc/sourcesc",
					Message:  "Resource not found",
				},
			},
		},
		{
			name:     "When destination storage class does not exist expect validation failure",
			sourceSC: "sourcesc",
			destSC:   "destsc",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "sourcesc",
					},
				},
			},
			expected: []ValidationFailure{
				{
					Resource: "sc/destsc",
					Message:  "Resource not found",
				},
			},
		},
		{
			name:                   "When source storage class does not exist and skip storage class validation is enabled expect no failure",
			sourceSC:               "sourcesc",
			destSC:                 "destsc",
			skipSourceSCValidation: true,
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "destsc",
					},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			clientset := fake.NewSimpleClientset(tt.resources...)
			logger := log.New(io.Discard, "", 0)
			result, err := validateStorageClasses(context.Background(), logger, clientset, tt.sourceSC, tt.destSC, tt.skipSourceSCValidation)
			if !tt.wantErr {
				req.NoError(err)
			} else {
				req.Error(err)
			}
			req.Equal(tt.expected, result)
		})
	}
}

func Test_deleteTmpPVCs(t *testing.T) {
	for _, tt := range []struct {
		name           string
		resources      []runtime.Object
		timeout        time.Duration
		pvc            *corev1.PersistentVolumeClaim
		wantErr        bool
		backgroundFunc func(*testing.T, k8sclient.Interface)
	}{
		{
			name:    "When deleting non existing pvc expect success",
			timeout: time.Second,
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "i-do-not-exist",
					Namespace: "default",
				},
			},
		},
		{
			name:    "When a pv has nil claim ref expect success",
			timeout: time.Second,
			resources: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "default",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-pv",
					},
				},
			},
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc",
					Namespace: "default",
				},
			},
		},
		{
			name:    "When a pv has a claim ref to a different pvc expect success",
			timeout: time.Second,
			resources: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "default",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv",
					},
					Spec: corev1.PersistentVolumeSpec{
						ClaimRef: &corev1.ObjectReference{
							Name:      "abc",
							Namespace: "default",
						},
					},
				},
			},
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "default",
				},
			},
		},
		{
			name:    "When a pv takes while to be purged expect suceess",
			timeout: 20 * time.Second,
			resources: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc",
						Namespace: "default",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv",
					},
					Spec: corev1.PersistentVolumeSpec{
						ClaimRef: &corev1.ObjectReference{
							Name:      "pvc",
							Namespace: "default",
						},
					},
				},
			},
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc",
					Namespace: "default",
				},
			},
			backgroundFunc: func(t *testing.T, kcli k8sclient.Interface) {
				time.Sleep(6 * time.Second)
				if err := kcli.CoreV1().PersistentVolumes().Delete(
					context.Background(), "pv", metav1.DeleteOptions{},
				); err != nil {
					t.Errorf("failed to delete test pv: %s", err)
				}
			},
		},
		{
			name:    "When a pv is not purged expect error (timeout)",
			timeout: 10 * time.Second,
			wantErr: true,
			resources: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc",
						Namespace: "default",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv",
					},
					Spec: corev1.PersistentVolumeSpec{
						ClaimRef: &corev1.ObjectReference{
							Name:      "pvc",
							Namespace: "default",
						},
					},
				},
			},
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc",
					Namespace: "default",
				},
			},
		},
		{
			name:    "When pv has a claim ref to a pvc from a different namespaces expect success",
			timeout: 20 * time.Second,
			resources: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc",
						Namespace: "default",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv",
					},
					Spec: corev1.PersistentVolumeSpec{
						ClaimRef: &corev1.ObjectReference{
							Name:      "pvc",
							Namespace: "default",
						},
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "another-pv",
					},
					Spec: corev1.PersistentVolumeSpec{
						ClaimRef: &corev1.ObjectReference{
							Name:      "pvc",
							Namespace: "different-namespace",
						},
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "yet-another-pv",
					},
					Spec: corev1.PersistentVolumeSpec{
						ClaimRef: &corev1.ObjectReference{
							Name:      "pvc",
							Namespace: "yet-another-different-namespace",
						},
					},
				},
			},
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc",
					Namespace: "default",
				},
			},
			backgroundFunc: func(t *testing.T, kcli k8sclient.Interface) {
				time.Sleep(6 * time.Second)
				if err := kcli.CoreV1().PersistentVolumes().Delete(
					context.Background(), "pv", metav1.DeleteOptions{},
				); err != nil {
					t.Errorf("failed to delete test pv: %s", err)
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			clientset := fake.NewSimpleClientset(tt.resources...)
			logger := log.New(io.Discard, "", 0)

			if tt.backgroundFunc != nil {
				go tt.backgroundFunc(t, clientset)
			}

			err := deleteTmpPVC(logger, clientset, tt.pvc, tt.timeout)
			if err != nil {
				if tt.wantErr {
					req.Error(err)
					return
				}
			}

			if tt.wantErr {
				req.Fail("Expecting error but received nil instead")
			}
		})
	}
}
