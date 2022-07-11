package migrate

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/pointer"
)

type testWriter struct {
	t *testing.T
}

func (tw testWriter) Write(p []byte) (n int, err error) {
	tw.t.Log(string(p))
	return len(p), nil
}

func TestScaleUpPods(t *testing.T) {
	zeroInt := int32(0)
	tests := []struct {
		name       string
		resources  []runtime.Object
		namespaces []string
		wantErr    bool
		validate   func(clientset k8sclient.Interface, t *testing.T) error
	}{
		{
			name:       "two namespaces, only scale one (statefulsets)",
			namespaces: []string{"ns1"},
			resources: []runtime.Object{
				&appsv1.StatefulSet{ // should be scaled by our code
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ns1",
						Namespace: "ns1",
						Annotations: map[string]string{
							scaleAnnotation: "2",
						},
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: &zeroInt,
					},
				},
				&appsv1.StatefulSet{ // should not be scaled by our code (wrong ns)
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ns2",
						Namespace: "ns2",
						Annotations: map[string]string{
							scaleAnnotation: "2",
						},
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: &zeroInt,
					},
				},
				&appsv1.StatefulSet{ // should not be scaled by our code (no annotation)
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ns1-no",
						Namespace: "ns1",
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: &zeroInt,
					},
				},
			},
			validate: func(clientset k8sclient.Interface, t *testing.T) error { // check that the statefulsets have the expected number of replicas and annotations
				ns1Set, err := clientset.AppsV1().StatefulSets("ns1").Get(context.TODO(), "ns1", metav1.GetOptions{})
				if err != nil {
					return err
				}
				if _, ok := ns1Set.ObjectMeta.Annotations[scaleAnnotation]; ok {
					// this annotation should have been deleted
					return fmt.Errorf("ss ns1 in ns1 still had annotation %s", scaleAnnotation)
				}
				if ns1Set.Spec.Replicas == nil || *ns1Set.Spec.Replicas != int32(2) {
					return fmt.Errorf("ss ns1 in ns1 still had wrong scale %d", *ns1Set.Spec.Replicas)
				}

				ns2Set, err := clientset.AppsV1().StatefulSets("ns2").Get(context.TODO(), "ns2", metav1.GetOptions{})
				if err != nil {
					return err
				}
				if _, ok := ns2Set.ObjectMeta.Annotations[scaleAnnotation]; !ok {
					// this annotation should not have been deleted
					return fmt.Errorf("ss ns2 in ns2 did not have annotation %s", scaleAnnotation)
				}
				if ns2Set.Spec.Replicas == nil || *ns2Set.Spec.Replicas != int32(0) {
					return fmt.Errorf("ss ns2 in ns2 had updated scale %d", *ns2Set.Spec.Replicas)
				}

				ns1noSet, err := clientset.AppsV1().StatefulSets("ns1").Get(context.TODO(), "ns1-no", metav1.GetOptions{})
				if err != nil {
					return err
				}
				if ns1noSet.Spec.Replicas == nil || *ns1noSet.Spec.Replicas != int32(0) {
					return fmt.Errorf("ss ns1-no in ns1 had updated scale %d", *ns1noSet.Spec.Replicas)
				}

				return nil
			},
		},

		{
			name:       "two namespaces, only scale one (deployments)",
			namespaces: []string{"ns1"},
			resources: []runtime.Object{
				&appsv1.Deployment{ // should be scaled by our code
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ns1",
						Namespace: "ns1",
						Annotations: map[string]string{
							scaleAnnotation: "2",
						},
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: &zeroInt,
					},
				},
				&appsv1.Deployment{ // should not be scaled by our code (wrong ns)
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ns2",
						Namespace: "ns2",
						Annotations: map[string]string{
							scaleAnnotation: "2",
						},
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: &zeroInt,
					},
				},
				&appsv1.Deployment{ // should not be scaled by our code (no annotation)
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ns1-no",
						Namespace: "ns1",
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: &zeroInt,
					},
				},
			},
			validate: func(clientset k8sclient.Interface, t *testing.T) error { // check that the deployments have the expected number of replicas and annotations
				ns1Set, err := clientset.AppsV1().Deployments("ns1").Get(context.TODO(), "ns1", metav1.GetOptions{})
				if err != nil {
					return err
				}
				if _, ok := ns1Set.ObjectMeta.Annotations[scaleAnnotation]; ok {
					// this annotation should have been deleted
					return fmt.Errorf("ss ns1 in ns1 still had annotation %s", scaleAnnotation)
				}
				if ns1Set.Spec.Replicas == nil || *ns1Set.Spec.Replicas != int32(2) {
					return fmt.Errorf("ss ns1 in ns1 still had wrong scale %d", *ns1Set.Spec.Replicas)
				}

				ns2Set, err := clientset.AppsV1().Deployments("ns2").Get(context.TODO(), "ns2", metav1.GetOptions{})
				if err != nil {
					return err
				}
				if _, ok := ns2Set.ObjectMeta.Annotations[scaleAnnotation]; !ok {
					// this annotation should not have been deleted
					return fmt.Errorf("ss ns2 in ns2 did not have annotation %s", scaleAnnotation)
				}
				if ns2Set.Spec.Replicas == nil || *ns2Set.Spec.Replicas != int32(0) {
					return fmt.Errorf("ss ns2 in ns2 had updated scale %d", *ns2Set.Spec.Replicas)
				}

				ns1noSet, err := clientset.AppsV1().Deployments("ns1").Get(context.TODO(), "ns1-no", metav1.GetOptions{})
				if err != nil {
					return err
				}
				if ns1noSet.Spec.Replicas == nil || *ns1noSet.Spec.Replicas != int32(0) {
					return fmt.Errorf("ss ns1-no in ns1 had updated scale %d", *ns1noSet.Spec.Replicas)
				}

				return nil
			},
		},
		{
			name:       "example test",
			resources:  []runtime.Object{},
			namespaces: []string{},
			validate: func(clientset k8sclient.Interface, t *testing.T) error {
				return nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := require.New(t)
			clientset := fake.NewSimpleClientset(test.resources...)
			testlog := log.New(testWriter{t: t}, "", 0)
			err := scaleUpPods(context.Background(), testlog, clientset, test.namespaces)
			req.NoError(err)

			err = test.validate(clientset, t)
			req.NoError(err)
		})
	}
}

func TestMutatePV(t *testing.T) {
	tests := []struct {
		name      string
		resources []runtime.Object
		pvname    string
		wantErr   bool
		ttmutator func(volume *corev1.PersistentVolume) *corev1.PersistentVolume
		ttchecker func(volume *corev1.PersistentVolume) bool
		validate  func(clientset k8sclient.Interface, t *testing.T) error
	}{
		{
			name:   "mutate PV",
			pvname: "pv1",
			resources: []runtime.Object{
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "pv1",
						Namespace:   "",
						Annotations: map[string]string{},
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRecycle,
					},
				},
			},
			ttmutator: func(volume *corev1.PersistentVolume) *corev1.PersistentVolume {
				volume.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
				return volume
			},
			ttchecker: func(volume *corev1.PersistentVolume) bool {
				return volume.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain
			},
			validate: func(clientset k8sclient.Interface, t *testing.T) error {
				pv, err := clientset.CoreV1().PersistentVolumes().Get(context.TODO(), "pv1", metav1.GetOptions{})
				if err != nil {
					return err
				}
				if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
					return fmt.Errorf("reclaim policy was %q not retain", pv.Spec.PersistentVolumeReclaimPolicy)
				}
				return nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := require.New(t)
			clientset := fake.NewSimpleClientset(test.resources...)
			testlog := log.New(testWriter{t: t}, "", 0)
			err := mutatePV(context.Background(), testlog, clientset, test.pvname, test.ttmutator, test.ttchecker)
			req.NoError(err)

			err = test.validate(clientset, t)
			req.NoError(err)
		})
	}
}

func TestValidateStorageClasses(t *testing.T) {
	tests := []struct {
		name                 string
		resources            []runtime.Object
		sourceSC             string
		destSC               string
		skipSourceValidation bool
		wantErr              bool
	}{
		{
			name:     "both StorageClasses exist and are distinct",
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
			name:     "source does not exist",
			sourceSC: "sourcesc",
			destSC:   "destsc",
			wantErr:  true,
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "destsc",
					},
				},
			},
		},
		{
			name:     "dest does not exist",
			sourceSC: "sourcesc",
			destSC:   "destsc",
			wantErr:  true,
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "sourcesc",
					},
				},
			},
		},
		{
			name:                 "source does not exist, but skipSourceValidation is set",
			sourceSC:             "sourcesc",
			destSC:               "destsc",
			wantErr:              false,
			skipSourceValidation: true,
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "destsc",
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := require.New(t)
			clientset := fake.NewSimpleClientset(test.resources...)
			testlog := log.New(testWriter{t: t}, "", 0)
			err := validateStorageClasses(context.Background(), testlog, clientset, test.sourceSC, test.destSC, test.skipSourceValidation)
			if !test.wantErr {
				req.NoError(err)
			} else {
				req.Error(err)
			}

		})
	}
}

func TestGetPVCs(t *testing.T) {
	dscString := "dsc"
	tests := []struct {
		name         string
		resources    []runtime.Object
		sourceScName string
		destScName   string
		namespace    string
		wantErr      bool
		originalPVCs map[string][]corev1.PersistentVolumeClaim
		namespaces   []string
		validate     func(clientset k8sclient.Interface, t *testing.T)
	}{
		{
			name:         "one PV, no PVC",
			sourceScName: "sc1",
			destScName:   "dsc",
			wantErr:      true,
			resources: []runtime.Object{
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv1",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "sc1",
					},
				},
			},
			validate: func(clientset k8sclient.Interface, t *testing.T) {
				return
			},
		},
		{
			name:         "one PV, one PVC",
			sourceScName: "sc1",
			destScName:   "dsc",
			wantErr:      false,
			resources: []runtime.Object{
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv1",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "sc1",
						Capacity: map[corev1.ResourceName]resource.Quantity{
							"storage": resource.MustParse("1Gi"),
						},
						ClaimRef: &corev1.ObjectReference{
							Kind:       "PersistentVolumeClaim",
							Namespace:  "ns1",
							Name:       "pvc1",
							APIVersion: "v1",
						},
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc1",
						Namespace: "ns1",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName: "pv1",
					},
				},
			},
			validate: func(clientset k8sclient.Interface, t *testing.T) {
				pvc, err := clientset.CoreV1().PersistentVolumeClaims("ns1").Get(context.TODO(), "pvc1-pvcmigrate", metav1.GetOptions{})
				require.NoError(t, err)
				require.Equalf(t, dscString, *pvc.Spec.StorageClassName, "storage class name was %q not dsc", *pvc.Spec.StorageClassName)
				require.Equalf(t, "1Gi", pvc.Spec.Resources.Requests.Storage().String(), "PVC size was %q not 1Gi", pvc.Spec.Resources.Requests.Storage().String())
			},
			originalPVCs: map[string][]corev1.PersistentVolumeClaim{
				"ns1": {
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pvc1",
							Namespace: "ns1",
						},
						Spec: corev1.PersistentVolumeClaimSpec{
							VolumeName: "pv1",
						},
					},
				},
			},
			namespaces: []string{"ns1"},
		},
		{
			name:         "different sc PV",
			sourceScName: "sc1",
			destScName:   "dsc",
			wantErr:      false,
			resources: []runtime.Object{
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv1",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "sc2",
					},
				},
			},
			validate: func(clientset k8sclient.Interface, t *testing.T) {
				return
			},
			originalPVCs: map[string][]corev1.PersistentVolumeClaim{},
			namespaces:   []string{},
		},

		{
			name:         "one PV, one PVC - migration in progress",
			sourceScName: "sc1",
			destScName:   "dsc",
			wantErr:      false,
			resources: []runtime.Object{
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv1",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "sc1",
						Capacity: map[corev1.ResourceName]resource.Quantity{
							"storage": resource.MustParse("1Gi"),
						},
						ClaimRef: &corev1.ObjectReference{
							Kind:       "PersistentVolumeClaim",
							Namespace:  "ns1",
							Name:       "pvc1",
							APIVersion: "v1",
						},
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc1",
						Namespace: "ns1",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName: "pv1",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv2",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "dsc",
						Capacity: map[corev1.ResourceName]resource.Quantity{
							"storage": resource.MustParse("1Gi"),
						},
						ClaimRef: &corev1.ObjectReference{
							Kind:       "PersistentVolumeClaim",
							Namespace:  "ns1",
							Name:       "pvc1-pvcmigrate",
							APIVersion: "v1",
						},
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc1-pvcmigrate",
						Namespace: "ns1",
						Labels: map[string]string{
							"test": "retained",
						},
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName: "pv2",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
						StorageClassName: &dscString,
						AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					},
				},
			},
			validate: func(clientset k8sclient.Interface, t *testing.T) {
				pvc, err := clientset.CoreV1().PersistentVolumeClaims("ns1").Get(context.TODO(), "pvc1-pvcmigrate", metav1.GetOptions{})
				require.NoError(t, err)
				require.Equalf(t, "retained", pvc.Labels["test"], "PVC was recreated instead of retained")
			},
			originalPVCs: map[string][]corev1.PersistentVolumeClaim{
				"ns1": {
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pvc1",
							Namespace: "ns1",
						},
						Spec: corev1.PersistentVolumeClaimSpec{
							VolumeName: "pv1",
						},
					},
				},
			},
			namespaces: []string{"ns1"},
		},

		{
			name:         "one PV, one PVC - migration in progress, wrong storage",
			sourceScName: "sc1",
			destScName:   "dsc",
			wantErr:      true,
			resources: []runtime.Object{
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv1",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "sc1",
						Capacity: map[corev1.ResourceName]resource.Quantity{
							"storage": resource.MustParse("1Gi"),
						},
						ClaimRef: &corev1.ObjectReference{
							Kind:       "PersistentVolumeClaim",
							Namespace:  "ns1",
							Name:       "pvc1",
							APIVersion: "v1",
						},
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc1",
						Namespace: "ns1",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName: "pv1",
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv2",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "dsc",
						Capacity: map[corev1.ResourceName]resource.Quantity{
							"storage": resource.MustParse("2Gi"),
						},
						ClaimRef: &corev1.ObjectReference{
							Kind:       "PersistentVolumeClaim",
							Namespace:  "ns1",
							Name:       "pvc1-pvcmigrate",
							APIVersion: "v1",
						},
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc1-pvcmigrate",
						Namespace: "ns1",
						Labels: map[string]string{
							"test": "retained",
						},
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName: "pv2",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("2Gi"),
							},
						},
						StorageClassName: &dscString,
						AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					},
				},
			},
		},

		{
			name:         "example test",
			resources:    []runtime.Object{},
			sourceScName: "",
			destScName:   "",
			originalPVCs: map[string][]corev1.PersistentVolumeClaim{},
			namespaces:   []string{},
			validate: func(clientset k8sclient.Interface, t *testing.T) {
				return
			},
		},
		{
			name:         "one PV, one PVC, interesting accessmode",
			sourceScName: "sc1",
			destScName:   "dsc",
			wantErr:      false,
			resources: []runtime.Object{
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv1",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "sc1",
						Capacity: map[corev1.ResourceName]resource.Quantity{
							"storage": resource.MustParse("1Gi"),
						},
						ClaimRef: &corev1.ObjectReference{
							Kind:       "PersistentVolumeClaim",
							Namespace:  "ns1",
							Name:       "pvc1",
							APIVersion: "v1",
						},
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteMany,
							corev1.ReadWriteOnce,
						},
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc1",
						Namespace: "ns1",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName: "pv1",
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteMany,
							corev1.ReadWriteOnce,
						},
					},
				},
			},
			validate: func(clientset k8sclient.Interface, t *testing.T) {
				pvc, err := clientset.CoreV1().PersistentVolumeClaims("ns1").Get(context.TODO(), "pvc1-pvcmigrate", metav1.GetOptions{})
				require.NoError(t, err)
				require.Equalf(t, dscString, *pvc.Spec.StorageClassName, "storage class name was %q not dsc", *pvc.Spec.StorageClassName)
				require.Equalf(t, "1Gi", pvc.Spec.Resources.Requests.Storage().String(), "PVC size was %q not 1Gi", pvc.Spec.Resources.Requests.Storage().String())

				require.Equal(t, []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteMany,
					corev1.ReadWriteOnce,
				}, pvc.Spec.AccessModes)
			},
			originalPVCs: map[string][]corev1.PersistentVolumeClaim{
				"ns1": {
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pvc1",
							Namespace: "ns1",
						},
						Spec: corev1.PersistentVolumeClaimSpec{
							VolumeName: "pv1",
							AccessModes: []corev1.PersistentVolumeAccessMode{
								corev1.ReadWriteMany,
								corev1.ReadWriteOnce,
							},
						},
					},
				},
			},
			namespaces: []string{"ns1"},
		},
		{
			name:         "two PVs, two PVCs",
			sourceScName: "sc1",
			destScName:   "dsc",
			wantErr:      false,
			resources: []runtime.Object{
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv1",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "sc1",
						Capacity: map[corev1.ResourceName]resource.Quantity{
							"storage": resource.MustParse("1Gi"),
						},
						ClaimRef: &corev1.ObjectReference{
							Kind:       "PersistentVolumeClaim",
							Namespace:  "ns1",
							Name:       "pvc1",
							APIVersion: "v1",
						},
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv2",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "sc1",
						Capacity: map[corev1.ResourceName]resource.Quantity{
							"storage": resource.MustParse("1Gi"),
						},
						ClaimRef: &corev1.ObjectReference{
							Kind:       "PersistentVolumeClaim",
							Namespace:  "ns1",
							Name:       "pvc2",
							APIVersion: "v1",
						},
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc1",
						Namespace: "ns1",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName: "pv1",
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc2",
						Namespace: "ns1",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName: "pv2",
					},
				},
			},
			validate: func(clientset k8sclient.Interface, t *testing.T) {
				pvc1, err := clientset.CoreV1().PersistentVolumeClaims("ns1").Get(context.TODO(), "pvc1-pvcmigrate", metav1.GetOptions{})
				require.NoError(t, err)
				require.Equalf(t, dscString, *pvc1.Spec.StorageClassName, "storage class name was %q not dsc", *pvc1.Spec.StorageClassName)
				require.Equalf(t, "1Gi", pvc1.Spec.Resources.Requests.Storage().String(), "PVC size was %q not 1Gi", pvc1.Spec.Resources.Requests.Storage().String())

				pvc2, err := clientset.CoreV1().PersistentVolumeClaims("ns1").Get(context.TODO(), "pvc2-pvcmigrate", metav1.GetOptions{})
				require.NoError(t, err)
				require.Equalf(t, dscString, *pvc2.Spec.StorageClassName, "storage class name was %q not dsc", *pvc2.Spec.StorageClassName)
				require.Equalf(t, "1Gi", pvc2.Spec.Resources.Requests.Storage().String(), "PVC size was %q not 1Gi", pvc2.Spec.Resources.Requests.Storage().String())

			},
			originalPVCs: map[string][]corev1.PersistentVolumeClaim{
				"ns1": {
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pvc1",
							Namespace: "ns1",
						},
						Spec: corev1.PersistentVolumeClaimSpec{
							VolumeName: "pv1",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pvc2",
							Namespace: "ns1",
						},
						Spec: corev1.PersistentVolumeClaimSpec{
							VolumeName: "pv2",
						},
					},
				},
			},
			namespaces: []string{"ns1"},
		},
		{
			name:         "two PVs, two PVCs with limitation",
			sourceScName: "sc1",
			destScName:   "dsc",
			namespace:    "ns1",
			wantErr:      false,
			resources: []runtime.Object{
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv1",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "sc1",
						Capacity: map[corev1.ResourceName]resource.Quantity{
							"storage": resource.MustParse("1Gi"),
						},
						ClaimRef: &corev1.ObjectReference{
							Kind:       "PersistentVolumeClaim",
							Namespace:  "ns1",
							Name:       "pvc1",
							APIVersion: "v1",
						},
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
					},
				},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv2",
					},
					Spec: corev1.PersistentVolumeSpec{
						StorageClassName: "sc1",
						Capacity: map[corev1.ResourceName]resource.Quantity{
							"storage": resource.MustParse("1Gi"),
						},
						ClaimRef: &corev1.ObjectReference{
							Kind:       "PersistentVolumeClaim",
							Namespace:  "ns2",
							Name:       "pvc2",
							APIVersion: "v1",
						},
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc1",
						Namespace: "ns1",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName: "pv1",
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pvc2",
						Namespace: "ns2",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName:       "pv2",
						StorageClassName: pointer.String("sc1"),
					},
				},
			},
			validate: func(clientset k8sclient.Interface, t *testing.T) {
				pvc1, err := clientset.CoreV1().PersistentVolumeClaims("ns1").Get(context.TODO(), "pvc1-pvcmigrate", metav1.GetOptions{})
				require.NoError(t, err)
				require.Equalf(t, dscString, *pvc1.Spec.StorageClassName, "storage class name was %q not dsc", *pvc1.Spec.StorageClassName)
				require.Equalf(t, "1Gi", pvc1.Spec.Resources.Requests.Storage().String(), "PVC size was %q not 1Gi", pvc1.Spec.Resources.Requests.Storage().String())

				pvc2, err := clientset.CoreV1().PersistentVolumeClaims("ns2").Get(context.TODO(), "pvc2", metav1.GetOptions{})
				require.NoError(t, err)
				require.Equalf(t, "sc1", *pvc2.Spec.StorageClassName, "storage class name was %q not sc1", *pvc2.Spec.StorageClassName)

			},
			originalPVCs: map[string][]corev1.PersistentVolumeClaim{
				"ns1": {
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pvc1",
							Namespace: "ns1",
						},
						Spec: corev1.PersistentVolumeClaimSpec{
							VolumeName: "pv1",
						},
					},
				},
			},
			namespaces: []string{"ns1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := require.New(t)
			clientset := fake.NewSimpleClientset(test.resources...)
			testlog := log.New(testWriter{t: t}, "", 0)
			originalPVCs, nses, err := getPVCs(context.Background(), testlog, clientset, test.sourceScName, test.destScName, test.namespace)
			if !test.wantErr {
				req.NoError(err)
			} else {
				req.Error(err)
				return
			}

			test.validate(clientset, t)

			req.Equal(test.originalPVCs, originalPVCs)
			req.Equal(test.namespaces, nses)
		})
	}
}

func Test_createMigrationPod(t *testing.T) {
	type args struct {
		ns            string
		sourcePvcName string
		destPvcName   string
		rsyncImage    string
	}
	tests := []struct {
		name    string
		args    args
		want    *corev1.Pod
		wantErr bool
	}{
		{
			name: "basic",
			args: args{
				ns:            "testns",
				sourcePvcName: "sourcepvc",
				destPvcName:   "destpvc",
				rsyncImage:    "imagename",
			},
			want: &corev1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "migrate-sourcepvc",
					Namespace: "testns",
					Labels: map[string]string{
						baseAnnotation: "sourcepvc",
					},
				},
				Spec: corev1.PodSpec{

					RestartPolicy: corev1.RestartPolicyNever,
					Volumes: []corev1.Volume{
						{
							Name: "source",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "sourcepvc",
								},
							},
						},
						{
							Name: "dest",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "destpvc",
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "pvmigrate",
							Image: "imagename",
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
				Status: corev1.PodStatus{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			clientset := fake.NewSimpleClientset()
			got, err := createMigrationPod(context.Background(), clientset, tt.args.ns, tt.args.sourcePvcName, tt.args.destPvcName, tt.args.rsyncImage)
			if tt.wantErr {
				req.Error(err)
				return
			}

			req.NoError(err)
			req.Equal(tt.want, got)
		})
	}
}

func Test_swapPVs(t *testing.T) {
	sourceScName := "sourceScName"
	destScName := "destScName"
	tests := []struct {
		name      string
		resources []runtime.Object
		wantPVs   []corev1.PersistentVolume
		wantPVCs  []corev1.PersistentVolumeClaim
		ns        string
		pvcName   string
		wantErr   bool
	}{
		{
			name:    "swap one PVC",
			ns:      "testns",
			pvcName: "sourcepvc",
			resources: []runtime.Object{
				// two PVCs
				&corev1.PersistentVolumeClaim{
					TypeMeta: metav1.TypeMeta{
						Kind:       "PersistentVolumeClaim",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sourcepvc",
						Namespace: "testns",
						Annotations: map[string]string{
							"testannotation": "sourcepvc",
						},
						Labels: map[string]string{
							"testlabel": "sourcepvc",
						},
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteMany,
						},
						Resources: corev1.ResourceRequirements{
							Requests: map[corev1.ResourceName]resource.Quantity{
								corev1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
						StorageClassName: &sourceScName,
						VolumeName:       "source-pv",
					},
					Status: corev1.PersistentVolumeClaimStatus{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteMany,
						},
						Capacity: map[corev1.ResourceName]resource.Quantity{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
						Phase: corev1.ClaimBound,
					},
				},
				&corev1.PersistentVolumeClaim{
					TypeMeta: metav1.TypeMeta{
						Kind:       "PersistentVolumeClaim",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sourcepvc-pvcmigrate",
						Namespace: "testns",
						Annotations: map[string]string{
							"testannotation": "sourcepvc-pvcmigrate",
						},
						Labels: map[string]string{
							"testlabel": "sourcepvc-pvcmigrate",
						},
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.ResourceRequirements{
							Requests: map[corev1.ResourceName]resource.Quantity{
								corev1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
						StorageClassName: &destScName,
						VolumeName:       "dest-pv",
					},
					Status: corev1.PersistentVolumeClaimStatus{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Capacity: map[corev1.ResourceName]resource.Quantity{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
						Phase: corev1.ClaimBound,
					},
				},
				// two PVs
				&corev1.PersistentVolume{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "PersistentVolume",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "source-pv",
						Labels: map[string]string{
							"testlabel": "source-pv",
						},
						Annotations: map[string]string{
							"testannotation": "source-pv",
						},
					},
					Spec: corev1.PersistentVolumeSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteMany,
						},
						Capacity: map[corev1.ResourceName]resource.Quantity{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
						ClaimRef: &corev1.ObjectReference{
							APIVersion: "v1",
							Kind:       "PersistentVolumeClaim",
							Namespace:  "testns",
							Name:       "sourcepvc",
						},
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
						StorageClassName:              sourceScName,
					},
					Status: corev1.PersistentVolumeStatus{
						Phase: corev1.VolumeBound,
					},
				},
				&corev1.PersistentVolume{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "PersistentVolume",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "dest-pv",
						Labels: map[string]string{
							"testlabel": "dest-pv",
						},
						Annotations: map[string]string{
							"testannotation": "dest-pv",
						},
					},
					Spec: corev1.PersistentVolumeSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Capacity: map[corev1.ResourceName]resource.Quantity{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
						ClaimRef: &corev1.ObjectReference{
							APIVersion: "v1",
							Kind:       "PersistentVolumeClaim",
							Namespace:  "testns",
							Name:       "sourcepvc-pvcmigrate",
						},
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
						StorageClassName:              sourceScName,
					},
					Status: corev1.PersistentVolumeStatus{
						Phase: corev1.VolumeBound,
					},
				},
			},
			wantPVs: []corev1.PersistentVolume{
				{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "PersistentVolume",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "dest-pv",
						Labels: map[string]string{
							"testlabel": "dest-pv",
						},
						Annotations: map[string]string{
							desiredReclaimAnnotation: "Delete",
							sourceNsAnnotation:       "testns",
							sourcePvcAnnotation:      "sourcepvc",
							"testannotation":         "dest-pv",
						},
					},
					Spec: corev1.PersistentVolumeSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Capacity: map[corev1.ResourceName]resource.Quantity{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
						StorageClassName:              sourceScName,
					},
					Status: corev1.PersistentVolumeStatus{
						Phase: corev1.VolumeBound,
					},
				},
			},
			wantPVCs: []corev1.PersistentVolumeClaim{
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "PersistentVolumeClaim",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sourcepvc",
						Namespace: "testns",
						Labels: map[string]string{
							"testlabel": "sourcepvc",
						},
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteMany,
						},
						Resources: corev1.ResourceRequirements{
							Requests: map[corev1.ResourceName]resource.Quantity{
								corev1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
						StorageClassName: &destScName,
						VolumeName:       "dest-pv",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			clientset := fake.NewSimpleClientset(tt.resources...)
			testlog := log.New(testWriter{t: t}, "", 0)
			err := swapPVs(context.Background(), testlog, clientset, tt.ns, tt.pvcName)
			if tt.wantErr {
				req.Error(err)
				return
			}
			req.NoError(err)

			finalPVs, err := clientset.CoreV1().PersistentVolumes().List(context.Background(), metav1.ListOptions{})
			req.NoError(err)
			req.Equal(tt.wantPVs, finalPVs.Items)

			finalPVCs, err := clientset.CoreV1().PersistentVolumeClaims(tt.ns).List(context.Background(), metav1.ListOptions{})
			req.NoError(err)
			req.Equal(tt.wantPVCs, finalPVCs.Items)
		})
	}
}

func Test_resetReclaimPolicy(t *testing.T) {
	retainVar := corev1.PersistentVolumeReclaimRetain
	tests := []struct {
		name      string
		resources []runtime.Object
		wantPVs   []corev1.PersistentVolume
		wantPVCs  []corev1.PersistentVolumeClaim
		pv        string
		reclaim   *corev1.PersistentVolumeReclaimPolicy
		wantErr   bool
	}{
		{
			name: "read from annotations",
			pv:   "pvname",
			resources: []runtime.Object{
				&corev1.PersistentVolume{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "PersistentVolume",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "pvname",
						Annotations: map[string]string{
							desiredReclaimAnnotation: "Delete",
							"testannotation":         "dest-pv",
						},
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRecycle,
					},
				},
			},
			wantPVs: []corev1.PersistentVolume{
				{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "PersistentVolume",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "pvname",
						Annotations: map[string]string{
							desiredReclaimAnnotation: "Delete",
							"testannotation":         "dest-pv",
						},
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
					},
				},
			},
		},
		{
			name:    "specified reclaim policy overrides annotation",
			pv:      "pvname",
			reclaim: &retainVar,
			resources: []runtime.Object{
				&corev1.PersistentVolume{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "PersistentVolume",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "pvname",
						Annotations: map[string]string{
							desiredReclaimAnnotation: "Delete",
							"testannotation":         "dest-pv",
						},
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRecycle,
					},
				},
			},
			wantPVs: []corev1.PersistentVolume{
				{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "PersistentVolume",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "pvname",
						Annotations: map[string]string{
							desiredReclaimAnnotation: "Delete",
							"testannotation":         "dest-pv",
						},
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
					},
				},
			},
		},
		{
			name: "no annotation, no reclaim, no change",
			pv:   "pvname",
			resources: []runtime.Object{
				&corev1.PersistentVolume{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "PersistentVolume",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "pvname",
						Annotations: map[string]string{
							"testannotation": "dest-pv",
						},
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRecycle,
					},
				},
			},
			wantPVs: []corev1.PersistentVolume{
				{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "PersistentVolume",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "pvname",
						Annotations: map[string]string{
							"testannotation": "dest-pv",
						},
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRecycle,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			clientset := fake.NewSimpleClientset(tt.resources...)
			testlog := log.New(testWriter{t: t}, "", 0)
			err := resetReclaimPolicy(context.Background(), testlog, clientset, tt.pv, tt.reclaim)
			if tt.wantErr {
				req.Error(err)
				return
			}
			req.NoError(err)

			finalPVs, err := clientset.CoreV1().PersistentVolumes().List(context.Background(), metav1.ListOptions{})
			req.NoError(err)
			req.Equal(tt.wantPVs, finalPVs.Items)
		})
	}
}

func Test_scaleDownPods(t *testing.T) {
	intVar := int32(2)
	intVarZero := int32(0)
	tests := []struct {
		name            string
		matchingPVCs    map[string][]corev1.PersistentVolumeClaim
		resources       []runtime.Object
		wantPods        map[string][]corev1.Pod
		wantDeployments map[string][]appsv1.Deployment
		wantSS          map[string][]appsv1.StatefulSet
		wantErr         bool
		nsList          []string
		backgroundFunc  func(context.Context, *log.Logger, k8sclient.Interface)
	}{
		{
			name:            "minimal test case",
			matchingPVCs:    map[string][]corev1.PersistentVolumeClaim{},
			resources:       []runtime.Object{},
			wantPods:        map[string][]corev1.Pod{},
			wantDeployments: map[string][]appsv1.Deployment{},
			wantSS:          map[string][]appsv1.StatefulSet{},
			wantErr:         false,
			nsList:          []string{},
		},
		{
			name: "existing migration pod",
			matchingPVCs: map[string][]corev1.PersistentVolumeClaim{
				"ns1": {
					{
						TypeMeta: metav1.TypeMeta{
							Kind:       "PersistentVolumeClaim",
							APIVersion: "v1",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "sourcepvc",
							Namespace: "ns1",
						},
						Spec: corev1.PersistentVolumeClaimSpec{},
					},
				},
			},
			resources: []runtime.Object{
				&corev1.Pod{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "migrationpod",
						Namespace: "ns1",
						Labels: map[string]string{
							baseAnnotation: "test",
						},
					},
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{
							{
								Name: "matchingVolume",
								VolumeSource: corev1.VolumeSource{
									PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
										ClaimName: "sourcepvc",
										ReadOnly:  false,
									},
								},
							},
						},
					},
					Status: corev1.PodStatus{},
				},
				&corev1.PersistentVolumeClaim{
					TypeMeta: metav1.TypeMeta{
						Kind:       "PersistentVolumeClaim",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sourcepvc",
						Namespace: "ns1",
					},
					Spec: corev1.PersistentVolumeClaimSpec{},
				},
			},
			wantPods: map[string][]corev1.Pod{
				"ns1": nil,
			},
			wantDeployments: map[string][]appsv1.Deployment{
				"ns1": nil,
			},
			wantSS: map[string][]appsv1.StatefulSet{
				"ns1": nil,
			},
			wantErr: false,
			nsList:  []string{"ns1"},
		},
		{
			name: "other pvc pod",
			matchingPVCs: map[string][]corev1.PersistentVolumeClaim{
				"ns1": {
					{
						TypeMeta: metav1.TypeMeta{
							Kind:       "PersistentVolumeClaim",
							APIVersion: "v1",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "sourcepvc",
							Namespace: "ns1",
						},
						Spec: corev1.PersistentVolumeClaimSpec{},
					},
				},
			},
			resources: []runtime.Object{
				&corev1.Pod{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "otherpod",
						Namespace: "ns1",
					},
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{
							{
								Name: "otherVolume",
								VolumeSource: corev1.VolumeSource{
									PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
										ClaimName: "otherpvc",
										ReadOnly:  false,
									},
								},
							},
						},
					},
					Status: corev1.PodStatus{},
				},
				&corev1.PersistentVolumeClaim{
					TypeMeta: metav1.TypeMeta{
						Kind:       "PersistentVolumeClaim",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sourcepvc",
						Namespace: "ns1",
					},
					Spec: corev1.PersistentVolumeClaimSpec{},
				},
				&corev1.PersistentVolumeClaim{
					TypeMeta: metav1.TypeMeta{
						Kind:       "PersistentVolumeClaim",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "otherpvc",
						Namespace: "ns1",
					},
					Spec: corev1.PersistentVolumeClaimSpec{},
				},
			},
			wantPods: map[string][]corev1.Pod{
				"ns1": {
					{
						TypeMeta: metav1.TypeMeta{
							Kind:       "Pod",
							APIVersion: "v1",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "otherpod",
							Namespace: "ns1",
						},
						Spec: corev1.PodSpec{
							Volumes: []corev1.Volume{
								{
									Name: "otherVolume",
									VolumeSource: corev1.VolumeSource{
										PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
											ClaimName: "otherpvc",
											ReadOnly:  false,
										},
									},
								},
							},
						},
						Status: corev1.PodStatus{},
					},
				},
			},
			wantDeployments: map[string][]appsv1.Deployment{
				"ns1": nil,
			},
			wantSS: map[string][]appsv1.StatefulSet{
				"ns1": nil,
			},
			wantErr: false,
			nsList:  []string{"ns1"},
		},
		{
			name: "existing unowned non-migration pod",
			matchingPVCs: map[string][]corev1.PersistentVolumeClaim{
				"ns1": {
					{
						TypeMeta: metav1.TypeMeta{
							Kind:       "PersistentVolumeClaim",
							APIVersion: "v1",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "sourcepvc",
							Namespace: "ns1",
						},
						Spec: corev1.PersistentVolumeClaimSpec{},
					},
				},
			},
			resources: []runtime.Object{
				&corev1.Pod{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "otherpod",
						Namespace: "ns1",
					},
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{
							{
								Name: "matchingVolume",
								VolumeSource: corev1.VolumeSource{
									PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
										ClaimName: "sourcepvc",
										ReadOnly:  false,
									},
								},
							},
						},
					},
					Status: corev1.PodStatus{},
				},
				&corev1.PersistentVolumeClaim{
					TypeMeta: metav1.TypeMeta{
						Kind:       "PersistentVolumeClaim",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sourcepvc",
						Namespace: "ns1",
					},
					Spec: corev1.PersistentVolumeClaimSpec{},
				},
			},
			wantErr: true,
		},
		{
			name: "existing statefulset pod",
			matchingPVCs: map[string][]corev1.PersistentVolumeClaim{
				"ns1": {
					{
						TypeMeta: metav1.TypeMeta{
							Kind:       "PersistentVolumeClaim",
							APIVersion: "v1",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "sourcepvc",
							Namespace: "ns1",
						},
						Spec: corev1.PersistentVolumeClaimSpec{},
					},
				},
			},
			resources: []runtime.Object{
				&appsv1.StatefulSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "StatefulSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "app-ss",
						Namespace: "ns1",
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: &intVar,
					},
				},
				&corev1.Pod{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sspod",
						Namespace: "ns1",
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "StatefulSet",
								Name:       "app-ss",
							},
						},
					},
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{
							{
								Name: "matchingVolume",
								VolumeSource: corev1.VolumeSource{
									PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
										ClaimName: "sourcepvc",
										ReadOnly:  false,
									},
								},
							},
						},
					},
					Status: corev1.PodStatus{},
				},
				&corev1.PersistentVolumeClaim{
					TypeMeta: metav1.TypeMeta{
						Kind:       "PersistentVolumeClaim",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sourcepvc",
						Namespace: "ns1",
					},
					Spec: corev1.PersistentVolumeClaimSpec{},
				},
			},
			wantPods: map[string][]corev1.Pod{
				"ns1": nil,
			},
			wantDeployments: map[string][]appsv1.Deployment{
				"ns1": nil,
			},
			wantSS: map[string][]appsv1.StatefulSet{
				"ns1": {
					{
						TypeMeta: metav1.TypeMeta{
							Kind:       "StatefulSet",
							APIVersion: "apps/v1",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "app-ss",
							Namespace: "ns1",
							Annotations: map[string]string{
								scaleAnnotation: "2",
							},
						},
						Spec: appsv1.StatefulSetSpec{
							Replicas: &intVarZero,
						},
					},
				},
			},
			wantErr: false,
			nsList:  []string{"ns1"},
			backgroundFunc: func(ctx context.Context, logger *log.Logger, k k8sclient.Interface) {
				// watch for the statefulset to be scaled down, and then delete the pod
				for true {
					select {
					case <-time.After(time.Second / 100):
						// check statefulset, maybe delete pod
						ss, err := k.AppsV1().StatefulSets("ns1").Get(ctx, "app-ss", metav1.GetOptions{})
						if err != nil {
							logger.Printf("got error checking statefulset app-ss: %s", err.Error())
							return
						}
						if ss.Spec.Replicas != nil && *ss.Spec.Replicas == 0 {
							err = k.CoreV1().Pods("ns1").Delete(ctx, "sspod", metav1.DeleteOptions{})
							if err != nil {
								logger.Printf("got error deleting pod sspod: %s", err.Error())
							}
							return
						}
					case <-ctx.Done():
						logger.Print("never saw statefulset scale down")
						return
					}
				}
			},
		},
		{
			name: "existing deployment pod",
			matchingPVCs: map[string][]corev1.PersistentVolumeClaim{
				"ns1": {
					{
						TypeMeta: metav1.TypeMeta{
							Kind:       "PersistentVolumeClaim",
							APIVersion: "v1",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "sourcepvc",
							Namespace: "ns1",
						},
						Spec: corev1.PersistentVolumeClaimSpec{},
					},
				},
			},
			resources: []runtime.Object{
				&appsv1.Deployment{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Deployment",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "app-dep",
						Namespace: "ns1",
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: &intVar,
					},
				},
				&appsv1.ReplicaSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "ReplicaSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "app-rs",
						Namespace: "ns1",
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       "app-dep",
							},
						},
					},
				},
				&corev1.Pod{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "deppod",
						Namespace: "ns1",
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "ReplicaSet",
								Name:       "app-rs",
							},
						},
					},
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{
							{
								Name: "matchingVolume",
								VolumeSource: corev1.VolumeSource{
									PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
										ClaimName: "sourcepvc",
										ReadOnly:  false,
									},
								},
							},
						},
					},
					Status: corev1.PodStatus{},
				},
				&corev1.PersistentVolumeClaim{
					TypeMeta: metav1.TypeMeta{
						Kind:       "PersistentVolumeClaim",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sourcepvc",
						Namespace: "ns1",
					},
					Spec: corev1.PersistentVolumeClaimSpec{},
				},
			},
			wantPods: map[string][]corev1.Pod{
				"ns1": nil,
			},
			wantDeployments: map[string][]appsv1.Deployment{
				"ns1": {
					{
						TypeMeta: metav1.TypeMeta{
							Kind:       "Deployment",
							APIVersion: "apps/v1",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "app-dep",
							Namespace: "ns1",
							Annotations: map[string]string{
								scaleAnnotation: "2",
							},
						},
						Spec: appsv1.DeploymentSpec{
							Replicas: &intVarZero,
						},
					},
				},
			},
			wantSS: map[string][]appsv1.StatefulSet{
				"ns1": nil,
			},
			wantErr: false,
			nsList:  []string{"ns1"},
			backgroundFunc: func(ctx context.Context, logger *log.Logger, k k8sclient.Interface) {
				// watch for the deployment to be scaled down, and then delete the pod
				for true {
					select {
					case <-time.After(time.Second / 100):
						// check deployment, maybe delete pod
						ss, err := k.AppsV1().Deployments("ns1").Get(ctx, "app-dep", metav1.GetOptions{})
						if err != nil {
							logger.Printf("got error checking deployment app-dep: %s", err.Error())
							return
						}
						if ss.Spec.Replicas != nil && *ss.Spec.Replicas == 0 {
							err = k.CoreV1().Pods("ns1").Delete(ctx, "deppod", metav1.DeleteOptions{})
							if err != nil {
								logger.Printf("got error deleting pod deppod: %s", err.Error())
							}
							return
						}
					case <-ctx.Done():
						logger.Print("never saw deployment scale down")
						return
					}
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			testCtx, cancelfunc := context.WithTimeout(context.Background(), time.Minute) // if your test takes more than 1m, there are issues
			defer cancelfunc()
			clientset := fake.NewSimpleClientset(tt.resources...)
			testlog := log.New(testWriter{t: t}, "", 0)
			if tt.backgroundFunc != nil {
				go tt.backgroundFunc(testCtx, testlog, clientset)
			}
			err := scaleDownPods(testCtx, testlog, clientset, tt.matchingPVCs, time.Second/20)
			if tt.wantErr {
				req.Error(err)
				testlog.Printf("got expected error %q", err.Error())
				return
			}
			req.NoError(err)

			actualPods := map[string][]corev1.Pod{}
			actualDeployments := map[string][]appsv1.Deployment{}
			actualSS := map[string][]appsv1.StatefulSet{}
			for _, ns := range tt.nsList {
				finalNsPods, err := clientset.CoreV1().Pods(ns).List(testCtx, metav1.ListOptions{})
				req.NoError(err)
				actualPods[ns] = finalNsPods.Items

				finalNsDeps, err := clientset.AppsV1().Deployments(ns).List(testCtx, metav1.ListOptions{})
				req.NoError(err)
				actualDeployments[ns] = finalNsDeps.Items

				finalNsSS, err := clientset.AppsV1().StatefulSets(ns).List(testCtx, metav1.ListOptions{})
				req.NoError(err)
				actualSS[ns] = finalNsSS.Items
			}
			req.Equal(tt.wantPods, actualPods)
			req.Equal(tt.wantDeployments, actualDeployments)
			req.Equal(tt.wantSS, actualSS)
		})
	}
}

func Test_swapDefaults(t *testing.T) {
	tests := []struct {
		name         string
		resources    []runtime.Object
		wantSCs      []storagev1.StorageClass
		oldDefaultSC string
		newDefaultSC string
		wantErr      bool
	}{
		{
			name: "proper setup",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "aSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "true",
						},
					},
					Provisioner: "abc",
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
					},
					Provisioner: "xyz",
				},
			},
			oldDefaultSC: "aSC",
			newDefaultSC: "bSC",
			wantSCs: []storagev1.StorageClass{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "aSC",
						Annotations: map[string]string{},
					},
					Provisioner: "abc",
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "true",
						},
					},
					Provisioner: "xyz",
				},
			},
		},

		{
			name: "other existing annotations setup",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "aSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "true",
							"otherannotation":               "blah",
						},
					},
					Provisioner: "abc",
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
						Annotations: map[string]string{
							"secondannotation": "xyz",
						},
					},
					Provisioner: "xyz",
				},
			},
			oldDefaultSC: "aSC",
			newDefaultSC: "bSC",
			wantSCs: []storagev1.StorageClass{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "aSC",
						Annotations: map[string]string{
							"otherannotation": "blah",
						},
					},
					Provisioner: "abc",
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "true",
							"secondannotation":              "xyz",
						},
					},
					Provisioner: "xyz",
				},
			},
		},

		{
			name: "new default SC does not exist",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "aSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "true",
						},
					},
					Provisioner: "abc",
				},
			},
			oldDefaultSC: "aSC",
			newDefaultSC: "bSC",
			wantErr:      true,
		},
		{
			name: "old default SC does not exist",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
					},
					Provisioner: "xyz",
				},
			},
			oldDefaultSC: "aSC",
			newDefaultSC: "bSC",
			wantSCs: []storagev1.StorageClass{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "true",
						},
					},
					Provisioner: "xyz",
				},
			},
		},
		{
			name: "old SC not actually default",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "aSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "false",
						},
					},
					Provisioner: "abc",
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
					},
					Provisioner: "xyz",
				},
			},
			oldDefaultSC: "aSC",
			newDefaultSC: "bSC",
			wantSCs: []storagev1.StorageClass{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "aSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "false",
						},
					},
					Provisioner: "abc",
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "true",
						},
					},
					Provisioner: "xyz",
				},
			},
		},
		{
			name: "old SC not actually default (nil annotations edition)",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "aSC",
					},
					Provisioner: "abc",
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
					},
					Provisioner: "xyz",
				},
			},
			oldDefaultSC: "aSC",
			newDefaultSC: "bSC",
			wantSCs: []storagev1.StorageClass{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "aSC",
					},
					Provisioner: "abc",
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "true",
						},
					},
					Provisioner: "xyz",
				},
			},
		},
		{
			name: "old SC not actually default (other annotations edition)",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "aSC",
						Annotations: map[string]string{
							"abc": "xyz",
						},
					},
					Provisioner: "abc",
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
					},
					Provisioner: "xyz",
				},
			},
			oldDefaultSC: "aSC",
			newDefaultSC: "bSC",
			wantSCs: []storagev1.StorageClass{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "aSC",
						Annotations: map[string]string{
							"abc": "xyz",
						},
					},
					Provisioner: "abc",
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "true",
						},
					},
					Provisioner: "xyz",
				},
			},
		},
		{
			name: "another SC was actually the default",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "aSC",
					},
					Provisioner: "abc",
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
					},
					Provisioner: "xyz",
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "cSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "true",
						},
					},
					Provisioner: "def",
				},
			},
			oldDefaultSC: "aSC",
			newDefaultSC: "bSC",
			wantErr:      true,
		},
		{
			name: "another SC was actually the default (other annotation edition)",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "aSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "false",
						},
					},
					Provisioner: "abc",
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
					},
					Provisioner: "xyz",
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "cSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "true",
						},
					},
					Provisioner: "def",
				},
			},
			oldDefaultSC: "aSC",
			newDefaultSC: "bSC",
			wantErr:      true,
		},
		{
			name: "the desired SC was already the default",
			resources: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "true",
						},
					},
					Provisioner: "xyz",
				},
			},
			oldDefaultSC: "aSC",
			newDefaultSC: "bSC",
			wantSCs: []storagev1.StorageClass{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bSC",
						Annotations: map[string]string{
							IsDefaultStorageClassAnnotation: "true",
						},
					},
					Provisioner: "xyz",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			clientset := fake.NewSimpleClientset(tt.resources...)
			testlog := log.New(testWriter{t: t}, "", 0)
			err := swapDefaultStorageClasses(context.Background(), testlog, clientset, tt.oldDefaultSC, tt.newDefaultSC)
			if tt.wantErr {
				req.Error(err)
				testlog.Printf("Got expected error %s", err.Error())
				return
			}
			req.NoError(err)

			finalSCs, err := clientset.StorageV1().StorageClasses().List(context.Background(), metav1.ListOptions{})
			req.NoError(err)
			req.Equal(tt.wantSCs, finalSCs.Items)
		})
	}
}

func Test_waitForDeletion(t *testing.T) {
	tests := []struct {
		name           string
		backgroundFunc func(context.Context, *log.Logger, k8sclient.Interface)
	}{
		{
			name: "wait 0.5s",
			backgroundFunc: func(ctx context.Context, logger *log.Logger, k k8sclient.Interface) {
				// wait a period of time before deleting the PVC
				time.Sleep(time.Second / 2)
				err := k.CoreV1().PersistentVolumeClaims("test").Delete(ctx, "test", metav1.DeleteOptions{})
				if err != nil {
					logger.Printf("got error deleting pvc test in test: %s", err.Error())
				}
				logger.Printf("deleted PVC")
				return
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			testCtx, cancelfunc := context.WithTimeout(context.Background(), time.Minute) // if your test takes more than 1m, there are issues
			defer cancelfunc()
			clientset := fake.NewSimpleClientset(
				[]runtime.Object{
					&corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test",
							Namespace: "test",
						},
					},
				}...,
			)
			testlog := log.New(testWriter{t: t}, "", 0)
			if tt.backgroundFunc != nil {
				go tt.backgroundFunc(testCtx, testlog, clientset)
			}
			err := waitForDeletion(testCtx, clientset, "test", "test")
			req.NoError(err)
			actualPVC, err := clientset.CoreV1().PersistentVolumeClaims("test").Get(testCtx, "test", metav1.GetOptions{})
			req.Errorf(err, "the PVC 'test' in 'test' should not have been found after waiting for its deletion")
			var nilPVC *corev1.PersistentVolumeClaim
			req.Equal(nilPVC, actualPVC)
		})
	}
}

func Test_newPvcName(t *testing.T) {
	tests := []struct {
		originalName string
		want         string
	}{
		{
			originalName: "abc",
			want:         "abc-pvcmigrate",
		},
		{
			originalName: "very very very long name test with a suffix that might be the only unique part of it 0",
			want:         "very very very long name test wy unique part of it 0-pvcmigrate",
		},
		{
			originalName: "0 very very very long name test with a prefix that might be the only unique part of it",
			want:         "0 very very very long name testnly unique part of it-pvcmigrate",
		},
		{
			originalName: "63 character (after suffix) name is untouched paddin",
			want:         "63 character (after suffix) name is untouched paddin-pvcmigrate",
		},
	}
	for _, tt := range tests {
		t.Run(tt.originalName, func(t *testing.T) {
			req := require.New(t)
			got := newPvcName(tt.originalName)
			req.Equal(tt.want, got)
		})
	}
}

func Test_copyAllPVCs(t *testing.T) {
	type podEvent struct {
		podAge    time.Duration
		podStatus corev1.PodPhase
	}

	tests := []struct {
		name         string
		matchingPVCs map[string][]corev1.PersistentVolumeClaim
		events       map[string]map[string][]podEvent // map of namespaces to pod names to a list of what status a pod should have and when
		wantErr      bool
	}{
		{
			name:         "minimal test case",
			matchingPVCs: map[string][]corev1.PersistentVolumeClaim{},
			wantErr:      false,
		},
		{
			name: "one PVC",
			matchingPVCs: map[string][]corev1.PersistentVolumeClaim{
				"ns1": {
					{
						TypeMeta: metav1.TypeMeta{
							Kind:       "PersistentVolumeClaim",
							APIVersion: "v1",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "sourcepvc",
							Namespace: "ns1",
						},
						Spec: corev1.PersistentVolumeClaimSpec{},
					},
				},
			},
			wantErr: false,
			events: map[string]map[string][]podEvent{
				"ns1": {
					"migrate-sourcepvc": {
						{
							podAge:    time.Millisecond * 100,
							podStatus: corev1.PodRunning,
						},
						{
							podAge:    time.Millisecond * 200,
							podStatus: corev1.PodSucceeded,
						},
					},
				},
			},
		},
		{
			name: "one PVC, failure",
			matchingPVCs: map[string][]corev1.PersistentVolumeClaim{
				"ns1": {
					{
						TypeMeta: metav1.TypeMeta{
							Kind:       "PersistentVolumeClaim",
							APIVersion: "v1",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "sourcepvc",
							Namespace: "ns1",
						},
						Spec: corev1.PersistentVolumeClaimSpec{},
					},
				},
			},
			wantErr: true,
			events: map[string]map[string][]podEvent{
				"ns1": {
					"migrate-sourcepvc": {
						{
							podAge:    time.Millisecond * 100,
							podStatus: corev1.PodRunning,
						},
						{
							podAge:    time.Millisecond * 200,
							podStatus: corev1.PodFailed,
						},
					},
				},
			},
		},
		{
			name: "three PVCs succeed",
			matchingPVCs: map[string][]corev1.PersistentVolumeClaim{
				"ns1": {
					{
						TypeMeta: metav1.TypeMeta{
							Kind:       "PersistentVolumeClaim",
							APIVersion: "v1",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "sourcepvc",
							Namespace: "ns1",
						},
						Spec: corev1.PersistentVolumeClaimSpec{},
					},
					{
						TypeMeta: metav1.TypeMeta{
							Kind:       "PersistentVolumeClaim",
							APIVersion: "v1",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pvc2",
							Namespace: "ns1",
						},
						Spec: corev1.PersistentVolumeClaimSpec{},
					},
				},
				"ns2": {
					{
						TypeMeta: metav1.TypeMeta{
							Kind:       "PersistentVolumeClaim",
							APIVersion: "v1",
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pvc3",
							Namespace: "ns2",
						},
						Spec: corev1.PersistentVolumeClaimSpec{},
					},
				},
			},
			wantErr: false,
			events: map[string]map[string][]podEvent{
				"ns1": {
					"migrate-sourcepvc": {
						{
							podAge:    time.Millisecond * 100,
							podStatus: corev1.PodRunning,
						},
						{
							podAge:    time.Millisecond * 200,
							podStatus: corev1.PodSucceeded,
						},
					},
					"migrate-pvc2": {
						{
							podAge:    time.Millisecond * 100,
							podStatus: corev1.PodSucceeded,
						},
					},
				},
				"ns2": {
					"migrate-pvc3": {
						{
							podAge:    time.Millisecond * 200,
							podStatus: corev1.PodSucceeded,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			testCtx, cancelfunc := context.WithTimeout(context.Background(), time.Second*10) // if your test takes more than 10s, there are issues
			defer cancelfunc()
			clientset := fake.NewSimpleClientset()
			testlog := log.New(testWriter{t: t}, "", 0)

			// handle making the pods start/succeed/fail/etc
			go func(ctx context.Context, logger *log.Logger, k k8sclient.Interface, events map[string]map[string][]podEvent) {
				for true {
					select {
					case <-time.After(time.Millisecond * 10):
						for ns, nsEvents := range events {
							for podName, podEvents := range nsEvents {
								pod, err := k.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
								if err != nil {
									if strings.Contains(err.Error(), "not found") {
										continue
									}

									logger.Printf("got error checking pod %s in %s: %s", podName, ns, err.Error())
									return
								}
								if pod.Status.StartTime == nil {
									pod.Status.StartTime = &metav1.Time{Time: time.Now()}
									pod.Status.Phase = corev1.PodPending
								} else {
									for _, event := range podEvents {
										if time.Now().After(pod.Status.StartTime.Add(event.podAge)) {
											pod.Status.Phase = event.podStatus
										}
									}
								}

								_, err = k.CoreV1().Pods(ns).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
								if err != nil {
									logger.Printf("got error updating pod status: %s", err.Error())
									return
								}
							}
						}
					case <-ctx.Done():
						return
					}
				}
			}(testCtx, testlog, clientset, tt.events)

			err := copyAllPVCs(testCtx, testlog, clientset, "sourcesc", "destsc", "testrsyncimage", tt.matchingPVCs, false, time.Millisecond*10)
			if tt.wantErr {
				req.Error(err)
				testlog.Printf("got expected error %q", err.Error())
				return
			}
			req.NoError(err)
		})
	}
}
