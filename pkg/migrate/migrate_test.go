package migrate

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
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
			clientset := fake.NewSimpleClientset(test.resources...)
			err := scaleUpPods(testWriter{t: t}, clientset, test.namespaces)
			assert.NoError(t, err)

			err = test.validate(clientset, t)
			assert.NoError(t, err)
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
			clientset := fake.NewSimpleClientset(test.resources...)
			err := mutatePV(clientset, test.pvname, test.ttmutator, test.ttchecker)
			assert.NoError(t, err)

			err = test.validate(clientset, t)
			assert.NoError(t, err)
		})
	}
}

func TestValidateStorageClasses(t *testing.T) {
	tests := []struct {
		name      string
		resources []runtime.Object
		sourceSC  string
		destSC    string
		wantErr   bool
	}{
		{
			name:     "both storageclasses exist and are distinct",
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset(test.resources...)
			err := validateStorageClasses(testWriter{t: t}, clientset, test.sourceSC, test.destSC)
			if !test.wantErr {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}

		})
	}
}
