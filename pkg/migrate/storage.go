package migrate

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/pointer"
)

// buildResourceName concats prefix and suffix and trims if longer than 63 chars.
func buildResourceName(prefix, suffix string) string {
	name := fmt.Sprintf("%s-%s", prefix, suffix)
	if len(name) > 63 {
		name = name[0:31] + name[len(name)-32:]
	}
	return name
}

// buildDiskAvailablePod returns a pod that mounts the provided pvc under /data and has as its
// command a "df /data". returned pod will have the provided node as required during scheduling.
func buildDiskAvailablePod(namespace, pvcName, image, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      buildResourceName("disk-free", pvcName),
			Namespace: namespace,
			Labels: map[string]string{
				"pvmigrate": "volume-bind",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "kubernetes.io/hostname",
										Operator: corev1.NodeSelectorOperator("In"),
										Values:   []string{node},
									},
								},
							},
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "vol",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcName,
						},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:    "volume-bind",
					Image:   image,
					Command: []string{"df"},
					Args:    []string{"-B1", "/data"},
					VolumeMounts: []corev1.VolumeMount{
						{
							MountPath: "/data",
							Name:      "vol",
						},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("32Mi"),
							corev1.ResourceCPU:    resource.MustParse("10m"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("32Mi"),
							corev1.ResourceCPU:    resource.MustParse("10m"),
						},
					},
				},
			},
		},
	}
}

// buildTempPVC returns a new 1Gi pvc with provided name and using provided storage class name.
// this temporary pvc is used when evaluating the node available disk size when using openebs.
func buildTempPVC(pvcname, scname string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      pvcname,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: pointer.String(scname),
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}
}

// logPodConditions prints the provided pod status conditions as a table through the provided
// logger.
func logPodConditions(logger *log.Logger, status *corev1.PodStatus) {
	if status == nil {
		return
	}

	logger.Printf("unable to run pod, conditions:")
	tw := tabwriter.NewWriter(logger.Writer(), 2, 2, 1, ' ', 0)
	fmt.Fprintf(tw, "type:\tstatus:\treason:\tmessage:\n")
	for _, cond := range status.Conditions {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", cond.Type, cond.Status, cond.Reason, cond.Message)
	}
	tw.Flush()
}

// parseDiskAvailablePodOutput parses the output (log) of the disk available checker pod. the
// output of the pod is expected to follow the format:
//
// Filesystem     1K-blocks     Used Available Use% Mounted on
// /dev/sda2       61608748 48707392   9739400  84% /data
func parseDiskAvailablePodOutput(podout []byte) (int64, error) {
	buf := bytes.NewBuffer(podout)
	scanner := bufio.NewScanner(buf)
	for scanner.Scan() {
		words := strings.Fields(scanner.Text())
		if len(words) == 0 {
			continue
		}

		// lastpos is where the mount point lives.
		lastpos := len(words) - 1
		if words[lastpos] != "/data" {
			continue
		}

		// availpos is the position where the actual available space lives.
		availpos := len(words) - 3
		bytes, err := strconv.ParseInt(words[availpos], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("error parsing %q as available space", words[availpos])
		}

		return bytes, nil
	}
	return 0, fmt.Errorf("unable to locate free space info in pod log: %s", string(podout))
}

// freeDiskSpacePerNode attempts to gather the free disk space for all nodes in the cluster. this
// function creates a temporary pvc and spawns a pod in each of the nodes of the cluster, the pod
// runs a "df" command and we parse its output. this is useful only if the storage class is backed
// by openebs as the total disk size is exposed inside the pod.
func freeDiskSpacePerNode(ctx context.Context, logger *log.Logger, cli k8sclient.Interface, scname, image string) (map[string]int64, error) {
	nodes, err := cli.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to list nodes: %w", err)
	}

	result := map[string]int64{}
	for _, node := range nodes.Items {
		pvcname := buildResourceName("disk-free", node.Name)
		tmppvc := buildTempPVC(pvcname, scname)

		_, err := cli.CoreV1().PersistentVolumeClaims("default").Create(ctx, tmppvc, metav1.CreateOptions{})
		if err != nil && !errors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("unable to create pvc: %w", err)
		}

		defer func(name string) {
			if err := cli.CoreV1().PersistentVolumeClaims("default").Delete(
				ctx, name, metav1.DeleteOptions{},
			); err != nil {
				logger.Printf("unable to delete pvc %s: %s", name, err)
			}
		}(pvcname)

		pod := buildDiskAvailablePod("default", tmppvc.Name, image, node.Name)
		podout, status, err := runEphemeralPod(ctx, logger, cli, 30*time.Second, pod)
		if err != nil {
			log.Printf("pod log:")
			log.Printf(string(podout))
			logPodConditions(logger, status)
			return nil, fmt.Errorf("unable to run pod on node %s: %w", node.Name, err)
		}

		avail, err := parseDiskAvailablePodOutput(podout)
		if err != nil {
			log.Printf("pod log:")
			log.Printf(string(podout))
			return nil, fmt.Errorf("unable to parse pod output: %w", err)
		}

		result[node.Name] = avail
	}
	return result, nil
}

// runEphemeralPod starts provided pod and waits until it finishes. the values returned are the
// pod logs, its last status, and an error. returned logs and pod status may be nil depending on
// the type of failure.
func runEphemeralPod(ctx context.Context, logger *log.Logger, cli k8sclient.Interface, timeout time.Duration, pod *corev1.Pod) ([]byte, *corev1.PodStatus, error) {
	podidx, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create pod id: %w", err)
	}

	pod, err = cli.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("error creating pod %s: %w", podidx, err)
	}

	defer func() {
		if err = cli.CoreV1().Pods(pod.Namespace).Delete(
			ctx, pod.Name, metav1.DeleteOptions{},
		); err != nil {
			logger.Printf("unable to delete pod %s: %s", podidx, err)
		}
	}()

	startedAt := time.Now()
	var lastPodStatus corev1.PodStatus
	var hasTimedOut bool
	for {
		var gotPod *corev1.Pod
		if gotPod, err = cli.CoreV1().Pods(pod.Namespace).Get(
			ctx, pod.Name, metav1.GetOptions{},
		); err != nil {
			return nil, nil, fmt.Errorf("failed getting pod %s: %w", podidx, err)
		}

		lastPodStatus = gotPod.Status
		if gotPod.Status.Phase == corev1.PodSucceeded {
			break
		}

		time.Sleep(time.Second)
		if time.Now().After(startedAt.Add(timeout)) {
			hasTimedOut = true
			break
		}
	}

	podlogs, err := cli.CoreV1().Pods(pod.Namespace).GetLogs(
		pod.Name, &corev1.PodLogOptions{},
	).Stream(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting pod log stream: %w", err)
	}

	defer func() {
		if err := podlogs.Close(); err != nil {
			logger.Printf("unable to close pod log stream: %s", err)
		}
	}()

	output, err := io.ReadAll(podlogs)
	if err != nil {
		return nil, &lastPodStatus, fmt.Errorf("unable to read pod logs: %w", err)
	}

	if hasTimedOut {
		return output, &lastPodStatus, fmt.Errorf("timeout waiting for the pod")
	}

	return output, &lastPodStatus, nil
}

// pvsByStorageClass returns a map of persistent volumes using the provided storage class name.
// returned pvs map is indexed by pv's name.
func pvsByStorageClass(ctx context.Context, clientset k8sclient.Interface, scname string) (map[string]corev1.PersistentVolume, error) {
	allpvs, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get persistent volumes: %w", err)
	}

	pvs := map[string]corev1.PersistentVolume{}
	for _, pv := range allpvs.Items {
		if pv.Spec.StorageClassName != scname {
			continue
		}

		idx, err := cache.MetaNamespaceKeyFunc(&pv)
		if err != nil {
			return nil, fmt.Errorf("unable to build namespace name key: %w", err)
		}

		pvs[idx] = *pv.DeepCopy()
	}
	return pvs, nil
}

// pvcsForPVs returns a pv to pvc mapping. the returned map is indexed by the pv name.
func pvcsForPVs(ctx context.Context, cli k8sclient.Interface, pvs map[string]corev1.PersistentVolume) (map[string]corev1.PersistentVolumeClaim, error) {
	pvcs := map[string]corev1.PersistentVolumeClaim{}
	for pvidx, pv := range pvs {
		cref := pv.Spec.ClaimRef
		if cref == nil {
			return nil, fmt.Errorf("pv %s without associated PVC", pvidx)
		}

		pvc, err := cli.CoreV1().PersistentVolumeClaims(cref.Namespace).Get(
			ctx, cref.Name, metav1.GetOptions{},
		)
		if err != nil {
			return nil, fmt.Errorf("failed to get pvc %s for pv %s: %w", cref.Name, pvidx, err)
		}

		pvcs[pvidx] = *pvc.DeepCopy()
	}
	return pvcs, nil
}

// largestPVS lists all pvs and associated pvcs, then look through the pvcs that are actually in
// use by any pod. returns a map whose index is the node name and the value is the biggest pv
// attached to a pod running in said node. this function also returns the largest pv that is not
// mounted in any pod.
func largestPVS(ctx context.Context, cli k8sclient.Interface, scname string) (map[string]int64, int64, error) {
	pvs, err := pvsByStorageClass(ctx, cli, scname)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to get pvs: %w", err)
	}

	pvcs, err := pvcsForPVs(ctx, cli, pvs)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to get pvcs for pvs: %w", err)
	}

	var largestUnused int64
	result := map[string]int64{}
	for pvidx, pvc := range pvcs {
		pv, ok := pvs[pvidx]
		if !ok {
			return nil, 0, fmt.Errorf("pv for pvc %s/%s not found", pvc.Namespace, pvc.Name)
		}

		pods, err := cli.CoreV1().Pods(pvc.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, 0, fmt.Errorf("error listing pods: %w", err)
		}

		var inuse bool
		for _, pod := range pods.Items {
			if !podUsesPVC(pod, pvc) {
				continue
			}

			bytes, dec := pv.Spec.Capacity.Storage().AsInt64()
			if !dec {
				return nil, 0, fmt.Errorf("error parsing pv %s storage size", pvidx)
			}

			inuse = true
			if _, ok := result[pod.Spec.NodeName]; !ok {
				result[pod.Spec.NodeName] = bytes
				continue
			}

			if bytes > result[pod.Spec.NodeName] {
				result[pod.Spec.NodeName] = bytes
			}
		}

		// if the pv is bound to a pvc in use we can move on to the next one as
		// its size has already been catalogued into the node where the pod is
		// running. if it is not in use we make sure we keep track of it as well
		// but then it is not assigned to any specific node.
		if inuse {
			continue
		}

		bytes, dec := pv.Spec.Capacity.Storage().AsInt64()
		if !dec {
			return nil, 0, fmt.Errorf("error parsing pv %s storage size", pvidx)
		}

		if bytes > largestUnused {
			largestUnused = bytes
		}
	}

	return result, largestUnused, nil
}

// podUsesPVC returs true of provided pod has provided pvc among its volumes.
func podUsesPVC(pod corev1.Pod, pvc corev1.PersistentVolumeClaim) bool {
	if pod.Namespace != pvc.Namespace {
		return false
	}

	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim == nil {
			continue
		}
		if vol.PersistentVolumeClaim.ClaimName != pvc.Name {
			continue
		}
		return true
	}

	return false
}
