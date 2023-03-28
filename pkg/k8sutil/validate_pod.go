package k8sutil

import (
	corev1 "k8s.io/api/core/v1"
)

func IsPodReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
