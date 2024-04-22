package k8sutil

import "fmt"

const nameSuffix = "-pvcmigrate"

// if the length after adding the suffix is more than 253 characters, we need to reduce that to fit within k8s limits
// pruning from the end runs the risk of dropping the '0'/'1'/etc of a statefulset's PVC name
// pruning from the front runs the risk of making a-replica-... and b-replica-... collide
// so this removes characters from the middle of the string
func NewPvcName(originalName string) string {
	candidate := originalName + nameSuffix
	if len(candidate) <= 253 {
		return candidate
	}

	// remove characters from the middle of the string to reduce the total length to 253 characters
	newCandidate := candidate[0:100] + candidate[len(candidate)-153:]
	return newCandidate
}

// NewPrefixedName returns a name prefixed by prefix and with length that is no longer than 63
// chars
func NewPrefixedName(prefix, original string) string {
	newName := fmt.Sprintf("%s-%s", prefix, original)
	if len(newName) > 63 {
		newName = newName[0:31] + newName[len(newName)-32:]
	}
	return newName
}
