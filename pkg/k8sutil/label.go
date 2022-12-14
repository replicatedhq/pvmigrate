package k8sutil

import "fmt"

// NewPrefixedName returns a name prefixed by prefix and with length that is no longer than 63
// chars
func NewPrefixedName(prefix, original string) string {
	newName := fmt.Sprintf("%s-%s", prefix, original)
	if len(newName) > 63 {
		newName = newName[0:31] + newName[len(newName)-32:]
	}
	return newName
}
