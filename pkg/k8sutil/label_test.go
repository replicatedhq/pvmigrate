package k8sutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewPrefixedName(t *testing.T) {
	tests := []struct {
		name         string
		originalName string
		prefix       string
		want         string
	}{
		{
			name:         "when name is < 63 chars expect new name to be prefixed",
			originalName: "abc",
			prefix:       "pvcmigrate",
			want:         "pvcmigrate-abc",
		},
		{
			name:         "when name is > 63 chars expect new name to be prefixed and 63 chars long",
			originalName: "this label will exceed its allowed length and than be truncated",
			prefix:       "pvcmigrate",
			want:         "pvcmigrate-this label will excewed length and than be truncated",
		},
	}
	for _, tt := range tests {
		t.Run(tt.originalName, func(t *testing.T) {
			req := require.New(t)
			got := NewPrefixedName(tt.prefix, tt.originalName)
			req.Equal(tt.want, got)
		})
	}
}
