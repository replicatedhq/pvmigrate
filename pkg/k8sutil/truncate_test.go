package k8sutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
			originalName: "very very very 253longlonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglong name test with a suffix that might be the only unique part of it 0",
			want:         "very very very 253longlonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglongloonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglong name test with a suffix that might be the only unique part of it 0-pvcmigrate",
		},
		{
			originalName: "0 very very very 253longlonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglong name test with a prefix that might be the only unique part of it",
			want:         "0 very very very 253longlonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglongglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglong name test with a prefix that might be the only unique part of it-pvcmigrate",
		},
		{
			originalName: "253 character (after suffix)  253longlonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglong name is untouched paddin",
			want:         "253 character (after suffix)  253longlonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglonglong name is untouched paddin-pvcmigrate",
		},
	}
	for _, tt := range tests {
		t.Run(tt.originalName, func(t *testing.T) {
			req := require.New(t)
			got := NewPvcName(tt.originalName)
			req.Equal(tt.want, got)
		})
	}
}

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
