package version

import "testing"

func TestPrint(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "this should not panic",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Print()
		})
	}
}
