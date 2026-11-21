package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSupervisorRetriesOnlyTheLegacyHandoffOnce(t *testing.T) {
	for _, tc := range []struct {
		name string
		exit string
		want string
	}{
		{"legacy handoff", "75", "xx"},
		{"startup failure", "1", "x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := locations{DataDir: t.TempDir()}
			if err := os.Mkdir(l.installation(), 0o700); err != nil {
				t.Fatal(err)
			}
			l.Java = filepath.Join(l.DataDir, "test-java")
			const script = "#!/bin/sh\nprintf x >> \"$T8_COUNT\"\nexit \"$T8_EXIT\"\n"
			if err := os.WriteFile(l.Java, []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			countPath := filepath.Join(l.DataDir, "launch-count")
			t.Setenv("T8_COUNT", countPath)
			t.Setenv("T8_EXIT", tc.exit)
			if err := (&supervisor{}).launch(l); err == nil {
				t.Fatal("expected the final failed child to be reported")
			}
			got, err := os.ReadFile(countPath)
			if err != nil || string(got) != tc.want {
				t.Fatalf("launch count = %q, error = %v, want %q", got, err, tc.want)
			}
		})
	}
}
