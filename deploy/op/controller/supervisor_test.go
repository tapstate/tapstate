package main

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWaitForStartRequiresAnEntryPoint(t *testing.T) {
	for _, test := range []struct {
		status    serviceStatus
		wantError string
	}{
		{serviceStatus{Phase: "Ready"}, ""},
		{serviceStatus{Phase: "Waiting for storage setup"}, ""},
		{serviceStatus{Phase: "Waiting for first administrator"}, ""},
		{serviceStatus{Phase: "Failed", Error: "Java exited with status 1"}, "Java exited with status 1"},
		{serviceStatus{Phase: "Failed"}, "Failed"},
		{serviceStatus{Phase: "Stopped"}, "Stopped"},
	} {
		t.Run(test.status.Phase+test.status.Error, func(t *testing.T) {
			l := locations{DataDir: t.TempDir()}
			if err := os.Mkdir(l.runDir(), 0o700); err != nil {
				t.Fatal(err)
			}
			t.Chdir(l.runDir())
			listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: controlSocket, Net: "unix"})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if err := listener.SetDeadline(time.Now().Add(statusTimeout)); err != nil {
				t.Fatal(err)
			}
			s := &supervisor{state: test.status}
			served := make(chan error, 1)
			go func() {
				conn, err := listener.AcceptUnix()
				if err == nil {
					s.serve(conn)
				}
				served <- err
			}()
			err = waitForStart(l)
			if err := <-served; err != nil {
				t.Fatal(err)
			}
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantError) ||
				!strings.Contains(err.Error(), "tapstate-ctl logs") {
				t.Fatalf("waitForStart() = %v, want an error containing %q and a log command", err, test.wantError)
			}
		})
	}
}

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
