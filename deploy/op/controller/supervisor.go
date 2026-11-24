package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const (
	controlSocket = "control.sock"
	statusTimeout = 3 * time.Second
	startTimeout  = 90 * time.Second
	stopTimeout   = 30 * time.Second
)

var (
	errStopped = errors.New("service stopped")
	errBusy    = errors.New("service control is starting or stopping")
	errCrashed = errors.New("service exited unexpectedly; use logs to inspect the cause")
)

type serviceStatus struct {
	Phase   string `json:"phase"`
	PID     int    `json:"pid,omitempty"`
	Version string `json:"version,omitempty"`
	URL     string `json:"url,omitempty"`
	Error   string `json:"error,omitempty"`
}

func printStatus(result serviceStatus, dataDir string) {
	fmt.Printf("%s (%s)", result.Phase, dataDir)
	if result.PID != 0 {
		fmt.Printf(", PID %d", result.PID)
	}
	if result.Version != "" {
		fmt.Printf(", Tapstate %s", result.Version)
	}
	fmt.Println()
	if result.URL != "" {
		fmt.Println("Open", result.URL)
	}
	if result.Error != "" {
		fmt.Println(result.Error)
	}
}

// A supervisor crash must not make a surviving Java process look stopped.
func requireNoJavaOwner(l locations) error {
	file, err := os.Open(filepath.Join(l.installation(), "owner.lock"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	region := syscall.Flock_t{Type: syscall.F_WRLCK, Whence: io.SeekStart, Start: 1, Len: 1}
	if err := syscall.FcntlFlock(file.Fd(), syscall.F_GETLK, &region); err != nil {
		return err
	}
	if region.Type != syscall.F_UNLCK {
		return fmt.Errorf("Java still owns this installation (PID %d). Its supervisor is unavailable; inspect the service logs", region.Pid)
	}
	return nil
}

func control(l locations, action string, timeout time.Duration) (serviceStatus, error) {
	var response serviceStatus
	dir, err := controlDirectory(l.runDir(), 0o077, false)
	if errors.Is(err, os.ErrNotExist) {
		if err := requireNoJavaOwner(l); err != nil {
			return response, err
		}
		return response, errStopped
	}
	if err != nil {
		return response, err
	}
	defer dir.Close()
	// Unix sockets have a small path limit. Work relative to the verified private run directory.
	if err := os.Chdir(dir.Name()); err != nil {
		return response, err
	}
	sock, err := os.Lstat(controlSocket)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return response, err
	}
	if sock != nil && (sock.Mode()&os.ModeSocket == 0 || !owned(sock)) {
		return response, errors.New("unexpected OP control socket type or owner")
	}
	conn, err := net.DialTimeout("unix", controlSocket, timeout)
	if err != nil {
		lockErr := fileLock(dir, syscall.LOCK_EX|syscall.LOCK_NB)
		if errors.Is(lockErr, syscall.EWOULDBLOCK) {
			return response, errBusy
		}
		if lockErr != nil {
			return response, lockErr
		}
		if err := requireNoJavaOwner(l); err != nil {
			return response, err
		}
		if sock != nil {
			return response, errCrashed
		}
		return response, errStopped
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return response, err
	}
	if _, err := fmt.Fprintln(conn, action); err != nil {
		return response, err
	}
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return response, err
	}
	if response.Error != "" {
		return response, errors.New(response.Error)
	}
	return response, nil
}

func stop(l locations) error {
	deadline := time.Now().Add(stopTimeout)
	for time.Now().Before(deadline) {
		_, err := control(l, "stop", time.Until(deadline))
		if errors.Is(err, errBusy) {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if errors.Is(err, errStopped) || errors.Is(err, errCrashed) {
			fmt.Printf("Stopped (%s)\n", l.DataDir)
			return nil
		}
		if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			return err
		}
		// Ensure the controller released its instance lock after waiting for its Java child.
		dir, openErr := controlDirectory(l.runDir(), 0o077, false)
		if openErr != nil {
			return openErr
		}
		defer dir.Close()
		for time.Now().Before(deadline) {
			err = fileLock(dir, syscall.LOCK_EX|syscall.LOCK_NB)
			if err == nil {
				fmt.Printf("Stopped (%s)\n", l.DataDir)
				return nil
			}
			if !errors.Is(err, syscall.EWOULDBLOCK) {
				return err
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return errors.New("stop timed out; the shutdown request may still be running. Check tapstate-ctl status and tapstate-ctl logs")
}

func start(dataDir, javaHome string) (bool, error) {
	binary, err := os.Executable()
	if err != nil {
		return false, err
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		return false, err
	}
	l := locations{DataDir: dataDir}
	guard, err := controlDirectory(l.runDir(), 0o077, true)
	if err != nil {
		return false, fmt.Errorf("%s: %v; retry with start --data-dir \"<writable directory>\"", dataDir, err)
	}
	defer guard.Close()
	err = fileLock(guard, syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return false, waitForStart(l)
	}
	if err != nil {
		return false, err
	}
	if err := requireNoJavaOwner(l); err != nil {
		return false, err
	}
	config, err := inspectPackage(binary)
	if err != nil {
		return false, fmt.Errorf("%v; re-extract the complete Tapstate OP archive", err)
	}
	config.DataDir = dataDir
	config.Java, err = javaPath(javaHome)
	if err != nil {
		return false, fmt.Errorf("%v; retry with start --java-home \"<Java 21 installation directory>\"", err)
	}
	// Transfer the acquired instance lock to the detached supervisor. No second start can
	// create another supervisor between Java validation and the first process.
	cmd := exec.Command(binary, "supervise")
	cmd.Dir = config.runDir()
	cmd.ExtraFiles = []*os.File{guard}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	input, err := cmd.StdinPipe()
	if err != nil {
		return false, err
	}
	if err := cmd.Start(); err != nil {
		input.Close()
		return false, err
	}
	err = json.NewEncoder(input).Encode(config)
	input.Close()
	guard.Close()
	if err != nil {
		cmd.Wait()
		return false, err
	}
	go func() { _ = cmd.Wait() }()
	if err := waitForStart(config); err != nil {
		return false, err
	}
	return true, nil
}

func waitForStart(l locations) error {
	for deadline := time.Now().Add(startTimeout); time.Now().Before(deadline); {
		result, err := control(l, "status", statusTimeout)
		if err == nil {
			switch result.Phase {
			case "Ready":
				printStatus(result, l.DataDir)
				return nil
			case "Waiting for storage setup", "Waiting for first administrator":
				printStatus(result, l.DataDir)
				fmt.Printf("If the wizard asks for installation authorization, use %s.\n",
					filepath.Join(l.installation(), "capability.json"))
				return nil
			case "Starting", "Stopping":
				// Keep polling until the supervisor reports an entry point or exits.
			default:
				err = fmt.Errorf("service did not start (status %q)", result.Phase)
			}
		}
		if err != nil && !errors.Is(err, errBusy) {
			return fmt.Errorf("%v; use tapstate-ctl logs --data-dir %s", err, shellQuote(l.DataDir))
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("startup timed out for %s; use tapstate-ctl status and tapstate-ctl logs", l.DataDir)
}

type supervisor struct {
	mu       sync.Mutex
	process  *os.Process
	stopping bool
	state    serviceStatus
	done     chan struct{}
}

func supervise() error {
	guard := os.NewFile(3, "supervisor lock")
	if guard == nil {
		return errors.New("missing supervisor lock")
	}
	defer guard.Close()
	var l locations
	if err := json.NewDecoder(os.Stdin).Decode(&l); err != nil {
		return err
	}
	inherited, err := guard.Stat()
	if err != nil {
		return err
	}
	working, err := os.Stat(".")
	if err != nil || !os.SameFile(inherited, working) {
		return errors.New("supervisor does not have its instance directory lock")
	}
	if err := fileLock(guard, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return err
	}
	socket, err := os.Lstat(controlSocket)
	if err == nil {
		if socket.Mode()&os.ModeSocket == 0 || !owned(socket) {
			return errors.New("unexpected OP control socket type or owner")
		}
		if err := os.Remove(controlSocket); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: controlSocket, Net: "unix"})
	if err != nil {
		return err
	}
	listener.SetUnlinkOnClose(false)
	s := &supervisor{state: serviceStatus{Phase: "Starting", Version: l.Version}, done: make(chan struct{})}
	var clients sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.AcceptUnix()
			if err != nil {
				return
			}
			clients.Add(1)
			go func() {
				defer clients.Done()
				s.serve(conn)
			}()
		}
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	go func() {
		select {
		case <-signals:
			_ = s.requestStop()
		case <-s.done:
		}
	}()
	err = s.launch(l)
	if err != nil {
		s.mu.Lock()
		s.state.Phase = "Failed"
		s.state.Error = err.Error()
		s.mu.Unlock()
		// Keep the failure available to the waiting start command. A leftover socket without a
		// live listener then distinguishes an abnormal exit from an orderly stop.
		time.Sleep(500 * time.Millisecond)
	}
	close(s.done)
	listener.Close()
	<-acceptDone
	clients.Wait()
	if err == nil {
		_ = os.Remove(controlSocket)
	}
	return err
}

func (s *supervisor) serve(conn *net.UnixConn) {
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(statusTimeout))
	request, err := bufio.NewReaderSize(conn, 32).ReadSlice('\n')
	if err != nil {
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(stopTimeout + time.Second))
	var result serviceStatus
	switch string(request) {
	case "status\n":
		s.mu.Lock()
		result = s.state
		s.mu.Unlock()
	case "stop\n":
		err := s.requestStop()
		if err != nil {
			result.Error = err.Error()
		} else {
			select {
			case <-s.done:
				result.Phase = "Stopped"
			case <-time.After(stopTimeout):
				result.Error = "stop timed out; check tapstate-ctl status and tapstate-ctl logs"
			}
		}
	default:
		result.Error = "unrecognized control command"
	}
	_ = json.NewEncoder(conn).Encode(result)
}

func (s *supervisor) requestStop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return nil
	}
	s.stopping = true
	s.state.Phase = "Stopping"
	if s.process != nil {
		if err := s.process.Signal(syscall.SIGTERM); !errors.Is(err, os.ErrProcessDone) {
			return err
		}
	}
	return nil
}

func (s *supervisor) launch(l locations) error {
	if _, err := os.Stat(l.installation()); errors.Is(err, os.ErrNotExist) {
		if err := s.runChild(l, l.javaCommand("installation", "--initialize"), false); err != nil {
			return fmt.Errorf("could not initialize %s: %w", l.installation(), err)
		}
	} else if err != nil {
		return err
	}
	for attempt := 0; attempt < 2; attempt++ {
		err := s.runChild(l, l.javaCommand("--tapstate.installation.automatic-restart=true"), true)
		s.mu.Lock()
		stopped := s.stopping
		s.mu.Unlock()
		if stopped {
			return nil
		}
		var exit *exec.ExitError
		if attempt == 0 && errors.As(err, &exit) && exit.ExitCode() == 75 {
			continue
		}
		if err == nil {
			return errors.New("Tapstate exited unexpectedly")
		}
		return fmt.Errorf("Tapstate exited: %v. Check logs; if the service port is occupied, set server.port in %s",
			err, filepath.Join(l.DataDir, "conf", "application.properties"))
	}
	return nil
}

func (s *supervisor) runChild(l locations, cmd *exec.Cmd, probe bool) error {
	stagePath := filepath.Join(l.installation(), "stage.json")
	oldStage, _ := os.Stat(stagePath)
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return nil
	}
	err := cmd.Start()
	if err == nil {
		s.process = cmd.Process
		s.state.PID = cmd.Process.Pid
		s.state.Phase = "Starting"
		s.state.URL = ""
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	var finished error
	if probe {
		finished = s.observe(stagePath, oldStage, exited)
	} else {
		finished = <-exited
	}
	s.mu.Lock()
	s.process = nil
	s.state.PID = 0
	stopped := s.stopping
	s.mu.Unlock()
	if stopped {
		return nil
	}
	return finished
}

func (s *supervisor) observe(stagePath string, oldStage os.FileInfo, exited <-chan error) error {
	client := &http.Client{
		Timeout:       2 * time.Second,
		Transport:     &http.Transport{},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer client.CloseIdleConnections()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-exited:
			return err
		case <-ticker.C:
			stat, err := os.Stat(stagePath)
			if err != nil || oldStage != nil && os.SameFile(oldStage, stat) {
				continue
			}
			var stage struct {
				ManagementURL string `json:"managementUrl"`
			}
			if readJSON(stagePath, &stage) != nil || !localManagementURL(stage.ManagementURL) {
				continue
			}
			phase, address := serviceEntry(client, stage.ManagementURL)
			if phase == "" {
				continue
			}
			s.mu.Lock()
			if !s.stopping {
				s.state.Phase = phase
				s.state.URL = address
			}
			s.mu.Unlock()
		}
	}
}

func serviceEntry(client *http.Client, url string) (string, string) {
	response, err := client.Get(url + "/auth/storage-entry")
	if err != nil {
		return "", ""
	}
	defer response.Body.Close()
	var entry struct {
		Mode           string `json:"mode"`
		Entry          string `json:"entry"`
		Management     bool   `json:"management"`
		NormalLoginURL string `json:"normalLoginUrl"`
	}
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&entry) != nil || !entry.Management || entry.Mode != "OP" {
		return "", ""
	}
	switch entry.Entry {
	case "SETUP_REQUIRED":
		return "Waiting for storage setup", url
	case "FIRST_ADMIN_REQUIRED":
		return "Waiting for first administrator", url
	case "LOGIN":
		return "Ready", first(entry.NormalLoginURL, url)
	default:
		return "", ""
	}
}
