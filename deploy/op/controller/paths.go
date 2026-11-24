package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

type fileEntry struct {
	Path string `json:"path"`
	SHA  string `json:"sha256"`
}

type packageManifest struct {
	SchemaVersion int                  `json:"schemaVersion"`
	Version       string               `json:"version"`
	Server        fileEntry            `json:"server"`
	Controllers   map[string]fileEntry `json:"controllers"`
	ConnectorLock string               `json:"connectorLockSha256"`
	Connectors    []struct {
		ID  string `json:"id"`
		SHA string `json:"sha256"`
	} `json:"connectors"`
}

type locations struct {
	PackageDir string
	DataDir    string
	Java       string
	Boot       string
	Version    string
}

func (l locations) installation() string { return filepath.Join(l.DataDir, "installation") }
func (l locations) logPath() string      { return filepath.Join(l.DataDir, "logs", "tapstate-server.log") }
func (l locations) runDir() string       { return filepath.Join(l.DataDir, "run") }

func readJSON(path string, value any) error {
	payload, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, value)
}

func inspectPackage(binary string) (locations, error) {
	var l locations
	l.PackageDir = filepath.Dir(filepath.Dir(filepath.Dir(binary)))
	var manifest packageManifest
	err := readJSON(filepath.Join(l.PackageDir, "op-manifest.json"), &manifest)
	if err != nil {
		return l, err
	}
	if manifest.SchemaVersion != 1 || !versionPattern.MatchString(manifest.Version) {
		return l, errors.New("invalid OP package manifest")
	}
	l.Version = manifest.Version
	server := "lib/app-" + l.Version + "-boot.jar"
	if manifest.Server.Path != server {
		return l, errors.New("unexpected server JAR path")
	}
	l.Boot = filepath.Join(l.PackageDir, server)
	platform := runtime.GOOS + "-" + runtime.GOARCH
	controller := manifest.Controllers[platform]
	if controller.Path != "bin/"+platform+"/tapstate-ctl" {
		return l, fmt.Errorf("this archive does not contain the %s controller", platform)
	}
	if err := verifyFile(binary, controller.SHA); err != nil {
		return l, err
	}
	if err := verifyFile(l.Boot, manifest.Server.SHA); err != nil {
		return l, err
	}
	if err := verifyFile(filepath.Join(l.PackageDir, "release", "connectors.lock.json"), manifest.ConnectorLock); err != nil {
		return l, err
	}
	seen := map[string]bool{}
	for _, connector := range manifest.Connectors {
		if !connectorPattern.MatchString(connector.ID) || seen[connector.ID] {
			return l, errors.New("invalid connector entry in OP manifest")
		}
		seen[connector.ID] = true
		if err := verifyFile(filepath.Join(l.PackageDir, "connectors", connector.ID+"-connector.jar"), connector.SHA); err != nil {
			return l, err
		}
	}
	if !seen["mongodb"] {
		return l, errors.New("MongoDB connector is missing from OP manifest")
	}
	return l, nil
}

var versionPattern = regexp.MustCompile(`^[0-9]+.[0-9]+.[0-9]+([-.][A-Za-z0-9]+)*$`)
var connectorPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
var javaVersion = regexp.MustCompile(`(?m)^[ 	]*java.specification.version = ([0-9]+)[ 	]*$`)

func verifyFile(path, expected string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, f); err != nil {
		return err
	}
	if hex.EncodeToString(digest.Sum(nil)) != expected {
		return fmt.Errorf("checksum mismatch in %s", path)
	}
	return nil
}

func javaPath(home string) (string, error) {
	java := "java"
	var err error
	if home != "" {
		java = filepath.Join(home, "bin", "java")
	} else {
		java, err = exec.LookPath(java)
		if err != nil {
			return "", err
		}
	}
	if !filepath.IsAbs(java) {
		java, err = filepath.Abs(java)
		if err != nil {
			return "", err
		}
	}
	out, err := exec.Command(java, "-XshowSettings:properties", "-version").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s: %w", java, err)
	}
	found := javaVersion.FindStringSubmatch(string(out))
	if len(found) == 0 || found[1] != "21" {
		return "", fmt.Errorf("%s did not report Java 21", java)
	}
	return java, nil
}

func configLocation(path string) string {
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(path) + "/"}).String()
}

func (l locations) javaCommand(args ...string) *exec.Cmd {
	config := "optional:" + configLocation(filepath.Join(l.PackageDir, "conf")) +
		",optional:" + configLocation(filepath.Join(l.DataDir, "conf"))
	base := []string{"--add-opens", "java.base/java.lang=ALL-UNNAMED", "-jar", l.Boot,
		"--spring.config.additional-location=" + config,
		"--tapstate.installation.directory=" + l.installation(),
		"--tapstate.connectors.seed-dir=" + filepath.Join(l.PackageDir, "connectors"),
		"--tapstate.log.dir=" + filepath.Dir(l.logPath()),
	}
	cmd := exec.Command(l.Java, append(base, args...)...)
	cmd.Dir = l.DataDir
	cmd.Env = os.Environ()
	if _, set := os.LookupEnv("TAPSTATE_BROWSER_AUTH_ALLOW_INSECURE_LOOPBACK"); !set {
		cmd.Env = append(cmd.Env, "TAPSTATE_BROWSER_AUTH_ALLOW_INSECURE_LOOPBACK=true")
	}
	return cmd
}

type preferences struct {
	SchemaVersion int    `json:"schemaVersion"`
	JavaHome      string `json:"javaHome,omitempty"`
	DataDir       string `json:"dataDir,omitempty"`
}

func accountPaths() (string, string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", "", err
	}
	if runtime.GOOS == "darwin" {
		location := filepath.Join(home, "Library", "Application Support")
		return filepath.Join(location, "tapstate-ctl", "controller.json"), filepath.Join(location, "tapstate"), nil
	}
	return filepath.Join(home, ".config", "tapstate", "controller.json"), filepath.Join(home, ".local", "share", "tapstate"), nil
}

func loadPreferences(path string) (preferences, error) {
	var p preferences
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || !owned(info) {
		return p, fmt.Errorf("unsafe permissions or owner for %s", path)
	}
	if err := readJSON(path, &p); err != nil {
		return preferences{}, err
	}
	if p.SchemaVersion != 1 ||
		(p.JavaHome != "" && !filepath.IsAbs(p.JavaHome)) || (p.DataDir != "" && !filepath.IsAbs(p.DataDir)) {
		return preferences{}, fmt.Errorf("invalid launch paths in %s", path)
	}
	return p, nil
}

func publishPreferences(path string, p preferences) error {
	p.SchemaVersion = 1
	f, err := os.CreateTemp(filepath.Dir(path), ".controller-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	err = json.NewEncoder(f).Encode(p)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

func owned(info os.FileInfo) bool {
	return int(info.Sys().(*syscall.Stat_t).Uid) == os.Geteuid()
}

func controlDirectory(path string, disallowed os.FileMode, create bool) (*os.File, error) {
	if create {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return nil, err
		}
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.IsDir() || before.Mode().Perm()&disallowed != 0 || !owned(before) {
		return nil, fmt.Errorf("unsafe permissions or owner for %s", path)
	}
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := dir.Stat()
	if err == nil && !os.SameFile(info, before) {
		err = fmt.Errorf("control directory changed: %s", path)
	}
	if err != nil {
		dir.Close()
		return nil, err
	}
	return dir, nil
}

func fileLock(directory *os.File, mode int) error {
	for {
		err := syscall.Flock(int(directory.Fd()), mode)
		if err != syscall.EINTR {
			return err
		}
	}
}

func localManagementURL(address string) bool {
	uri, err := url.Parse(address)
	if err != nil || uri.Scheme != "http" || uri.Hostname() != "127.0.0.1" ||
		uri.User != nil || uri.Path != "" || uri.RawQuery != "" || uri.Fragment != "" {
		return false
	}
	port, err := strconv.Atoi(uri.Port())
	return err == nil && port >= 1 && port <= 65535
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
