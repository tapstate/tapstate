package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "tapstate-ctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: tapstate-ctl start|stop|restart|status|logs [options]")
	}
	if args[0] == "supervise" {
		return supervise()
	}
	action := args[0]
	launch := action == "start" || action == "restart"
	if !launch && action != "stop" && action != "status" && action != "logs" {
		return fmt.Errorf("unknown command %q; use start, stop, restart, status or logs", action)
	}
	options := flag.NewFlagSet(action, flag.ContinueOnError)
	dataArg := options.String("data-dir", "", "Tapstate work directory")
	javaArg := new(string)
	if launch {
		javaArg = options.String("java-home", "", "Java 21 installation directory")
	}
	follow := new(bool)
	if action == "logs" {
		follow = options.Bool("follow", false, "follow the service log")
	}
	if err := options.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if options.NArg() != 0 {
		return errors.New("unexpected arguments")
	}
	explicitData, explicitJava := *dataArg, *javaArg
	var err error
	if explicitData != "" {
		explicitData, err = filepath.Abs(explicitData)
		if err != nil {
			return err
		}
	}
	if explicitJava != "" {
		explicitJava, err = filepath.Abs(explicitJava)
		if err != nil {
			return err
		}
	}
	prefPath, defaultData, err := accountPaths()
	if err != nil {
		return err
	}
	save := launch && (explicitJava != "" || explicitData != "")
	if save {
		lock, err := controlDirectory(filepath.Dir(prefPath), 0o022, true)
		if err != nil {
			return err
		}
		defer lock.Close()
		if err := fileLock(lock, syscall.LOCK_EX); err != nil {
			return err
		}
	}
	saved, err := loadPreferences(prefPath)
	if err != nil && !(launch && explicitData != "" && explicitJava != "") {
		return fmt.Errorf("cannot read %s: %v; run start with both --java-home and --data-dir to recover", prefPath, err)
	}
	l := locations{DataDir: first(explicitData, saved.DataDir, defaultData)}
	if launch {
		if err := os.MkdirAll(l.DataDir, 0o700); err != nil {
			return fmt.Errorf("%s: %v; retry with start --data-dir \"<writable directory>\"", l.DataDir, err)
		}
	}
	if real, err := filepath.EvalSymlinks(l.DataDir); err == nil {
		l.DataDir = real
	}
	if action == "logs" {
		return logs(l, *follow)
	}
	if action == "status" {
		result, err := control(l, "status", statusTimeout)
		if errors.Is(err, errStopped) {
			fmt.Printf("Stopped (%s)\n", l.DataDir)
			return nil
		}
		if err == nil {
			printStatus(result, l.DataDir)
		}
		return err
	}
	if action == "stop" {
		return stop(l)
	}
	if action == "restart" {
		if err := stop(l); err != nil {
			return err
		}
	}
	started, err := start(l.DataDir, first(explicitJava, saved.JavaHome, os.Getenv("JAVA_HOME")))
	if err != nil {
		return err
	}
	if started && save {
		if explicitJava != "" {
			saved.JavaHome = explicitJava
		}
		if explicitData != "" {
			saved.DataDir = l.DataDir
		}
		if err := publishPreferences(prefPath, saved); err != nil {
			stopErr := stop(l)
			return errors.Join(fmt.Errorf("cannot save %s: %w", prefPath, err), stopErr)
		}
	}
	return nil
}

func first(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func logs(l locations, follow bool) error {
	if _, err := os.Stat(l.logPath()); err != nil {
		return err
	}
	args := []string{"-n", "100"}
	if follow {
		args = append(args, "-F")
	}
	cmd := exec.Command("tail", append(args, l.logPath())...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
