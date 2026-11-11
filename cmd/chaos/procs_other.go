//go:build !unix

package main

import (
	"errors"
	"os"
)

// The harness is meant to run in the Linux container. On other systems it
// still builds, but cannot pause workers and stops them with a hard kill.
const canPause = false

func suspend(*os.Process) error     { return errors.New("SIGSTOP is not available on this OS") }
func resume(*os.Process) error      { return nil }
func terminate(p *os.Process) error { return p.Kill() }
