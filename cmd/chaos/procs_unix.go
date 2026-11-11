//go:build unix

package main

import (
	"os"
	"syscall"
)

const canPause = true

func suspend(p *os.Process) error   { return p.Signal(syscall.SIGSTOP) }
func resume(p *os.Process) error    { return p.Signal(syscall.SIGCONT) }
func terminate(p *os.Process) error { return p.Signal(syscall.SIGTERM) }
