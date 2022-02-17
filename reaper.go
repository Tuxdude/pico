package pico

import (
	"fmt"

	"github.com/tuxdude/zzzlogi"
	"golang.org/x/sys/unix"
)

// reapedProcInfo stores information about a single reaped process.
type reapedProcInfo struct {
	// pid of the reaped process.
	pid int
	// Wait status obtained from the wait system call while reaping
	// the process.
	waitStatus unix.WaitStatus
}

// String returns the string representation of the reaped process information.
func (r *reapedProcInfo) String() string {
	return fmt.Sprintf("{pid: %d waitStatus: %v}", r.pid, r.waitStatus)
}

type zombieReaper struct {
	// Logger used by the zombie reaper.
	log zzzlogi.Logger
}

func newZombieReaper(log zzzlogi.Logger) *zombieReaper {
	return &zombieReaper{
		log: log,
	}
}

// reapZombies reaps zombie child processes if any by performing one or more
// non-blocking wait system calls, and returns once there are no further
// zombie child processes left.
func (z *zombieReaper) reap() []*reapedProcInfo {
	var result []*reapedProcInfo
	for {
		var wstatus unix.WaitStatus
		var pid int
		var err error
		err = unix.EINTR
		for err == unix.EINTR {
			pid, err = unix.Wait4(-1, &wstatus, unix.WNOHANG, nil)
		}
		proc := z.parseWait4Result(pid, err, wstatus)
		if proc == nil {
			break
		}
		result = append(result, proc)
	}
	return result
}

// logProcExitStatus logs the specified reaped process's exit status.
func (z *zombieReaper) logProcExitStatus(pid int, wstatus unix.WaitStatus) {
	exitStatus := wstatus.ExitStatus()
	if !wstatus.Exited() {
		if wstatus.Signaled() {
			z.log.Warnf(
				"Reaped zombie pid: %d was terminated by signal: %s, wstatus: %v!",
				pid,
				sigInfo(wstatus.Signal()),
				wstatus,
			)
		} else {
			z.log.Errorf("Reaped zombie pid: %d did not exit gracefully, wstatus: %v!", pid, wstatus)
		}
	} else {
		if exitStatus != 0 {
			z.log.Warnf(
				"Reaped zombie pid: %d, exited with exit status: %d, wstatus: %v",
				pid,
				exitStatus,
				wstatus,
			)
		} else {
			z.log.Infof(
				"Reaped zombie pid: %d, exited with exit status: %d, wstatus: %v",
				pid,
				exitStatus,
				wstatus,
			)
		}
	}
}

// parseWait4Result parses the wait status information to build the reaped
// process information.
func (z *zombieReaper) parseWait4Result(pid int, err error, wstatus unix.WaitStatus) *reapedProcInfo {
	if err == unix.ECHILD {
		// No more children, nothing further to do here.
		return nil
	}
	if err != nil {
		z.log.Errorf("Zombie Reaper - Got an unexpected error during wait: %v", err)
		return nil
	}
	if pid <= 0 {
		return nil
	}

	z.logProcExitStatus(pid, wstatus)
	return &reapedProcInfo{
		pid:        pid,
		waitStatus: wstatus,
	}
}
