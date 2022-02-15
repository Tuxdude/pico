// Package pico provides the service manager used by picoinit.
package pico

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/tuxdude/zzzlogi"
	"golang.org/x/sys/unix"
)

var (
	// All the signals to monitor.
	listeningSigs = []os.Signal{
		unix.SIGHUP,    //  1
		unix.SIGINT,    //  2
		unix.SIGQUIT,   //  3
		unix.SIGILL,    //  4
		unix.SIGTRAP,   //  5
		unix.SIGABRT,   //  6
		unix.SIGIOT,    //  6
		unix.SIGBUS,    //  7
		unix.SIGFPE,    //  8
		unix.SIGKILL,   //  9
		unix.SIGUSR1,   // 10
		unix.SIGSEGV,   // 11
		unix.SIGUSR2,   // 12
		unix.SIGPIPE,   // 13
		unix.SIGALRM,   // 14
		unix.SIGTERM,   // 15
		unix.SIGSTKFLT, // 16
		unix.SIGCHLD,   // 17
		unix.SIGCONT,   // 18
		unix.SIGSTOP,   // 19
		unix.SIGTSTP,   // 20
		unix.SIGTTIN,   // 21
		unix.SIGTTOU,   // 22
		// unix.SIGURG,    // 23
		unix.SIGXCPU,   // 24
		unix.SIGXFSZ,   // 25
		unix.SIGVTALRM, // 26
		unix.SIGPROF,   // 27
		unix.SIGWINCH,  // 28
		unix.SIGIO,     // 29
		unix.SIGPWR,    // 30
		unix.SIGSYS,    // 31
	}
)

// InitServiceManager manages init responsibility in a system (by reaping
// processes which get parented to pid 1), and allows launching/managing
// services (including forwarding signals to them from the process where
// InitServiceManager is running).
type InitServiceManager interface {
	// LaunchServices launches the specified list of services.
	LaunchServices(services ...*ServiceInfo) error
	// Wait initiates the blocking wait of the init process. The
	// call doesn't return until all the services have terminated.
	// The return value indicates the final exit status code to be used.
	Wait() int
}

// ServiceInfo represents the service that will be managed by
// InitServiceManager.
type ServiceInfo struct {
	// The full path to the binary for launching this service.
	Cmd string
	// The list of command line arguments that will need to be passed
	// to the binary for launching this service.
	Args []string
}

type serviceManagerImpl struct {
	log   zzzlogi.Logger
	sigCh chan os.Signal
}

type reapedProcInfo struct {
	pid        int
	waitStatus unix.WaitStatus
}

func (r *reapedProcInfo) String() string {
	return fmt.Sprintf("{pid: %d waitStatus: %v}", r.pid, r.waitStatus)
}

// NewServiceManager instantiates an InitServiceManager along with
// performing the necessary initialization.
func NewServiceManager(log zzzlogi.Logger) (InitServiceManager, error) {
	sm := &serviceManagerImpl{
		log:   log,
		sigCh: make(chan os.Signal, 10),
	}
	go sm.signalHandler()

	// Sleep for a short duration to allow the goroutines to initialize.
	// TODO: Replace this with a channel preferably.
	time.Sleep(100 * time.Millisecond)

	return sm, nil
}

func (s *serviceManagerImpl) LaunchServices(services ...*ServiceInfo) error {
	return fmt.Errorf("not implemented")
}

func (s *serviceManagerImpl) Wait() int {
	return -1
}

func (s *serviceManagerImpl) logProcExitStatus(pid int, wstatus unix.WaitStatus) {
	exitStatus := wstatus.ExitStatus()
	if !wstatus.Exited() {
		if wstatus.Signaled() {
			s.log.Warnf(
				"Reaped zombie pid: %d was terminated by signal: %q, wstatus: %v!",
				pid,
				wstatus.Signal(),
				wstatus,
			)
		} else {
			s.log.Errorf("Reaped zombie pid: %d did not exit gracefully, wstatus: %v!", pid, wstatus)
		}
	} else {
		if exitStatus != 0 {
			s.log.Warnf(
				"Reaped zombie pid: %d, exited with exit status: %d, wstatus: %v",
				pid,
				exitStatus,
				wstatus,
			)
		} else {
			s.log.Infof(
				"Reaped zombie pid: %d, exited with exit status: %d, wstatus: %v",
				pid,
				exitStatus,
				wstatus,
			)
		}
	}
}

func (s *serviceManagerImpl) parseWait4Result(pid int, err error, wstatus unix.WaitStatus) *reapedProcInfo {
	if err == unix.ECHILD {
		// No more children, nothing further to do here.
		return nil
	}
	if err != nil {
		s.log.Errorf("Reap Zombies - Got a different unexpected error during wait: %v", err)
		return nil
	}
	if pid <= 0 {
		return nil
	}

	s.logProcExitStatus(pid, wstatus)
	return &reapedProcInfo{
		pid:        pid,
		waitStatus: wstatus,
	}
}

func (s *serviceManagerImpl) reapZombies() []*reapedProcInfo {
	s.log.Debugf("Zombie Reaper - Received signal: %q", unix.SIGCHLD)

	var result []*reapedProcInfo
	for {
		var wstatus unix.WaitStatus
		var pid int
		var err error
		err = unix.EINTR
		for err == unix.EINTR {
			pid, err = unix.Wait4(-1, &wstatus, unix.WNOHANG, nil)
		}
		proc := s.parseWait4Result(pid, err, wstatus)
		if proc == nil {
			break
		}
		result = append(result, proc)
	}
	return result
}

func (s *serviceManagerImpl) signalHandler() {
	signal.Notify(s.sigCh, listeningSigs...)
	for {
		sig, ok := <-s.sigCh
		if !ok {
			s.log.Infof("Signal handler is exiting ...")
			return
		}

		if sig == unix.SIGCHLD {
			s.reapZombies()
			// TODO: Handle the list of reaped processes.
		} else {
			go s.multicastSig(sig)
		}
	}
}

func (s *serviceManagerImpl) multicastSig(sig os.Signal) int {
	s.log.Infof("Signal Forwader - Chaining signal: %q to all services", sig)

	// TODO: Implement this.
	return 0
}
