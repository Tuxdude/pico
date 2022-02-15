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

func (s *serviceManagerImpl) reapZombies() {
	s.log.Debugf("Zombie Reaper - Received signal: %q", unix.SIGCHLD)

	// TODO: Implement this.
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
