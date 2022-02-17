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

// serviceManagerImpl is the implementation of the combo init and service
// manager (aka picoinit).
type serviceManagerImpl struct {
	// Logger used by the service manager.
	log zzzlogi.Logger
	// The channel used to receive notifications about signals from the OS.
	sigCh chan os.Signal
	// The channel used to notify that the signal handler goroutine has exited.
	sigHandlerDoneCh chan interface{}
	// The channel used to receive notification about the first service
	// that gets terminated.
	serviceTermNotificationCh <-chan *terminatedService

	// Zombie process reaper.
	reaper *zombieReaper
	// Service repository.
	repo *serviceRepo
	// Service janitor.
	janitor *serviceJanitor
}

func sigInfo(sig unix.Signal) string {
	return fmt.Sprintf("%s(%d){%q}", unix.SignalName(sig), sig, os.Signal(sig))
}

// String returns the string representation of service information.
func (s *ServiceInfo) String() string {
	return fmt.Sprintf("{Cmd: %q Args: %v}", s.Cmd, s.Args)
}

// NewServiceManager instantiates an InitServiceManager, performs the
// necessary initialization for the init responsibilities, and launches the
// specified list of services.
func NewServiceManager(log zzzlogi.Logger, services ...*ServiceInfo) (InitServiceManager, error) {
	multiServiceMode := false
	if len(services) > 1 {
		multiServiceMode = true
	}

	sm := &serviceManagerImpl{
		log:              log,
		sigCh:            make(chan os.Signal, 10),
		sigHandlerDoneCh: make(chan interface{}, 1),
	}
	sm.reaper = newZombieReaper(log)
	sm.repo = newServiceRepo(log)
	sm.janitor, sm.serviceTermNotificationCh = newServiceJanitor(log, sm.repo, multiServiceMode)

	readyCh := make(chan interface{}, 1)
	go sm.signalHandler(readyCh)
	<-readyCh

	err := launchServices(log, sm.repo, services...)
	if err != nil {
		sm.shutDown()
		return nil, fmt.Errorf("failed to launch services, reason: %v", err)
	}

	return sm, nil
}

// Wait performs a blocking wait for all the launched services to terminate.
// Once the first launched service terminates, the service manager initiates
// the shut down sequence terminating all remaining services and returns
// the exit status code based on single service mode or multi service mode.
// In single service mode, the exit status code is the same as that of the
// service which exited. In multi service mode, the exit status code is the
// same as the first service which exited if non-zero, 77 otherwise.
func (s *serviceManagerImpl) Wait() int {
	// Wait for the first service termination.
	t := <-s.serviceTermNotificationCh

	s.log.Infof("Shutting down since service: %v terminated", t.service)

	s.shutDown()
	return t.exitStatus
}

// signalHandler registers signals to get notified on, and blocks in a loop
// to receive and handle signals. If sigCh is closed, the loop terminates
// and control exits this function.
func (s *serviceManagerImpl) signalHandler(readyCh chan interface{}) {
	signal.Notify(s.sigCh, listeningSigs...)
	readyCh <- nil
	close(readyCh)

	for {
		osSig, ok := <-s.sigCh
		if !ok {
			s.log.Debugf("Signal handler is exiting ...")
			s.sigHandlerDoneCh <- nil
			close(s.sigHandlerDoneCh)
			return
		}

		sig := osSig.(unix.Signal)
		s.log.Debugf("Signal Handler received %s", sigInfo(sig))
		if sig == unix.SIGCHLD {
			procs := s.reaper.reap()
			go s.janitor.handleProcTermination(procs)
		} else {
			go s.multicastSig(sig)
		}
	}
}

// multicastSig forwards the specified signal to all running services managed by
// the service manager.
func (s *serviceManagerImpl) multicastSig(sig unix.Signal) int {
	pids := s.repo.pids()

	count := len(pids)
	if count > 0 {
		s.log.Infof(
			"Signal Forwader - Multicasting signal: %s to %d services",
			sigInfo(sig),
			count,
		)
	}

	for _, pid := range pids {
		err := unix.Kill(pid, sig)
		if err != nil {
			s.log.Warnf("Error sending signal: %s to pid: %d", sigInfo(sig), pid)
		}
	}
	return count
}

// shutDown terminates any running services launched by InitServiceManager,
// unregisters notifications for all signals, and frees up any other
// monitoring resources.
func (s *serviceManagerImpl) shutDown() {
	sig := unix.SIGTERM
	totalAttempts := 3
	pendingTries := totalAttempts + 1
	for pendingTries > 0 {
		if pendingTries == 1 {
			sig = unix.SIGKILL
		}
		pendingTries--

		count := s.multicastSig(sig)
		if count == 0 {
			break
		}
		if pendingTries > 0 {
			s.log.Infof(
				"Graceful termination Attempt [%d/%d] - Sent signal %s to %d services",
				totalAttempts+1-pendingTries,
				totalAttempts,
				sigInfo(sig),
				count,
			)
		} else {
			s.log.Infof("All graceful termination attempts exhausted, sent signal %s to %d services", sigInfo(sig), count)
		}

		sleepUntil := time.NewTimer(5 * time.Second)
		tick := time.NewTicker(10 * time.Millisecond)
		keepWaiting := true
		for keepWaiting {
			select {
			case <-tick.C:
				if s.repo.count() == 0 {
					keepWaiting = false
					pendingTries = 0
				}
			case <-sleepUntil.C:
				keepWaiting = false
			}
		}
		sleepUntil.Stop()
		tick.Stop()
	}

	s.shutDownSignalHandler()
	s.log.Infof("All services have terminated!")
}

// shutDownSignalHandler gracefully shuts down the signal handler goroutine.
func (s *serviceManagerImpl) shutDownSignalHandler() {
	signal.Reset()
	close(s.sigCh)

	// Wait for the signal handler goroutine to exit gracefully
	// within a period of 100ms after which we give up and exit
	// anyway since the rest of the clean up is complete.
	timeout := time.NewTimer(100 * time.Millisecond)
	select {
	case <-s.sigHandlerDoneCh:
		s.log.Debugf("Signal handler has exited")
	case <-timeout.C:
		s.log.Debugf("Signal handler did not exit, giving up and proceeding with termination")
	}
	timeout.Stop()
}
