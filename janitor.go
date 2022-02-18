package pico

import (
	"sync"
	"time"

	"github.com/tuxdude/zzzlogi"
	"golang.org/x/sys/unix"
)

// Service janitor.
type serviceJanitor struct {
	// Logger used by the janitor.
	log zzzlogi.Logger
	// Service repository.
	repo janitorRepo
	// Signal manager.
	signals janitorSignalManager
	// True if more than one service is being managed by the service
	// manager, false otherwise.
	multiServiceMode bool
	// Mutex controlling access to the field shuttingDown.
	shuttingDownMu sync.Mutex
	// True if shutting down, false otherwise.
	shuttingDown bool
	// Service termination notification channel.
	termNotificationCh chan *terminatedService
}

// janitorRepo is the repository interface used by the janitor to remove
// the terminated services from the repository.
type janitorRepo interface {
	removeService(pid int) (*launchedService, bool)
	count() int
}

type janitorSignalManager interface {
	multicastSig(sig unix.Signal) int
}

// terminatedService contains information about the launched service that
// was terminated along with its exit status.
type terminatedService struct {
	service    *launchedService
	exitStatus int
}

// newServiceJanitor instantiates a new janitor.
func newServiceJanitor(
	log zzzlogi.Logger,
	repo janitorRepo,
	signals janitorSignalManager,
	multiServiceMode bool,
) *serviceJanitor {
	return &serviceJanitor{
		log:                log,
		repo:               repo,
		signals:            signals,
		multiServiceMode:   multiServiceMode,
		termNotificationCh: make(chan *terminatedService, 1),
	}
}

// handleProcTerminaton handles the termination of the specified processes.
func (s *serviceJanitor) handleProcTerminaton(procs []*reapedProc) {
	for _, proc := range procs {
		s.log.Debugf("Observed reaped pid: %d wstatus: %v", proc.pid, proc.waitStatus)
		// We could be reaping processes that weren't one of the service processes
		// we launched directly (however, likely to be one of its children).
		serv, match := s.repo.removeService(proc.pid)
		if match {
			// Only handle services that service manager cares about.
			s.handleServiceTermination(serv, proc.waitStatus.ExitStatus())
		}
	}
}

// wait waits till the first service terminates and returns the terminated
// service information along with its exit status.
func (s *serviceJanitor) wait() (*launchedService, int) {
	t := <-s.termNotificationCh
	if t == nil {
		s.log.Warnf("Service termination notification channel closed Unexpectedly, possibly indicates a bug")
		return nil, 77
	}
	return t.service, t.exitStatus
}

// handleServiceTermination handles the termination of the specified service.
func (s *serviceJanitor) handleServiceTermination(serv *launchedService, exitStatus int) {
	s.log.Infof("Service: %v exited, exit status: %d", serv, exitStatus)

	if !s.markShutDown() {
		// We are already in the middle of a shut down, nothing more to do.
		return
	}

	var resultExitStatus int
	if !s.multiServiceMode {
		// In single service mode persist the exit code same as the
		// terminated service.
		resultExitStatus = exitStatus
	} else {
		// In multi service mode calculate the exit code based on:
		//     - Use the terminated process's exit code if non-zero.
		//     - Set exit code to a pre-determined non-zero value if
		//       terminated process's exit code is zero.
		if exitStatus != 0 {
			resultExitStatus = exitStatus
		} else {
			resultExitStatus = 77
		}
	}

	// Wake up the waiter goroutine to handle the rest.
	s.termNotificationCh <- &terminatedService{
		service:    serv,
		exitStatus: resultExitStatus,
	}
	close(s.termNotificationCh)
}

// markShutDown marks shut down state within the janitor which prevents
// future notifications over the channel for service terminations.
func (s *serviceJanitor) markShutDown() bool {
	s.shuttingDownMu.Lock()
	defer s.shuttingDownMu.Unlock()
	if s.shuttingDown {
		return false
	}
	s.shuttingDown = true
	return true
}

// shutDown terminates any running services launched by Init.
func (s *serviceJanitor) shutDown() {
	s.markShutDown()

	sig := unix.SIGTERM
	totalAttempts := 3
	pendingTries := totalAttempts + 1
	for pendingTries > 0 {
		if pendingTries == 1 {
			sig = unix.SIGKILL
		}
		pendingTries--

		count := s.signals.multicastSig(sig)
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
}
