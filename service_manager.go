// Package pico provides the service manager used by picoinit.
package pico

import (
	"fmt"
	"time"

	"github.com/tuxdude/zzzlogi"
	"golang.org/x/sys/unix"
)

// InitServiceManager manages init responsibility in a system (by reaping
// processes which get parented to pid 1), and allows launching/managing
// services (including forwarding signals to them from the process where
// InitServiceManager is running).
type InitServiceManager interface {
	// Wait initiates the blocking wait of the init process. The
	// call doesn't return until all the services have terminated.
	// The return value indicates the final exit status to be used.
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

// String returns the string representation of service information.
func (s *ServiceInfo) String() string {
	return fmt.Sprintf("{Cmd: %q Args: %v}", s.Cmd, s.Args)
}

// serviceManagerImpl is the implementation of the combo init and service
// manager (aka picoinit).
type serviceManagerImpl struct {
	// Logger used by the service manager.
	log zzzlogi.Logger

	// The channel used to receive notification about the first service
	// that gets terminated.
	serviceTermNotificationCh <-chan *terminatedService

	// Zombie process reaper.
	reaper *zombieReaper
	// Service repository.
	repo *serviceRepo
	// Service janitor.
	janitor *serviceJanitor
	// Signal manager.
	signals *signalManager
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
		log: log,
	}
	sm.repo = newServiceRepo(log)
	sm.reaper = newZombieReaper(log)
	sm.janitor, sm.serviceTermNotificationCh = newServiceJanitor(log, sm.repo, multiServiceMode)
	sm.signals = newSignalManager(log, sm.repo, sm.reaper, sm.janitor)

	err := launchServices(log, sm.repo, services...)
	if err != nil {
		sm.janitor.markShutDown()
		sm.shutDown()
		return nil, fmt.Errorf("failed to launch services, reason: %v", err)
	}

	return sm, nil
}

// Wait performs a blocking wait for all the launched services to terminate.
// Once the first launched service terminates, the service manager initiates
// the shut down sequence terminating all remaining services and returns
// the exit status based on single service mode or multi service mode.
// In single service mode, the exit status is the same as that of the
// service which exited. In multi service mode, the exit status is the
// same as the first service which exited if non-zero, 77 otherwise.
func (s *serviceManagerImpl) Wait() int {
	// Wait for the first service termination.
	t := <-s.serviceTermNotificationCh

	s.log.Infof("Shutting down since service: %v terminated", t.service)

	s.shutDown()
	return t.exitStatus
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

	s.signals.shutDown()
	s.log.Infof("All services have terminated!")
}
