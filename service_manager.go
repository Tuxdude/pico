package pico

import (
	"fmt"
	"time"

	"github.com/tuxdude/zzzlogi"
	"golang.org/x/sys/unix"
)

// serviceManagerImpl is the implementation of the combo init and service
// manager (aka picoinit).
type serviceManagerImpl struct {
	// Logger used by the service manager.
	log zzzlogi.Logger
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
	multiServiceMode := len(services) > 1
	sm := &serviceManagerImpl{
		log: log,
	}
	sm.repo = newServiceRepo(log)
	sm.janitor = newServiceJanitor(log, sm.repo, multiServiceMode)
	sm.signals = newSignalManager(log, sm.repo, newZombieReaper(log), sm.janitor)

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
	serv, exitStatus := s.janitor.wait()
	s.log.Infof("Shutting down since service: %v terminated", serv)

	s.shutDown()
	return exitStatus
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
