package pico

import (
	"fmt"
	"time"

	"github.com/tuxdude/zzzlogi"
	"golang.org/x/sys/unix"
)

// initImpl is the implementation of picoinit, the combo init and service
// manager.
type initImpl struct {
	// Logger used by init.
	log zzzlogi.Logger
	// Service repository.
	repo *serviceRepo
	// Service janitor.
	janitor *serviceJanitor
	// Signal manager.
	signals *signalManager
}

// NewInit instantiates a Pico Init/Service Manager combo, performs the
// necessary initialization for the init responsibilities, and launches the
// specified list of services.
func NewInit(log zzzlogi.Logger, services ...*Service) (Init, error) {
	multiServiceMode := len(services) > 1
	init := &initImpl{
		log: log,
	}
	init.repo = newServiceRepo(log)
	init.janitor = newServiceJanitor(log, init.repo, multiServiceMode)
	init.signals = newSignalManager(log, init.repo, newZombieReaper(log), init.janitor)

	err := launchServices(log, init.repo, services...)
	if err != nil {
		init.janitor.markShutDown()
		init.shutDown()
		return nil, fmt.Errorf("failed to launch services, reason: %v", err)
	}

	return init, nil
}

// Wait performs a blocking wait for all the launched services to terminate.
// Once the first launched service terminates, init initiates the shut down
// sequence terminating all remaining services and returns the exit status
// based on single service mode or multi service mode.
// In single service mode, the exit status is the same as that of the
// service which exited. In multi service mode, the exit status is the
// same as the first service which exited if non-zero, 77 otherwise.
func (i *initImpl) Wait() int {
	serv, exitStatus := i.janitor.wait()
	i.log.Infof("Shutting down since service: %v terminated", serv)

	i.shutDown()
	return exitStatus
}

// shutDown terminates any running services launched by Init, unregisters
// notifications for all signals, and frees up any other monitoring resources.
func (i *initImpl) shutDown() {
	sig := unix.SIGTERM
	totalAttempts := 3
	pendingTries := totalAttempts + 1
	for pendingTries > 0 {
		if pendingTries == 1 {
			sig = unix.SIGKILL
		}
		pendingTries--

		count := i.signals.multicastSig(sig)
		if count == 0 {
			break
		}
		if pendingTries > 0 {
			i.log.Infof(
				"Graceful termination Attempt [%d/%d] - Sent signal %s to %d services",
				totalAttempts+1-pendingTries,
				totalAttempts,
				sigInfo(sig),
				count,
			)
		} else {
			i.log.Infof("All graceful termination attempts exhausted, sent signal %s to %d services", sigInfo(sig), count)
		}

		sleepUntil := time.NewTimer(5 * time.Second)
		tick := time.NewTicker(10 * time.Millisecond)
		keepWaiting := true
		for keepWaiting {
			select {
			case <-tick.C:
				if i.repo.count() == 0 {
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

	i.signals.shutDown()
	i.log.Infof("All services have terminated!")
}
