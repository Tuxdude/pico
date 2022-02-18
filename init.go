package pico

import (
	"fmt"

	"github.com/tuxdude/zzzlogi"
)

// initImpl is the implementation of picoinit, the combo init and service
// manager.
type initImpl struct {
	// Logger used by init.
	log zzzlogi.Logger
	// State machine.
	state *stateMachine
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
		log:   log,
		state: newStateMachine(log),
	}

	init.state.set(stateInitializing)
	init.repo = newServiceRepo(log)
	init.signals = newSignalManager(log, init.repo, func(proc []*reapedProc) {
		init.janitor.handleProcTerminaton(proc)
	})
	init.janitor = newServiceJanitor(log, init.repo, init.signals, multiServiceMode)

	init.state.set(stateLaunchingServices)
	err := launchServices(log, init.repo, services...)
	if err != nil {
		init.shutDown()
		return nil, fmt.Errorf("failed to launch services, reason: %v", err)
	}

	init.state.set(stateRunning)
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
	i.state.set(stateTerminatingServices)
	i.janitor.shutDown()

	i.state.set(stateShuttingDown)
	i.signals.shutDown()

	i.state.set(stateHalted)
	i.log.Infof("All services have terminated!")
}
