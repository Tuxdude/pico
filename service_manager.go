// Package pico provides the service manager used by picoinit.
package pico

import (
	"fmt"

	"github.com/tuxdude/zzzlogi"
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
	log zzzlogi.Logger
}

// NewServiceManager instantiates an InitServiceManager along with
// performing the necessary initialization.
func NewServiceManager(log zzzlogi.Logger) (InitServiceManager, error) {
	sm := &serviceManagerImpl{
		log: log,
	}
	return sm, nil
}

func (s *serviceManagerImpl) LaunchServices(services ...*ServiceInfo) error {
	return fmt.Errorf("not implemented")
}

func (s *serviceManagerImpl) Wait() int {
	return -1
}
