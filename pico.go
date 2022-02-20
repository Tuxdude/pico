// Package pico provides the service manager used by picoinit.
package pico

import (
	"fmt"

	"github.com/tuxdude/zzzlogi"
)

// Init manages init responsibility in a system (by reaping processes which
// get parented to pid 1), and allows launching/managing services (including
// forwarding signals to them from the process where Init is running).
type Init interface {
	// Wait initiates the blocking wait of the init process. The
	// call doesn't return until all the services have terminated.
	// The return value indicates the final exit status to be used.
	Wait() int
}

// Service represents the service that will be managed by Init.
type Service struct {
	// The full path to the binary for launching this service.
	Cmd string
	// The list of command line arguments that will need to be passed
	// to the binary for launching this service.
	Args []string
}

// String returns the string representation of service information.
func (s *Service) String() string {
	return fmt.Sprintf("{Cmd: %q Args: %v}", s.Cmd, s.Args)
}

// Hook represents the command/script that will be invoked by Init
// prior to launching the services. Init will wait for the termination
// of this hook with a success exit status prior to launching the
// services.
type Hook struct {
	// The full path to the binary for this pre-launch hook.
	Cmd string
	// The list of command line arguments that will need to be passed
	// to the binary for starting this hook.
	Args []string
}

// String returns the string representation of hook.
func (s *Hook) String() string {
	return fmt.Sprintf("{Cmd: %q Args: %v}", s.Cmd, s.Args)
}

// InitConfig is the configuration used by Init.
type InitConfig struct {
	// The logger used by Init.
	Log zzzlogi.Logger
	// The pre-launch hook to be executed prior to launching the services.
	// The services will not be launched until the pre-launch hook exits
	// successfully. If the pre-launch hook exits with a non-zero exit status,
	// Init will also fail.
	PreLaunch *Hook
	// The list of services to be launched and managed by Init. Init will start
	// the shut down procedure as soon as the first service terminates. As part
	// of the shut down procedure, Init will attempt to shut down any running
	// services gracefully with a SIGTERM three times, followed by a
	// non-graceful SIGKILL to terminate them prior to exiting.
	Services []*Service
}
