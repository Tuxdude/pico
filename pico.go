// Package pico provides the service manager used by picoinit.
package pico

import "fmt"

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
