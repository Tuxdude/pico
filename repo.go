package pico

import (
	"fmt"
	"sync"

	"github.com/tuxdude/zzzlogi"
)

// launchedServiceInfo stores information about a single launched service.
type launchedServiceInfo struct {
	// pid of the launched service.
	pid int
	// Service information.
	service ServiceInfo
}

// Strings returns the string representation of launched service information.
func (l *launchedServiceInfo) String() string {
	return fmt.Sprintf(
		"{pid: %d cmd: %q args: %q}",
		l.pid,
		l.service.Cmd,
		l.service.Args,
	)
}

// serviceRepo is a service repository that allows adding, removing and
// querying services.
type serviceRepo struct {
	// Logger used by the service repository.
	log zzzlogi.Logger
	// Mutex controlling access to the service list.
	mu sync.Mutex
	// List of services.
	services map[int]*launchedServiceInfo
}

// newServiceRepo instantiates a new service repository.
func newServiceRepo(log zzzlogi.Logger) *serviceRepo {
	return &serviceRepo{
		log:      log,
		services: make(map[int]*launchedServiceInfo),
	}
}

// addService adds the specified the launched service to the service list
// in the repository.
func (s *serviceRepo) addService(proc *launchedServiceInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.services[proc.pid] = proc
	s.log.Debugf("Added pid: %d to the list of services", proc.pid)
}

// removeService removes the launched service from the service list in the
// repository based on the specified pid. If the pid doesn't match one of
// the services in the repository, the call is a no-op.
func (s *serviceRepo) removeService(pid int) (*launchedServiceInfo, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if proc, ok := s.services[pid]; ok {
		delete(s.services, pid)
		s.log.Debugf("Deleted pid: %d from the list of services", pid)
		return proc, true
	}
	return nil, false
}

// count returns the count of services in the repository.
func (s *serviceRepo) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.services)
}

// pids returns the list of pids for the services in the repository.
func (s *serviceRepo) pids() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.services) == 0 {
		return nil
	}
	pids := make([]int, len(s.services))
	i := 0
	for pid := range s.services {
		pids[i] = pid
		i++
	}
	return pids
}
