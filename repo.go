package pico

import (
	"fmt"
	"sync"

	"github.com/tuxdude/zzzlogi"
)

type serviceOrHook struct {
	cmd  string
	args []string
}

// launchedService stores information about a single launched service.
type launchedServiceOrHook struct {
	// pid of the launched service.
	pid int
	// Service or Hook information.
	entity serviceOrHook
}

// Strings returns the string representation of launched service information.
func (l *launchedServiceOrHook) String() string {
	return fmt.Sprintf(
		"{pid: %d cmd: %q args: %q}",
		l.pid,
		l.entity.cmd,
		l.entity.args,
	)
}

// serviceRepo is a service repository that allows adding, removing and
// querying services or hooks.
type serviceRepo struct {
	// Logger used by the service repository.
	log zzzlogi.Logger
	// Mutex controlling access to the service list.
	mu sync.Mutex
	// List of services or hooks.
	entities map[int]*launchedServiceOrHook
}

// newServiceRepo instantiates a new service repository.
func newServiceRepo(log zzzlogi.Logger) *serviceRepo {
	return &serviceRepo{
		log:      log,
		entities: make(map[int]*launchedServiceOrHook),
	}
}

// addService adds the specified the launched service to the service list
// in the repository.
func (s *serviceRepo) addService(proc *launchedServiceOrHook) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entities[proc.pid] = proc
	s.log.Tracef("Added pid: %d to the list of services/hooks", proc.pid)
}

// removeService removes the launched service from the service list in the
// repository based on the specified pid. If the pid doesn't match one of
// the services/hooks in the repository, the call is a no-op.
func (s *serviceRepo) removeService(pid int) (*launchedServiceOrHook, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if proc, ok := s.entities[pid]; ok {
		delete(s.entities, pid)
		s.log.Tracef("Deleted pid: %d from the list of services/hooks", pid)
		return proc, true
	}
	return nil, false
}

// count returns the count of services/hooks in the repository.
func (s *serviceRepo) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entities)
}

// pids returns the list of pids for the services/hooks in the repository.
func (s *serviceRepo) pids() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entities) == 0 {
		return nil
	}
	pids := make([]int, len(s.entities))
	i := 0
	for pid := range s.entities {
		pids[i] = pid
		i++
	}
	return pids
}
