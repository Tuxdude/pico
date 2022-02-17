// Package pico provides the service manager used by picoinit.
package pico

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/tuxdude/zzzlogi"
	"golang.org/x/sys/unix"
)

var (
	// All the signals to monitor.
	listeningSigs = []os.Signal{
		unix.SIGHUP,    //  1
		unix.SIGINT,    //  2
		unix.SIGQUIT,   //  3
		unix.SIGILL,    //  4
		unix.SIGTRAP,   //  5
		unix.SIGABRT,   //  6
		unix.SIGIOT,    //  6
		unix.SIGBUS,    //  7
		unix.SIGFPE,    //  8
		unix.SIGKILL,   //  9
		unix.SIGUSR1,   // 10
		unix.SIGSEGV,   // 11
		unix.SIGUSR2,   // 12
		unix.SIGPIPE,   // 13
		unix.SIGALRM,   // 14
		unix.SIGTERM,   // 15
		unix.SIGSTKFLT, // 16
		unix.SIGCHLD,   // 17
		unix.SIGCONT,   // 18
		unix.SIGSTOP,   // 19
		unix.SIGTSTP,   // 20
		unix.SIGTTIN,   // 21
		unix.SIGTTOU,   // 22
		// unix.SIGURG,    // 23
		unix.SIGXCPU,   // 24
		unix.SIGXFSZ,   // 25
		unix.SIGVTALRM, // 26
		unix.SIGPROF,   // 27
		unix.SIGWINCH,  // 28
		unix.SIGIO,     // 29
		unix.SIGPWR,    // 30
		unix.SIGSYS,    // 31
	}
)

// InitServiceManager manages init responsibility in a system (by reaping
// processes which get parented to pid 1), and allows launching/managing
// services (including forwarding signals to them from the process where
// InitServiceManager is running).
type InitServiceManager interface {
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

// serviceManagerImpl is the implementation of the combo init and service
// manager (aka picoinit).
type serviceManagerImpl struct {
	log                 zzzlogi.Logger
	shuttingDown        bool
	multiServiceMode    bool
	finalExitCode       int
	sigCh               chan os.Signal
	serviceTermWaiterCh chan *launchedServiceInfo

	servicesMu sync.Mutex
	services   map[int]*launchedServiceInfo
}

// launchedServiceInfo stores information about a single launched service.
type launchedServiceInfo struct {
	// pid of the launched service.
	pid int
	// Service information.
	service ServiceInfo
}

// reapedProcInfo stores information about a single reaped process.
type reapedProcInfo struct {
	// pid of the reaped process.
	pid int
	// Wait status obtained from the wait system call while reaping
	// the process.
	waitStatus unix.WaitStatus
}

// String returns the string representation of service information.
func (s *ServiceInfo) String() string {
	return fmt.Sprintf("{Cmd: %q Args: %v}", s.Cmd, s.Args)
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

// String returns the string representation of the reaped process information.
func (r *reapedProcInfo) String() string {
	return fmt.Sprintf("{pid: %d waitStatus: %v}", r.pid, r.waitStatus)
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
		log:                 log,
		multiServiceMode:    multiServiceMode,
		finalExitCode:       77,
		sigCh:               make(chan os.Signal, 10),
		serviceTermWaiterCh: make(chan *launchedServiceInfo, 1),
		services:            make(map[int]*launchedServiceInfo),
	}
	go sm.signalHandler()

	// Sleep for a short duration to allow the goroutines to initialize.
	// TODO: Replace this with a channel preferably.
	time.Sleep(100 * time.Millisecond)

	err := sm.launchServices(services...)
	if err != nil {
		return nil, fmt.Errorf("failed to launch services, reason: %v", err)
	}

	return sm, nil
}

// launchServices launches the specified list of services.
func (s *serviceManagerImpl) launchServices(services ...*ServiceInfo) error {
	for _, serv := range services {
		err := s.launchService(serv.Cmd, serv.Args...)
		if err != nil {
			s.shutDown()
			return err
		}
	}
	return nil
}

// Wait performs a blocking wait for all the launched services to terminate.
// Once the first launched service terminates, the service manager initiates
// the shut down sequence terminating all remaining services and returns
// the exit status code based on single service mode or multi service mode.
// In single service mode, the exit status code is the same as that of the
// service which exited. In multi service mode, the exit status code is the
// same as the first service which exited if non-zero, 77 otherwise.
func (s *serviceManagerImpl) Wait() int {
	// Wait for the first service termination.
	serv := <-s.serviceTermWaiterCh
	close(s.serviceTermWaiterCh)

	s.log.Infof("Cleaning up since service: %v terminated", serv)

	s.shutDown()
	return s.finalExitCode
}

// logProcExitStatus logs the specified process's exit status information.
func (s *serviceManagerImpl) logProcExitStatus(pid int, wstatus unix.WaitStatus) {
	exitStatus := wstatus.ExitStatus()
	if !wstatus.Exited() {
		if wstatus.Signaled() {
			s.log.Warnf(
				"Reaped zombie pid: %d was terminated by signal: %q, wstatus: %v!",
				pid,
				wstatus.Signal(),
				wstatus,
			)
		} else {
			s.log.Errorf("Reaped zombie pid: %d did not exit gracefully, wstatus: %v!", pid, wstatus)
		}
	} else {
		if exitStatus != 0 {
			s.log.Warnf(
				"Reaped zombie pid: %d, exited with exit status: %d, wstatus: %v",
				pid,
				exitStatus,
				wstatus,
			)
		} else {
			s.log.Infof(
				"Reaped zombie pid: %d, exited with exit status: %d, wstatus: %v",
				pid,
				exitStatus,
				wstatus,
			)
		}
	}
}

// parseWait4Result parses the wait status information to build the reaped
// process information.
func (s *serviceManagerImpl) parseWait4Result(pid int, err error, wstatus unix.WaitStatus) *reapedProcInfo {
	if err == unix.ECHILD {
		// No more children, nothing further to do here.
		return nil
	}
	if err != nil {
		s.log.Errorf("Reap Zombies - Got a different unexpected error during wait: %v", err)
		return nil
	}
	if pid <= 0 {
		return nil
	}

	s.logProcExitStatus(pid, wstatus)
	return &reapedProcInfo{
		pid:        pid,
		waitStatus: wstatus,
	}
}

// reapZombies reaps zombie child processes if any by performing one or more
// non-blocking wait system calls, and returns once there are no further
// zombie child processes left.
func (s *serviceManagerImpl) reapZombies() []*reapedProcInfo {
	s.log.Debugf("Zombie Reaper - Received signal: %q", unix.SIGCHLD)

	var result []*reapedProcInfo
	for {
		var wstatus unix.WaitStatus
		var pid int
		var err error
		err = unix.EINTR
		for err == unix.EINTR {
			pid, err = unix.Wait4(-1, &wstatus, unix.WNOHANG, nil)
		}
		proc := s.parseWait4Result(pid, err, wstatus)
		if proc == nil {
			break
		}
		result = append(result, proc)
	}
	return result
}

// signalHandler registers signals to get notified on, and blocks in a loop
// to receive and handle signals. If sigCh is closed, the loop terminates
// and control exits this function.
func (s *serviceManagerImpl) signalHandler() {
	signal.Notify(s.sigCh, listeningSigs...)
	for {
		sig, ok := <-s.sigCh
		if !ok {
			s.log.Infof("Signal handler is exiting ...")
			return
		}

		if sig == unix.SIGCHLD {
			procs := s.reapZombies()
			go s.handleProcTermination(procs)
		} else {
			go s.multicastSig(sig)
		}
	}
}

// addService adds the specified the launched service to the service list.
func (s *serviceManagerImpl) addService(proc *launchedServiceInfo) {
	s.servicesMu.Lock()
	defer s.servicesMu.Unlock()

	s.services[proc.pid] = proc
	s.log.Debugf("Added pid: %d to the list of services", proc.pid)
}

// removeService removes the launched service from the service list based on
// the specified pid. If the pid doesn't match one of the launched services,
// the call is a no-op.
func (s *serviceManagerImpl) removeService(pid int) (*launchedServiceInfo, bool) {
	s.servicesMu.Lock()
	defer s.servicesMu.Unlock()
	if proc, ok := s.services[pid]; ok {
		delete(s.services, pid)
		s.log.Debugf("Deleted pid: %d from the list of services", pid)
		return proc, true
	}
	return nil, false
}

// handleProcTermination handles the termination of the specified processes.
func (s *serviceManagerImpl) handleProcTermination(procs []*reapedProcInfo) {
	for _, proc := range procs {
		s.log.Debugf("Observed reaped pid: %d wstatus: %v", proc.pid, proc.waitStatus)
		// We could be reaping processes that weren't one of the service processes
		// we launched directly (however, likely to be one of its children).
		serv, match := s.removeService(proc.pid)
		if match {
			// Only handle services that service manager cares about.
			s.handleServiceTermination(serv, proc.waitStatus.ExitStatus())
		}
	}
}

// handleServiceTermination handles the termination of the specified service.
func (s *serviceManagerImpl) handleServiceTermination(serv *launchedServiceInfo, exitStatus int) {
	s.log.Infof("Service: %v exited, finalExitCode: %d", serv, exitStatus)
	if s.shuttingDown {
		// We are already in the middle of a shut down, nothing more to do.
		return
	}
	s.shuttingDown = true

	if !s.multiServiceMode {
		// In single service mode persist the exit code same as the
		// terminated service.
		s.finalExitCode = exitStatus
	} else {
		// In multi service mode calculate the exit code based on:
		//     - Use the terminated process's exit code if non-zero.
		//     - Set exit code to a pre-determined non-zero value if
		//       terminated process's exit code is zero.
		if exitStatus != 0 {
			s.finalExitCode = exitStatus
		} else {
			s.finalExitCode = 77
		}
	}

	// Wake up the waiter goroutine to handle the rest.
	s.serviceTermWaiterCh <- serv
}

// multicastSig forwards the specified signal to all running services managed by
// the service manager.
func (s *serviceManagerImpl) multicastSig(sig os.Signal) int {
	s.servicesMu.Lock()
	defer s.servicesMu.Unlock()

	count := len(s.services)
	if count > 0 {
		s.log.Infof("Signal Forwader - Chaining signal: %q to %d services", sig, count)
	}

	for pid := range s.services {
		err := unix.Kill(pid, sig.(unix.Signal))
		if err != nil {
			s.log.Warnf("Error sending signal: %s to pid: %d", sig, pid)
		}
	}
	return count
}

// serviceCount returns the count of running services managed by the
// service manager.
func (s *serviceManagerImpl) serviceCount() int {
	s.servicesMu.Lock()
	defer s.servicesMu.Unlock()
	return len(s.services)
}

// launchService launches the specified service binary invoking it with the
// specified list of command line arguments.
func (s *serviceManagerImpl) launchService(bin string, args ...string) error {
	cmd := exec.Command(bin, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		// Use a new process group for the child.
		Setpgid: true,
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to launch service %q %q, reason: %v", bin, args, err)
	}

	proc := &launchedServiceInfo{}
	proc.pid = cmd.Process.Pid
	proc.service.Cmd = bin
	proc.service.Args = make([]string, len(args))
	copy(proc.service.Args, args)
	s.addService(proc)

	s.log.Infof("Launched service %q pid: %d", bin, proc.pid)
	return nil
}

// shutDown terminates any running services launched by InitServiceManager,
// unregisters notifications for all signals, and frees up any other
// monitoring resources.
func (s *serviceManagerImpl) shutDown() {
	sig := unix.SIGTERM
	pendingTries := 4
	for pendingTries > 0 {
		if pendingTries == 1 {
			sig = unix.SIGKILL
		}
		pendingTries--

		count := s.multicastSig(sig)
		if count == 0 {
			break
		}
		s.log.Infof("Sent signal %q to %d services", sig, count)

		sleepUntil := time.NewTimer(5 * time.Second)
		tick := time.NewTicker(10 * time.Millisecond)
		keepWaiting := true
		for keepWaiting {
			select {
			case <-tick.C:
				if s.serviceCount() == 0 {
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

	signal.Reset()
	close(s.sigCh)
	// The tiny sleep will allow the goroutines to exit.
	// TODO: Replace this by a WaitGroup instead.
	time.Sleep(10 * time.Millisecond)

	s.log.Infof("All services have terminated!")
}
