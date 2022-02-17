// Package pico provides the service manager used by picoinit.
package pico

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"sync"
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
	// Logger used by the service manager.
	log zzzlogi.Logger
	// True if more than one service is being managed by the service
	// manager, false otherwise.
	multiServiceMode bool
	// Final exit status code to return.
	finalExitCode int
	// The channel used to receive notifications about signals from the OS.
	sigCh chan os.Signal
	// The channel used to notify that the signal handler goroutine has exited.
	sigHandlerDoneCh chan interface{}
	// The channel used to receive notification about the first service
	// that gets terminated.
	serviceTermWaiterCh chan *launchedServiceInfo
	// Zombie process reaper.
	reaper *zombieReaper

	// Mutex controlling access to the field shuttingDown.
	stateMu sync.Mutex
	// True if shutting down, false otherwise.
	shuttingDown bool

	// Mutex controlling access to the field services.
	servicesMu sync.Mutex
	// List of services.
	services map[int]*launchedServiceInfo
}

// launchedServiceInfo stores information about a single launched service.
type launchedServiceInfo struct {
	// pid of the launched service.
	pid int
	// Service information.
	service ServiceInfo
}

func sigInfo(sig unix.Signal) string {
	return fmt.Sprintf("%s(%d){%q}", unix.SignalName(sig), sig, os.Signal(sig))
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
		reaper:              newZombieReaper(log),
		sigCh:               make(chan os.Signal, 10),
		sigHandlerDoneCh:    make(chan interface{}, 1),
		serviceTermWaiterCh: make(chan *launchedServiceInfo, 1),
		services:            make(map[int]*launchedServiceInfo),
	}

	readyCh := make(chan interface{}, 1)
	go sm.signalHandler(readyCh)
	<-readyCh

	err := sm.launchServices(services...)
	if err != nil {
		return nil, fmt.Errorf("failed to launch services, reason: %v", err)
	}

	return sm, nil
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

	s.log.Infof("Shutting down since service: %v terminated", serv)

	s.shutDown()
	return s.finalExitCode
}

// signalHandler registers signals to get notified on, and blocks in a loop
// to receive and handle signals. If sigCh is closed, the loop terminates
// and control exits this function.
func (s *serviceManagerImpl) signalHandler(readyCh chan interface{}) {
	signal.Notify(s.sigCh, listeningSigs...)
	readyCh <- nil
	close(readyCh)

	for {
		osSig, ok := <-s.sigCh
		if !ok {
			s.log.Debugf("Signal handler is exiting ...")
			s.sigHandlerDoneCh <- nil
			close(s.sigHandlerDoneCh)
			return
		}

		sig := osSig.(unix.Signal)
		s.log.Debugf("Signal Handler received %s", sigInfo(sig))
		if sig == unix.SIGCHLD {
			procs := s.reaper.reap()
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

	if !s.markShutDown() {
		// We are already in the middle of a shut down, nothing more to do.
		return
	}

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

func (s *serviceManagerImpl) markShutDown() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.shuttingDown {
		return false
	}
	s.shuttingDown = true
	return true
}

// multicastSig forwards the specified signal to all running services managed by
// the service manager.
func (s *serviceManagerImpl) multicastSig(sig unix.Signal) int {
	s.servicesMu.Lock()
	defer s.servicesMu.Unlock()

	count := len(s.services)
	if count > 0 {
		s.log.Infof(
			"Signal Forwader - Multicasting signal: %s to %d services",
			sigInfo(sig),
			count,
		)
	}

	for pid := range s.services {
		err := unix.Kill(pid, sig)
		if err != nil {
			s.log.Warnf("Error sending signal: %s to pid: %d", sigInfo(sig), pid)
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

// attachStdinTTY attaches the TTY to the process with the specified pid.
func (s *serviceManagerImpl) attachStdinTTY(pid int) error {
	err := unix.IoctlSetPointerInt(unix.Stdin, unix.TIOCSPGRP, pid)
	if err == nil {
		s.log.Debugf("Attached TTY of stdin to pid: %d", pid)
		return nil
	}

	var errNo unix.Errno
	if errors.As(err, &errNo) {
		if errNo == unix.ENOTTY {
			s.log.Infof("No stdin TTY found to attach, ignoring")
			return nil
		}
		return fmt.Errorf("tcsetpgrp failed attempting to attach stdin TTY, errno: %v", errNo)
	}
	return fmt.Errorf("tcsetpgrp failed attempting to attach stdin TTY, reason: %T %v", err, err)
}

// launchService launches the specified service binary invoking it with the
// specified list of command line arguments.
func (s *serviceManagerImpl) launchService(bin string, args ...string) error {
	cmd := exec.Command(bin, args...)
	cmd.SysProcAttr = &unix.SysProcAttr{
		// Use a new process group for the child.
		Setpgid: true,
	}
	if !s.multiServiceMode {
		// Only in single service mode we redirect stdin to the
		// one and only service that is being launched.
		cmd.Stdin = os.Stdin
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to launch service %q %q, reason: %v", bin, args, err)
	}

	if !s.multiServiceMode {
		// Only in single service mode, attach the stdin TTY (if present) to
		// the one and only service launched.
		err := s.attachStdinTTY(cmd.Process.Pid)
		if err != nil {
			return err
		}
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
		s.log.Infof("Sent signal %s to %d services", sigInfo(sig), count)

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

	s.shutDownSignalHandler()
	s.log.Infof("All services have terminated!")
}

// shutDownSignalHandler gracefully shuts down the signal handler goroutine.
func (s *serviceManagerImpl) shutDownSignalHandler() {
	signal.Reset()
	close(s.sigCh)

	// Wait for the signal handler goroutine to exit gracefully
	// within a period of 100ms after which we give up and exit
	// anyway since the rest of the clean up is complete.
	timeout := time.NewTimer(100 * time.Millisecond)
	select {
	case <-s.sigHandlerDoneCh:
		s.log.Debugf("Signal handler has exited")
	case <-timeout.C:
		s.log.Debugf("Signal handler did not exit, giving up and proceeding with termination")
	}
	timeout.Stop()
}
