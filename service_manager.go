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
	log              zzzlogi.Logger
	multiServiceMode bool
	sigCh            chan os.Signal

	servicesMu sync.Mutex
	services   map[int]*launchedServiceInfo
}

type launchedServiceInfo struct {
	pid     int
	service ServiceInfo
}

type reapedProcInfo struct {
	pid        int
	waitStatus unix.WaitStatus
}

func (l *launchedServiceInfo) String() string {
	return fmt.Sprintf(
		"{pid: %d cmd: %q args: %q}",
		l.pid,
		l.service.Cmd,
		l.service.Args,
	)
}

func (r *reapedProcInfo) String() string {
	return fmt.Sprintf("{pid: %d waitStatus: %v}", r.pid, r.waitStatus)
}

// NewServiceManager instantiates an InitServiceManager along with
// performing the necessary initialization.
func NewServiceManager(log zzzlogi.Logger) (InitServiceManager, error) {
	sm := &serviceManagerImpl{
		log:      log,
		sigCh:    make(chan os.Signal, 10),
		services: make(map[int]*launchedServiceInfo),
	}
	go sm.signalHandler()

	// Sleep for a short duration to allow the goroutines to initialize.
	// TODO: Replace this with a channel preferably.
	time.Sleep(100 * time.Millisecond)

	return sm, nil
}

func (s *serviceManagerImpl) LaunchServices(services ...*ServiceInfo) error {
	for _, serv := range services {
		err := s.launchService(serv.Cmd, serv.Args...)
		if err != nil {
			// TODO: Shut down any prior launched services as well
			// as other monitoring goroutines launched.
			return err
		}
	}
	return nil
}

func (s *serviceManagerImpl) Wait() int {
	return -1
}

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

func (s *serviceManagerImpl) addService(proc *launchedServiceInfo) {
	s.servicesMu.Lock()
	defer s.servicesMu.Unlock()
	if !s.multiServiceMode {
		if len(s.services) == 1 {
			s.multiServiceMode = true
		} else if len(s.services) != 0 {
			s.log.Fatalf("Number of existing services is neither 0 or 1, yet multiServiceMode is set to false! len: %d", len(s.services))
		}
	}

	s.services[proc.pid] = proc
	s.log.Debugf("Added pid: %d to the list of services", proc.pid)
}

func (s *serviceManagerImpl) handleProcTermination(procs []*reapedProcInfo) {
	for _, proc := range procs {
		s.log.Debugf("Observed reaped pid: %d wstatus: %v", proc.pid, proc.waitStatus)
		// TODO: Implement this.
	}
}

func (s *serviceManagerImpl) multicastSig(sig os.Signal) int {
	s.log.Infof("Signal Forwader - Chaining signal: %q to all services", sig)

	// TODO: Implement this.
	return 0
}

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
