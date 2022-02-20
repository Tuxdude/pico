package pico

import (
	"fmt"
	"os"
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

// signalManagerReaper is the reaper interface used by signal manager for
// reaping processes.
type signalManagerReaper interface {
	reap() []*reapedProc
}

// reapedProcObserver is the janitor interface used by signal manager for
// notifying the janitor about new process terminations.
type reapedProcObserver func(proc []*reapedProc)

// signalManagerRepo is the repository interface used by signal manager for
// obtaining the list of running service pids.
type signalManagerRepo interface {
	pids() []int
}

// signalManager owns registering for signal notifications and handling them
// by interfacing with the service repository, the zombie reaper and the
// service janitor.
type signalManager struct {
	// Logger used by the signal manager.
	log zzzlogi.Logger
	// The channel used to receive notifications about signals from the OS.
	sigCh chan os.Signal
	// The channel used to notify that the signal handler goroutine has exited.
	sigHandlerDoneCh chan interface{}

	repo   signalManagerRepo
	reaper signalManagerReaper

	reapObserverMu sync.Mutex
	reapObserver   reapedProcObserver
}

// newSignalManager instantiates a signal manager and initiates the signal handler
// goroutine to monitor signals.
func newSignalManager(
	log zzzlogi.Logger,
	repo signalManagerRepo,
) *signalManager {
	sm := &signalManager{
		log:              log,
		sigCh:            make(chan os.Signal, 10),
		sigHandlerDoneCh: make(chan interface{}, 1),
		repo:             repo,
		reaper:           newZombieReaper(log),
	}
	sm.start()
	return sm
}

// signalHandler registers signals to get notified on, and blocks in a loop
// to receive and handle signals. If sigCh is closed, the loop terminates
// and control exits this function.
func (s *signalManager) signalHandler(readyCh chan interface{}) {
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
			go s.reap()
		} else {
			go s.multicastSig(sig)
		}
	}
}

// multicastSig forwards the specified signal to all running services in the
// repository.
func (s *signalManager) multicastSig(sig unix.Signal) int {
	pids := s.repo.pids()

	count := len(pids)
	if count > 0 {
		s.log.Infof(
			"Signal Forwader - Multicasting signal: %s to %d services",
			sigInfo(sig),
			count,
		)
	}

	for _, pid := range pids {
		err := unix.Kill(pid, sig)
		if err != nil {
			s.log.Warnf("Error sending signal: %s to pid: %d", sigInfo(sig), pid)
		}
	}
	return count
}

func (s *signalManager) setReapObserver(obs reapedProcObserver) {
	s.reapObserverMu.Lock()
	s.reapObserver = obs
	s.reapObserverMu.Unlock()
}

func (s *signalManager) clearReapObserver() {
	s.setReapObserver(nil)
}

func (s *signalManager) reap() {
	s.reapObserverMu.Lock()
	if s.reapObserver != nil {
		procs := s.reaper.reap()
		s.reapObserver(procs)
	}
	s.reapObserverMu.Unlock()
}

// start starts the signal handler goroutine and waits for
// it to initialize.
func (s *signalManager) start() {
	readyCh := make(chan interface{}, 1)
	go s.signalHandler(readyCh)
	<-readyCh
}

// shutDown gracefully shuts down the signal handler goroutine.
func (s *signalManager) shutDown() {
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

// sigInfo provides a human readable descriptive information for the
// specified signal.
func sigInfo(sig unix.Signal) string {
	return fmt.Sprintf("%s(%d){%q}", unix.SignalName(sig), sig, os.Signal(sig))
}
