package pico

import (
	"fmt"
	"sync"

	"github.com/tuxdude/zzzlogi"
)

const (
	stateInvalid initState = iota
	stateInitializing
	stateLaunchingServices
	stateRunning
	stateTerminatingServices
	stateShuttingDown
	stateHalted
)

var (
	stateStr = map[initState]string{
		stateInvalid:             "INVALID",
		stateInitializing:        "INITIALIZING",
		stateLaunchingServices:   "LAUNCHING_SERVICES",
		stateRunning:             "RUNNING",
		stateTerminatingServices: "TERMINATING_SERVICES",
		stateShuttingDown:        "SHUTTING_DOWN",
		stateHalted:              "HALTED",
	}
	validTransitions = map[initState]map[initState]bool{
		stateInvalid: {
			stateInitializing: true,
		},
		stateInitializing: {
			stateLaunchingServices: true,
			stateShuttingDown:      true,
		},
		stateLaunchingServices: {
			stateRunning:             true,
			stateTerminatingServices: true,
		},
		stateRunning: {
			stateTerminatingServices: true,
		},
		stateTerminatingServices: {
			stateShuttingDown: true,
		},
		stateShuttingDown: {
			stateHalted: true,
		},
		stateHalted: nil,
	}
)

type initState uint8

func (i initState) String() string {
	s, ok := stateStr[i]
	if !ok {
		panic(fmt.Errorf("initState String() - invalid init state %d", i))
	}
	return s
}

// stateMachine is a state machine for picoinit.
type stateMachine struct {
	// Logger used by state machine.
	log zzzlogi.Logger
	// Mutex for protecting access to the state field.
	mu sync.Mutex
	// Current state of picoinit.
	state initState
}

// newStateMachine instantiates a new state machine.
func newStateMachine(log zzzlogi.Logger) *stateMachine {
	return &stateMachine{
		log:   log,
		state: stateInvalid,
	}
}

func (s *stateMachine) String() string {
	return s.state.String()
}

// setState sets the specified state as the target state of
// the state machine after validating the state transition.
func (s *stateMachine) set(state initState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.validateTarget(state)
	s.log.Debugf("State Transition [%s] -> [%s]", s.state, state)
	s.state = state
}

func (s *stateMachine) validateTarget(target initState) {
	validMap, ok := validTransitions[s.state]
	if !ok {
		s.log.Fatalf("validateTransition - invalid init state %d", s.state)
	}
	isValid, ok := validMap[target]
	if !ok || !isValid {
		s.log.Fatalf("Invalid state transition, cannot transition from %s -> %s", s.state, target)
	}
}
