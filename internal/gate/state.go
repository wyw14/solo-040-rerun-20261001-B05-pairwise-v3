package gate

import (
	"errors"
	"sync"
	"time"

	"github.com/wyw14/cry-96/internal/model"
)

type PositionRecord struct {
	Direction model.Direction    `json:"direction"`
	Position  model.GatePosition `json:"position"`
	UpdatedAt time.Time          `json:"updated_at"`
}

// State keeps observed positions separate from accepted commands.
type State struct {
	mu        sync.RWMutex
	positions map[string]map[model.Direction]PositionRecord
}

func NewState(chambers ...string) *State {
	state := &State{positions: make(map[string]map[model.Direction]PositionRecord)}
	for _, chamberID := range chambers {
		state.ensure(chamberID)
	}
	return state
}

func (s *State) ensure(chamberID string) map[model.Direction]PositionRecord {
	positions, ok := s.positions[chamberID]
	if !ok {
		// Default to unobserved. A restart must not invent a "closed" reading:
		// until a committed event supplies a position, the gate state is unknown.
		positions = map[model.Direction]PositionRecord{
			model.DirectionUpstream:   {Direction: model.DirectionUpstream, Position: model.GateUnobserved},
			model.DirectionDownstream: {Direction: model.DirectionDownstream, Position: model.GateUnobserved},
		}
		s.positions[chamberID] = positions
	}
	return positions
}

func (s *State) Observe(chamberID string, direction model.Direction, position model.GatePosition, now time.Time) error {
	if chamberID == "" || !direction.Valid() {
		return errors.New("valid chamber and direction are required")
	}
	if !position.Observed() {
		return errors.New("observed gate position must be a real field reading")
	}
	s.mu.Lock()
	positions := s.ensure(chamberID)
	positions[direction] = PositionRecord{Direction: direction, Position: position, UpdatedAt: now.UTC()}
	s.mu.Unlock()
	return nil
}

// Restore rehydrates a gate position exclusively from a committed event or
// snapshot. The recovery folds inputs in committed order, so each call is a
// later reading and simply replaces the previous one; no timestamp tie can
// discard a settled state the way a clock-based guard might.
func (s *State) Restore(chamberID string, direction model.Direction, position model.GatePosition, observedAt time.Time) error {
	if chamberID == "" || !direction.Valid() {
		return errors.New("valid chamber and direction are required")
	}
	if !position.Observed() {
		return errors.New("restored gate position must be a committed field reading")
	}
	s.mu.Lock()
	positions := s.ensure(chamberID)
	positions[direction] = PositionRecord{Direction: direction, Position: position, UpdatedAt: observedAt.UTC()}
	s.mu.Unlock()
	return nil
}

func (s *State) Position(chamberID string, direction model.Direction) PositionRecord {
	s.mu.RLock()
	positions := s.positions[chamberID]
	record := positions[direction]
	s.mu.RUnlock()
	return record
}

func (s *State) Sealed(chamberID string) bool {
	s.mu.RLock()
	positions := s.positions[chamberID]
	upstream := positions[model.DirectionUpstream]
	downstream := positions[model.DirectionDownstream]
	s.mu.RUnlock()
	return upstream.Position.Sealed() && downstream.Position.Sealed()
}

func (s *State) Snapshot(chamberID string) map[model.Direction]model.GatePosition {
	s.mu.RLock()
	positions := s.positions[chamberID]
	result := map[model.Direction]model.GatePosition{
		model.DirectionUpstream:   positions[model.DirectionUpstream].Position,
		model.DirectionDownstream: positions[model.DirectionDownstream].Position,
	}
	s.mu.RUnlock()
	return result
}
