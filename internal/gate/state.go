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

// State keeps observed positions separate from accepted commands. Directions
// without any committed observation stay unknown instead of borrowing a
// fabricated sealed default.
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
		positions = map[model.Direction]PositionRecord{
			model.DirectionUpstream:   {Direction: model.DirectionUpstream, Position: model.GateUnknown},
			model.DirectionDownstream: {Direction: model.DirectionDownstream, Position: model.GateUnknown},
		}
		s.positions[chamberID] = positions
	}
	return positions
}

func (s *State) Observe(chamberID string, direction model.Direction, position model.GatePosition, now time.Time) error {
	if chamberID == "" || !direction.Valid() {
		return errors.New("valid chamber and direction are required")
	}
	s.mu.Lock()
	positions := s.ensure(chamberID)
	positions[direction] = PositionRecord{Direction: direction, Position: position, UpdatedAt: now.UTC()}
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
