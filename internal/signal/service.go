package signal

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

type TransitLookup interface {
	Current(string) (model.TransitCycle, bool)
	ApplySignalHandoff(context.Context, uuid.UUID, uint64) error
}

type Handoff struct {
	ChamberID  string
	CycleID    uuid.UUID
	Generation uint64
	ObservedAt time.Time
}

// Service validates field handoffs before asking cycle to advance.
type Service struct {
	mu       sync.RWMutex
	store    *Store
	lookup   TransitLookup
	handoffs []Handoff
}

func NewService(store *Store) *Service { return &Service{store: store} }

func (s *Service) BindTransitLookup(lookup TransitLookup) { s.lookup = lookup }

func (s *Service) ConfirmHandoff(ctx context.Context, handoff Handoff) error {
	if s.lookup == nil {
		return errors.New("transit lookup is not bound")
	}
	current, ok := s.lookup.Current(handoff.ChamberID)
	if !ok {
		return errors.New("no active transit for chamber")
	}
	if current.Stage.Terminal() {
		return errors.New("completed transit cannot be reopened")
	}
	if !current.IdentityMatches(handoff.CycleID, handoff.Generation) {
		return errors.New("handoff belongs to another transit generation")
	}
	if err := s.lookup.ApplySignalHandoff(ctx, handoff.CycleID, handoff.Generation); err != nil {
		return err
	}
	s.mu.Lock()
	s.handoffs = append(s.handoffs, handoff)
	s.mu.Unlock()
	return nil
}

func (s *Service) History() []Handoff {
	s.mu.RLock()
	result := append([]Handoff(nil), s.handoffs...)
	s.mu.RUnlock()
	return result
}

// RestoreHandoff appends a journaled handoff during replay without
// re-validating against the transit lookup.
func (s *Service) RestoreHandoff(handoff Handoff) {
	s.mu.Lock()
	s.handoffs = append(s.handoffs, handoff)
	s.mu.Unlock()
}

func (s *Service) States() []model.SignalState { return s.store.List() }
