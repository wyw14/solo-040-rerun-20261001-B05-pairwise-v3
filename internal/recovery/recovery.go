// Package recovery rebuilds every journaled runtime projection from the
// committed event stream so a restart never falls back to fabricated
// defaults: restored gate positions, queues, signals, plans, commands,
// emergency records and leases must match the events that were committed.
package recovery

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/cycle"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/lease"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/safety"
	"github.com/wyw14/cry-96/internal/schedule"
	"github.com/wyw14/cry-96/internal/signal"
)

// Projections are the derived runtime stores rebuilt during replay. Every
// projection is restored only from committed events (plus an optional
// snapshot baseline); nothing is seeded from component defaults.
type Projections struct {
	Cycles   *cycle.Coordinator
	Gates    *gate.State
	Commands *gate.Commands
	Queue    *admission.Queue
	Plans    *schedule.Planner
	Signals  *signal.Store
	Handoffs *signal.Service
	Safety   *safety.Coordinator
	Leases   *lease.Registry
}

// Service replays chamber partitions into the projections.
type Service struct {
	replayer    *journal.Replayer
	projections Projections
}

func NewService(replayer *journal.Replayer, projections Projections) *Service {
	return &Service{replayer: replayer, projections: projections}
}

// RestoreAll replays every chamber partition in deterministic order and
// returns the last committed transit cycle per chamber.
func (s *Service) RestoreAll(ctx context.Context) (map[string]model.TransitCycle, error) {
	states, err := s.replayer.ReplayAll(ctx)
	if err != nil {
		return nil, err
	}
	chamberIDs := make([]string, 0, len(states))
	for chamberID := range states {
		chamberIDs = append(chamberIDs, chamberID)
	}
	sort.Strings(chamberIDs)
	restored := make(map[string]model.TransitCycle)
	for _, chamberID := range chamberIDs {
		cycleValue, err := s.restoreChamber(states[chamberID])
		if err != nil {
			return nil, err
		}
		if cycleValue != nil {
			restored[chamberID] = cycleValue.Clone()
		}
	}
	return restored, nil
}

func (s *Service) restoreChamber(state journal.ReplayState) (*model.TransitCycle, error) {
	current, err := s.applySnapshot(state.Snapshot)
	if err != nil {
		return nil, err
	}
	for _, event := range state.Applied {
		cycleValue, err := s.apply(event)
		if err != nil {
			return nil, err
		}
		if cycleValue != nil {
			current = cycleValue
		}
	}
	return current, nil
}

// applySnapshot seeds the projections from a stored snapshot baseline; the
// events that follow it are replayed on top by restoreChamber.
func (s *Service) applySnapshot(snapshot model.ChamberSnapshot) (*model.TransitCycle, error) {
	var current *model.TransitCycle
	if snapshot.Cycle != nil {
		cycleValue := snapshot.Cycle.Clone()
		s.projections.Cycles.Restore(cycleValue)
		current = &cycleValue
	}
	for direction, position := range snapshot.Gates {
		if position == "" {
			continue
		}
		if err := s.projections.Gates.Observe(snapshot.ChamberID, direction, position, snapshot.SavedAt); err != nil {
			return nil, err
		}
	}
	for _, signalState := range snapshot.Signals {
		s.projections.Signals.Set(signalState.VesselID, signalState.CycleID, signalState.Generation, signalState.Aspect, signalState.UpdatedAt)
	}
	return current, nil
}

func (s *Service) apply(event model.Event) (*model.TransitCycle, error) {
	switch event.Type {
	case model.EventCycleCreated, model.EventCycleAdvanced:
		return s.applyCycle(event)
	case model.EventPlanCreated:
		return nil, s.applyPlanCreated(event)
	case model.EventPlanCancelled:
		return nil, s.applyPlanCancelled(event)
	case model.EventCommandQueued, model.EventCommandSettled:
		return nil, s.applyCommand(event)
	case model.EventGateObserved:
		return nil, s.applyGateObservation(event)
	case model.EventSignalChanged:
		return nil, s.applySignalChanged(event)
	case model.EventLeaseChanged:
		return nil, s.applyLeaseChanged(event)
	case model.EventEmergencyStarted, model.EventEmergencySettled:
		return nil, s.applyEmergency(event)
	default:
		return nil, nil
	}
}

func (s *Service) applyCycle(event model.Event) (*model.TransitCycle, error) {
	var decoded model.TransitCycle
	if err := event.Decode(&decoded); err != nil {
		return nil, err
	}
	if decoded.ChamberID != event.ChamberID {
		return nil, fmt.Errorf("cycle partition mismatch chamber=%s cycle=%s", event.ChamberID, decoded.ChamberID)
	}
	s.projections.Cycles.Restore(decoded)
	// A transit only reaches completed through a confirmed field handoff, so
	// the handoff log is rebuilt from the same committed transition.
	if event.Type == model.EventCycleAdvanced && decoded.Stage == model.StageCompleted {
		s.projections.Handoffs.RestoreHandoff(signal.Handoff{
			ChamberID: event.ChamberID, CycleID: decoded.ID,
			Generation: decoded.Generation, ObservedAt: event.OccurredAt,
		})
	}
	cycleCopy := decoded
	return &cycleCopy, nil
}

func (s *Service) applyPlanCreated(event model.Event) error {
	var plan model.ConvoyPlan
	if err := event.Decode(&plan); err != nil {
		return err
	}
	if plan.ID == uuid.Nil || len(plan.Vessels) == 0 {
		return nil // enqueue marker event without the plan payload
	}
	if plan.ChamberID != event.ChamberID {
		return fmt.Errorf("plan partition mismatch chamber=%s plan=%s", event.ChamberID, plan.ChamberID)
	}
	s.projections.Plans.Restore(plan)
	return s.projections.Queue.Add(event.ChamberID, plan.ID, plan.Vessels)
}

func (s *Service) applyPlanCancelled(event model.Event) error {
	var payload struct {
		Note string `json:"note"`
	}
	if err := event.Decode(&payload); err != nil {
		return err
	}
	planID := event.CycleID
	if plan, ok := s.projections.Plans.Get(planID); ok {
		plan.Cancelled = true
		plan.CancelNote = payload.Note
		s.projections.Plans.Restore(plan)
	}
	// Mirror the live cancellation: queue entries are kept for recovery with
	// their released flags cleared.
	removed := s.projections.Queue.RemovePlan(event.ChamberID, planID)
	s.projections.Queue.Restore(event.ChamberID, removed)
	return nil
}

func (s *Service) applyCommand(event model.Event) error {
	var command model.DeviceCommand
	if err := event.Decode(&command); err != nil {
		return err
	}
	if command.ChamberID != event.ChamberID {
		return fmt.Errorf("command partition mismatch chamber=%s command=%s", event.ChamberID, command.ChamberID)
	}
	s.projections.Commands.Restore(command)
	direction, ok := gate.DirectionForDevice(command.Device)
	if !ok {
		return nil // valve commands never move gate positions
	}
	position := model.GateClosing
	if event.Type == model.EventCommandSettled {
		position = model.GateClosed
	}
	if command.Action == "open" {
		position = model.GateOpening
		if event.Type == model.EventCommandSettled {
			position = model.GateOpen
		}
	}
	return s.projections.Gates.Observe(event.ChamberID, direction, position, event.OccurredAt)
}

func (s *Service) applyGateObservation(event model.Event) error {
	var observation model.GateObservation
	if err := event.Decode(&observation); err != nil {
		return err
	}
	if observation.ChamberID != event.ChamberID {
		return fmt.Errorf("observation partition mismatch chamber=%s observation=%s", event.ChamberID, observation.ChamberID)
	}
	return s.projections.Gates.Observe(event.ChamberID, observation.Direction, observation.Position, observation.ObservedAt)
}

func (s *Service) applySignalChanged(event model.Event) error {
	// signal.changed carries three payload shapes: a full SignalState from
	// the publisher, a compensation marker, and an admission Decision.
	var probe struct {
		Aspect      *model.SignalAspect `json:"aspect"`
		Compensated bool                `json:"compensated"`
		Allowed     *bool               `json:"allowed"`
	}
	if err := event.Decode(&probe); err != nil {
		return err
	}
	switch {
	case probe.Aspect != nil:
		var state model.SignalState
		if err := event.Decode(&state); err != nil {
			return err
		}
		s.projections.Signals.Set(state.VesselID, state.CycleID, state.Generation, state.Aspect, state.UpdatedAt)
	case probe.Compensated:
		var compensation model.SignalCompensation
		if err := event.Decode(&compensation); err != nil {
			return err
		}
		if compensation.Restored != nil {
			restored := *compensation.Restored
			s.projections.Signals.Set(restored.VesselID, restored.CycleID, restored.Generation, restored.Aspect, restored.UpdatedAt)
		} else {
			// Legacy markers carry no restored state; the only deterministic
			// rebuild is to clear the vessel signal.
			s.projections.Signals.Remove(compensation.VesselID)
		}
	case probe.Allowed != nil && *probe.Allowed:
		// Admission decisions journal the released vessel together with the
		// transit identity carried by the event itself.
		var decision struct {
			VesselID uuid.UUID `json:"vessel_id"`
		}
		if err := event.Decode(&decision); err != nil {
			return err
		}
		s.projections.Signals.Set(decision.VesselID, event.CycleID, event.Generation, model.SignalProceed, event.OccurredAt)
		s.projections.Queue.MarkReleased(event.ChamberID, decision.VesselID)
	}
	return nil
}

func (s *Service) applyLeaseChanged(event model.Event) error {
	var probe struct {
		Released bool `json:"released"`
	}
	if err := event.Decode(&probe); err != nil {
		return err
	}
	if probe.Released {
		var release struct {
			VesselID uuid.UUID `json:"vessel_id"`
		}
		if err := event.Decode(&release); err != nil {
			return err
		}
		s.projections.Leases.RestoreRelease(release.VesselID)
		return nil
	}
	var leaseValue model.Lease
	if err := event.Decode(&leaseValue); err != nil {
		return err
	}
	s.projections.Leases.Restore(leaseValue)
	return nil
}

func (s *Service) applyEmergency(event model.Event) error {
	var record model.EmergencyRecord
	if err := event.Decode(&record); err != nil {
		return err
	}
	if record.ChamberID != event.ChamberID {
		return fmt.Errorf("emergency partition mismatch chamber=%s record=%s", event.ChamberID, record.ChamberID)
	}
	s.projections.Safety.Restore(record)
	if event.Type == model.EventEmergencyStarted {
		// The live stop cancels every pending actuator command at this point
		// in the stream.
		s.projections.Commands.CancelChamber(event.ChamberID, event.OccurredAt)
	}
	return nil
}
