// Package recovery rebuilds every in-memory subsystem from the committed event
// stream. Restored state must be derivable solely from events: a restart is
// not allowed to invent field readings (gates default to "unobserved", never
// "closed") or drop queued/signal state that committed events imply.
package recovery

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/cycle"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/lease"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/safety"
	"github.com/wyw14/cry-96/internal/schedule"
)

// Recovery folds each chamber partition's snapshot and event tail back into the
// live coordinators.
type Recovery struct {
	replayer   *journal.Replayer
	cycles     *cycle.Coordinator
	gates      *gate.State
	commands   *gate.Commands
	queue      *admission.Queue
	planner    *schedule.Planner
	signals    SignalStore
	leases     *lease.Registry
	safety     *safety.Coordinator
	hydraulics *hydraulics.Controller

	// signalHistory shadows prior aspects per vessel so a compensation event
	// can restore the value the failed publication overwrote.
	signalHistory map[uuid.UUID][]model.SignalState
}

// SignalStore is the subset of signal.Store used during replay.
type SignalStore interface {
	Set(vesselID, cycleID uuid.UUID, generation uint64, aspect model.SignalAspect, now time.Time) model.SignalState
	Remove(vesselID uuid.UUID)
}

func New(
	replayer *journal.Replayer,
	cycles *cycle.Coordinator,
	gates *gate.State,
	commands *gate.Commands,
	queue *admission.Queue,
	planner *schedule.Planner,
	signals SignalStore,
	leases *lease.Registry,
	safetyCoordinator *safety.Coordinator,
	hydraulicsController *hydraulics.Controller,
) *Recovery {
	return &Recovery{
		replayer: replayer, cycles: cycles, gates: gates, commands: commands,
		queue: queue, planner: planner, signals: signals, leases: leases,
		safety: safetyCoordinator, hydraulics: hydraulicsController,
		signalHistory: make(map[uuid.UUID][]model.SignalState),
	}
}

// RestoreAll replays every chamber partition and returns the latest cycle per
// chamber that currently owns one.
func (r *Recovery) RestoreAll(ctx context.Context) (map[string]model.TransitCycle, error) {
	states, err := r.replayer.ReplayAll(ctx)
	if err != nil {
		return nil, err
	}
	restored := make(map[string]model.TransitCycle)
	for chamberID, state := range states {
		latest, err := r.restoreChamber(chamberID, state)
		if err != nil {
			return nil, err
		}
		if latest != nil {
			restored[chamberID] = *latest
		}
	}
	return restored, nil
}

func (r *Recovery) restoreChamber(chamberID string, state journal.ReplayState) (*model.TransitCycle, error) {
	// Seed from the durable snapshot first; the event tail then overtakes it.
	if cycleValue := state.Snapshot.Cycle; cycleValue != nil {
		if cycleValue.ChamberID != chamberID {
			return nil, fmt.Errorf("snapshot partition mismatch chamber=%s cycle=%s", chamberID, cycleValue.ChamberID)
		}
		r.cycles.Restore(*cycleValue)
	}
	if !state.Snapshot.SavedAt.IsZero() {
		for direction, position := range state.Snapshot.Gates {
			if !direction.Valid() {
				return nil, fmt.Errorf("snapshot gate for chamber=%s has invalid direction", chamberID)
			}
			if position.Observed() {
				if err := r.gates.Restore(chamberID, direction, position, state.Snapshot.SavedAt); err != nil {
					return nil, err
				}
			}
		}
	}
	for _, signalState := range state.Snapshot.Signals {
		r.setSignal(signalState)
	}

	var latest *model.TransitCycle
	for _, event := range state.Applied {
		if event.ChamberID != chamberID {
			return nil, fmt.Errorf("event partition mismatch chamber=%s event=%s", chamberID, event.ChamberID)
		}
		cycleValue, err := r.apply(event)
		if err != nil {
			return nil, fmt.Errorf("replay event %d (%s) chamber=%s: %w", event.Sequence, event.Type, chamberID, err)
		}
		if cycleValue != nil {
			latest = cycleValue
		}
	}
	// Re-open the leveling window only while the recovered cycle is still
	// leveling; later stages no longer accept level samples.
	if latest != nil && latest.Stage == model.StageLeveling && r.hydraulics != nil {
		if err := r.hydraulics.BeginLeveling(*latest); err != nil {
			return nil, err
		}
	}
	return latest, nil
}

// apply folds one committed event into the coordinators. It returns the cycle
// carried by cycle lifecycle events.
func (r *Recovery) apply(event model.Event) (*model.TransitCycle, error) {
	switch event.Type {
	case model.EventCycleCreated, model.EventCycleAdvanced:
		var cycleValue model.TransitCycle
		if err := event.Decode(&cycleValue); err != nil {
			return nil, err
		}
		r.cycles.Restore(cycleValue)
		return &cycleValue, nil

	case model.EventCommandQueued, model.EventCommandSettled:
		var command model.DeviceCommand
		if err := event.Decode(&command); err != nil {
			return nil, err
		}
		r.commands.Restore(command)
		// Valve commands share this event type but move no gate.
		if position, ok, err := gatePosition(command.Device, event.Type, command.Action); err != nil {
			return nil, err
		} else if ok {
			direction := model.DirectionUpstream
			if command.Device == model.DeviceDownstreamGate {
				direction = model.DirectionDownstream
			}
			if err := r.gates.Restore(command.ChamberID, direction, position, event.OccurredAt); err != nil {
				return nil, err
			}
		}
		return nil, nil

	case model.EventGateObserved:
		var reading gate.PositionRecord
		if err := event.Decode(&reading); err != nil {
			return nil, err
		}
		if !reading.Direction.Valid() {
			return nil, fmt.Errorf("gate observation for chamber=%s has invalid direction", event.ChamberID)
		}
		if err := r.gates.Restore(event.ChamberID, reading.Direction, reading.Position, reading.UpdatedAt); err != nil {
			return nil, err
		}
		return nil, nil

	case model.EventPlanCreated:
		// Two events share this type: the admission summary map (plan_id,
		// queued, at) and the full plan. Only the full plan carries the
		// vessels the queue rebuilds from.
		var plan model.ConvoyPlan
		if err := event.Decode(&plan); err != nil || plan.ID == uuid.Nil || len(plan.Vessels) == 0 {
			return nil, nil
		}
		r.planner.Restore(plan)
		if err := r.queue.Add(plan.ChamberID, plan.ID, plan.Vessels); err != nil {
			return nil, err
		}
		return nil, nil

	case model.EventPlanCancelled:
		// Plan lifecycle events carry the plan id in the cycle-id slot.
		plan, ok := r.planner.Get(event.CycleID)
		if !ok {
			return nil, fmt.Errorf("cancelled plan %s not found in stream", event.CycleID)
		}
		var detail struct {
			Note string `json:"note"`
		}
		_ = event.Decode(&detail)
		plan.Cancelled = true
		plan.CancelNote = detail.Note
		r.planner.Restore(plan)
		// Mirror Planner.Cancel: entries are removed and restored unreleased,
		// converging on the same live queue ordering.
		removed := r.queue.RemovePlan(plan.ChamberID, plan.ID)
		for index := range removed {
			removed[index].Released = false
		}
		r.queue.Restore(plan.ChamberID, removed)
		return nil, nil

	case model.EventSignalChanged:
		return nil, r.applySignal(event)

	case model.EventLeaseChanged:
		return nil, r.applyLease(event)

	case model.EventEmergencyStarted, model.EventEmergencySettled:
		var record model.EmergencyRecord
		if err := event.Decode(&record); err != nil {
			return nil, err
		}
		r.safety.Restore(record)
		return nil, nil

	default:
		// Forward-compatible events carry no state this build owns.
		return nil, nil
	}
}

func (r *Recovery) applySignal(event model.Event) error {
	var probe struct {
		VesselID    uuid.UUID          `json:"vessel_id"`
		Aspect      model.SignalAspect `json:"aspect"`
		Allowed     bool               `json:"allowed"`
		Compensated bool               `json:"compensated"`
	}
	if err := event.Decode(&probe); err != nil {
		return err
	}
	switch {
	case probe.Compensated:
		r.compensateSignal(probe.VesselID)
	case probe.Aspect != "":
		var signalState model.SignalState
		if err := event.Decode(&signalState); err != nil {
			return err
		}
		r.setSignal(signalState)
	case probe.Allowed:
		// A committed admission decision sets proceed and releases the head.
		decision := model.SignalState{
			VesselID: probe.VesselID, CycleID: event.CycleID,
			Generation: event.Generation, Aspect: model.SignalProceed, UpdatedAt: event.OccurredAt,
		}
		r.setSignal(decision)
		if !r.queue.MarkReleased(event.ChamberID, probe.VesselID) {
			return fmt.Errorf("admission decision for vessel %s has no queue entry", probe.VesselID)
		}
	}
	return nil
}

// setSignal commits an aspect to the live store and shadows it so a later
// compensation within the same replay can reinstate the prior value.
func (r *Recovery) setSignal(state model.SignalState) {
	r.signals.Set(state.VesselID, state.CycleID, state.Generation, state.Aspect, state.UpdatedAt)
	r.signalHistory[state.VesselID] = append(r.signalHistory[state.VesselID], state)
}

// compensateSignal mirrors Publisher.Compensate: drop the failed publication's
// value and restore the previous committed aspect when one exists.
func (r *Recovery) compensateSignal(vesselID uuid.UUID) {
	history := r.signalHistory[vesselID]
	if len(history) > 0 {
		history = history[:len(history)-1]
		r.signalHistory[vesselID] = history
	}
	if len(history) == 0 {
		r.signals.Remove(vesselID)
		return
	}
	previous := history[len(history)-1]
	r.signals.Set(previous.VesselID, previous.CycleID, previous.Generation, previous.Aspect, previous.UpdatedAt)
}

func (r *Recovery) applyLease(event model.Event) error {
	var probe struct {
		VesselID     uuid.UUID `json:"vessel_id"`
		Owner        string    `json:"owner"`
		FencingToken uint64    `json:"fencing_token"`
		Released     bool      `json:"released"`
	}
	if err := event.Decode(&probe); err != nil {
		return err
	}
	if probe.Released {
		r.leases.RestoreRelease(probe.VesselID)
		return nil
	}
	if probe.VesselID == uuid.Nil || probe.Owner == "" || probe.FencingToken == 0 {
		return nil
	}
	var leaseValue model.Lease
	if err := event.Decode(&leaseValue); err != nil {
		return err
	}
	r.leases.Restore(leaseValue)
	return nil
}

// gatePosition maps a committed gate command event to the field position it
// implies. Valve commands report ok=false.
func gatePosition(device model.DeviceKind, kind model.EventType, action string) (model.GatePosition, bool, error) {
	if device != model.DeviceUpstreamGate && device != model.DeviceDownstreamGate {
		return "", false, nil
	}
	switch {
	case kind == model.EventCommandQueued && action == "open":
		return model.GateOpening, true, nil
	case kind == model.EventCommandQueued:
		return model.GateClosing, true, nil
	case kind == model.EventCommandSettled && action == "open":
		return model.GateOpen, true, nil
	case kind == model.EventCommandSettled:
		return model.GateClosed, true, nil
	default:
		return "", false, fmt.Errorf("unknown gate command event/action %s/%s", kind, action)
	}
}
