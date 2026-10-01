package gate

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Controller links physical observations, command lifecycle and the journal.
type Controller struct {
	state    *State
	commands *Commands
	recorder *journal.Recorder
	now      func() time.Time
}

func NewController(state *State, commands *Commands, recorder *journal.Recorder, now func() time.Time) *Controller {
	if now == nil {
		now = time.Now
	}
	return &Controller{state: state, commands: commands, recorder: recorder, now: now}
}

// DirectionForDevice maps a gate device to its chamber direction. Valve
// devices have no gate direction and report false.
func DirectionForDevice(device model.DeviceKind) (model.Direction, bool) {
	switch device {
	case model.DeviceUpstreamGate:
		return model.DirectionUpstream, true
	case model.DeviceDownstreamGate:
		return model.DirectionDownstream, true
	default:
		return "", false
	}
}

func (c *Controller) Request(ctx context.Context, cycle model.TransitCycle, direction model.Direction, open bool) (model.DeviceCommand, error) {
	if err := ctx.Err(); err != nil {
		return model.DeviceCommand{}, err
	}
	device := model.DeviceUpstreamGate
	if direction == model.DirectionDownstream {
		device = model.DeviceDownstreamGate
	}
	action := "close"
	position := model.GateClosing
	if open {
		action = "open"
		position = model.GateOpening
	}
	command, err := c.commands.Issue(cycle, device, action, 0, c.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if err := c.state.Observe(cycle.ChamberID, direction, position, c.now()); err != nil {
		return model.DeviceCommand{}, err
	}
	_, err = c.recorder.Record(ctx, cycle.ChamberID, model.EventCommandQueued, cycle.ID, cycle.Generation, command)
	return command, err
}

func (c *Controller) Confirm(ctx context.Context, ack model.DeviceAck) (model.DeviceCommand, error) {
	command, err := c.commands.Settle(ack, c.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	// Only gate devices produce gate observations; settling a valve command
	// must not fabricate a position for an unrelated gate.
	if direction, ok := DirectionForDevice(command.Device); ok {
		position := model.GateClosed
		if command.Action == "open" {
			position = model.GateOpen
		}
		if err := c.state.Observe(command.ChamberID, direction, position, c.now()); err != nil {
			return model.DeviceCommand{}, err
		}
	}
	_, err = c.recorder.Record(ctx, command.ChamberID, model.EventCommandSettled, command.CycleID, command.Generation, command)
	return command, err
}

// Observe applies a field-reported position and journals it so recovery
// rebuilds the same observation instead of a default.
func (c *Controller) Observe(ctx context.Context, chamberID string, direction model.Direction, position model.GatePosition) error {
	if chamberID == "" {
		return errors.New("chamber is required")
	}
	observedAt := c.now().UTC()
	if err := c.state.Observe(chamberID, direction, position, observedAt); err != nil {
		return err
	}
	observation := model.GateObservation{
		ChamberID: chamberID, Direction: direction, Position: position, ObservedAt: observedAt,
	}
	_, err := c.recorder.Record(ctx, chamberID, model.EventGateObserved, uuid.Nil, 0, observation)
	return err
}

func (c *Controller) State() *State       { return c.state }
func (c *Controller) Commands() *Commands { return c.commands }
