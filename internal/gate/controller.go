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
	direction := model.DirectionUpstream
	if command.Device == model.DeviceDownstreamGate {
		direction = model.DirectionDownstream
	}
	position := model.GateClosed
	if command.Action == "open" {
		position = model.GateOpen
	}
	if err := c.state.Observe(command.ChamberID, direction, position, c.now()); err != nil {
		return model.DeviceCommand{}, err
	}
	_, err = c.recorder.Record(ctx, command.ChamberID, model.EventCommandSettled, command.CycleID, command.Generation, command)
	return command, err
}

func (c *Controller) Observe(ctx context.Context, chamberID string, direction model.Direction, position model.GatePosition) error {
	if chamberID == "" {
		return errors.New("chamber is required")
	}
	observedAt := c.now().UTC()
	if err := c.state.Observe(chamberID, direction, position, observedAt); err != nil {
		return err
	}
	// Field telemetry is itself a committed observation: without this event a
	// restart would lose a real gate reading and fall back to "unobserved".
	reading := PositionRecord{Direction: direction, Position: position, UpdatedAt: observedAt}
	_, err := c.recorder.Record(ctx, chamberID, model.EventGateObserved, uuid.Nil, 0, reading)
	return err
}

func (c *Controller) State() *State       { return c.state }
func (c *Controller) Commands() *Commands { return c.commands }
