package safety

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Coordinator publishes completion only after its actuator barrier drains.
type Coordinator struct {
	mu       sync.RWMutex
	commands *gate.Commands
	barrier  *Barrier
	recorder *journal.Recorder
	records  map[string]model.EmergencyRecord
	now      func() time.Time
}

func NewCoordinator(commands *gate.Commands, barrier *Barrier, recorder *journal.Recorder, now func() time.Time) *Coordinator {
	if now == nil {
		now = time.Now
	}
	return &Coordinator{
		commands: commands, barrier: barrier, recorder: recorder,
		records: make(map[string]model.EmergencyRecord), now: now,
	}
}

func (c *Coordinator) Stop(ctx context.Context, chamberID string) (model.EmergencyRecord, error) {
	record := model.EmergencyRecord{
		ID: uuid.New(), ChamberID: chamberID, Status: model.EmergencyDraining,
		RequestedAt: c.now().UTC(), Message: "cancelling active actuator commands",
	}
	c.mu.Lock()
	c.records[chamberID] = record
	c.mu.Unlock()
	if _, err := c.recorder.Record(ctx, chamberID, model.EventEmergencyStarted, uuid.Nil, 0, record); err != nil {
		return model.EmergencyRecord{}, err
	}
	c.commands.CancelChamber(chamberID, c.now())
	if err := c.barrier.Wait(ctx, chamberID); err != nil {
		record.Status = model.EmergencyFailed
		record.Message = err.Error()
		c.mu.Lock()
		c.records[chamberID] = record
		c.mu.Unlock()
		return record, err
	}
	completedAt := c.now().UTC()
	record.Status = model.EmergencyCompleted
	record.CompletedAt = &completedAt
	record.Message = "all actuator commands settled"
	if _, err := c.recorder.Record(ctx, chamberID, model.EventEmergencySettled, uuid.Nil, 0, record); err != nil {
		return model.EmergencyRecord{}, err
	}
	c.mu.Lock()
	c.records[chamberID] = record
	c.mu.Unlock()
	return record, nil
}

func (c *Coordinator) Status(chamberID string) model.EmergencyRecord {
	c.mu.RLock()
	record := c.records[chamberID]
	c.mu.RUnlock()
	if record.ChamberID == "" {
		record = model.EmergencyRecord{ChamberID: chamberID, Status: model.EmergencyIdle}
	}
	return record
}

func (c *Coordinator) Active(chamberID string) bool {
	status := c.Status(chamberID).Status
	return status == model.EmergencyDraining
}

// Restore rehydrates an emergency record from a committed event. Events arrive
// in order, so the last record (draining/completed/failed) wins.
func (c *Coordinator) Restore(record model.EmergencyRecord) {
	c.mu.Lock()
	c.records[record.ChamberID] = record
	c.mu.Unlock()
}
