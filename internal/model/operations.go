package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type Vessel struct {
	ID       uuid.UUID `json:"id"`
	Name     string    `json:"name"`
	CallSign string    `json:"call_sign"`
	LengthM  float64   `json:"length_m"`
}

func NewVessel(name, callSign string, lengthM float64) (Vessel, error) {
	if name == "" || callSign == "" || lengthM <= 0 {
		return Vessel{}, errors.New("vessel name, call sign and positive length are required")
	}
	return Vessel{ID: uuid.New(), Name: name, CallSign: callSign, LengthM: lengthM}, nil
}

type ConvoyPlan struct {
	ID         uuid.UUID `json:"id"`
	ChamberID  string    `json:"chamber_id"`
	Direction  Direction `json:"direction"`
	Vessels    []Vessel  `json:"vessels"`
	CreatedAt  time.Time `json:"created_at"`
	Cancelled  bool      `json:"cancelled"`
	CancelNote string    `json:"cancel_note,omitempty"`
}

func NewConvoyPlan(chamberID string, direction Direction, vessels []Vessel, now time.Time) (ConvoyPlan, error) {
	if chamberID == "" || !direction.Valid() || len(vessels) == 0 {
		return ConvoyPlan{}, errors.New("plan requires chamber, direction and vessels")
	}
	copyOfVessels := append([]Vessel(nil), vessels...)
	return ConvoyPlan{
		ID: uuid.New(), ChamberID: chamberID, Direction: direction,
		Vessels: copyOfVessels, CreatedAt: now.UTC(),
	}, nil
}

func (p ConvoyPlan) Clone() ConvoyPlan {
	p.Vessels = append([]Vessel(nil), p.Vessels...)
	return p
}

type Lease struct {
	VesselID     uuid.UUID `json:"vessel_id"`
	Owner        string    `json:"owner"`
	FencingToken uint64    `json:"fencing_token"`
	ExpiresAt    time.Time `json:"expires_at"`
}

func (l Lease) Active(now time.Time) bool {
	return l.Owner != "" && l.FencingToken > 0 && now.Before(l.ExpiresAt)
}

type SignalAspect string

const (
	SignalStop    SignalAspect = "stop"
	SignalProceed SignalAspect = "proceed"
	SignalHold    SignalAspect = "hold"
)

type SignalState struct {
	VesselID   uuid.UUID    `json:"vessel_id"`
	CycleID    uuid.UUID    `json:"cycle_id"`
	Generation uint64       `json:"generation"`
	Aspect     SignalAspect `json:"aspect"`
	UpdatedAt  time.Time    `json:"updated_at"`
}

// SignalCompensation journals the rollback of a published signal so replay can
// rebuild the exact store state instead of guessing what was reverted.
type SignalCompensation struct {
	VesselID    uuid.UUID    `json:"vessel_id"`
	Compensated bool         `json:"compensated"`
	Restored    *SignalState `json:"restored,omitempty"`
}

type EmergencyStatus string

const (
	EmergencyIdle      EmergencyStatus = "idle"
	EmergencyDraining  EmergencyStatus = "draining"
	EmergencyCompleted EmergencyStatus = "completed"
	EmergencyFailed    EmergencyStatus = "failed"
)

type EmergencyRecord struct {
	ID          uuid.UUID       `json:"id"`
	ChamberID   string          `json:"chamber_id"`
	Status      EmergencyStatus `json:"status"`
	RequestedAt time.Time       `json:"requested_at"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	Message     string          `json:"message,omitempty"`
}
