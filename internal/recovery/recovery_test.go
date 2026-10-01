package recovery

import (
	"context"
	"testing"
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
	"github.com/wyw14/cry-96/internal/signal"
)

const chamber = "chamber-1"

type rig struct {
	store    journal.Store
	now      func() time.Time
	recorder *journal.Recorder
	gates    *gate.State
	commands *gate.Commands
	gateCtrl *gate.Controller
	queue    *admission.Queue
	planner  *schedule.Planner
	signals  *signal.Store
	leases   *lease.Registry
	hydr     *hydraulics.Controller
	cycles   *cycle.Coordinator
	safetyC  *safety.Coordinator
	recovery *Recovery
}

func newRig(t *testing.T, store journal.Store) *rig {
	t.Helper()
	now := func() time.Time { return time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC) }
	recorder := journal.NewRecorder(store, now)
	gates := gate.NewState(chamber)
	commands := gate.NewCommands()
	gateCtrl := gate.NewController(gates, commands, recorder, now)
	signals := signal.NewStore()
	queue := admission.NewQueue()
	admissionCtrl := admission.NewController(queue, gates, signals, now)
	admissionSvc := admission.NewService(admissionCtrl, recorder, now)
	planner := schedule.NewPlanner(admissionSvc, recorder, now)
	leases := lease.NewRegistry()
	ramp := hydraulics.NewRamp(time.Millisecond, now)
	hydr := hydraulics.NewController(commands, recorder, ramp, now)
	cycles := cycle.NewCoordinator(gateCtrl, hydr, recorder, now)
	safetyC := safety.NewCoordinator(commands, safety.NewBarrier(), recorder, now)
	restorer := New(journal.NewReplayer(store), cycles, gates, commands, queue, planner,
		signals, leases, safetyC, hydr)
	return &rig{
		store: store, now: now, recorder: recorder, gates: gates, commands: commands,
		gateCtrl: gateCtrl, queue: queue, planner: planner, signals: signals,
		leases: leases, hydr: hydr, cycles: cycles, safetyC: safetyC, recovery: restorer,
	}
}

func TestRecoversGateQueueSignalAndCycle(t *testing.T) {
	ctx := context.Background()
	store := journal.NewMemoryStore()

	// --- first "process": build live state through committed events ---
	first := newRig(t, store)

	// Default gate state is unknown and must not count as sealed.
	if first.gates.Sealed(chamber) {
		t.Fatal("fresh gate state must not report sealed before any observation")
	}
	if pos := first.gates.Snapshot(chamber)[model.DirectionUpstream]; pos != model.GateUnobserved {
		t.Fatalf("default upstream position = %q, want unobserved", pos)
	}
	if err := first.gateCtrl.Observe(ctx, chamber, model.DirectionUpstream, model.GateClosed); err != nil {
		t.Fatal(err)
	}
	if err := first.gateCtrl.Observe(ctx, chamber, model.DirectionDownstream, model.GateClosed); err != nil {
		t.Fatal(err)
	}

	vessel, err := model.NewVessel("MV Aries", "WXY1", 80)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := first.planner.Create(ctx, chamber, model.DirectionUpstream, []model.Vessel{vessel})
	if err != nil {
		t.Fatal(err)
	}
	vessel = plan.Vessels[0]

	// Start a cycle; the service path also admits the head vessel, committing a
	// proceed signal and releasing the queue entry.
	cycleSvc := cycle.NewService(first.cycles, admission.NewService(
		admission.NewController(first.queue, first.gates, first.signals, first.now),
		first.recorder, first.now))
	cycleValue, err := cycleSvc.Start(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}

	// Open the upstream gate; commit queued then settled (advancing stage).
	if _, err := first.gateCtrl.Request(ctx, cycleValue, model.DirectionUpstream, true); err != nil {
		t.Fatal(err)
	}
	pending := first.commands.Pending(chamber)
	if len(pending) != 1 {
		t.Fatalf("pending commands = %d, want 1", len(pending))
	}
	ack := model.DeviceAck{
		CommandID: pending[0].ID, CycleID: pending[0].CycleID, ChamberID: chamber,
		Generation: pending[0].Generation, Epoch: pending[0].Epoch, Device: pending[0].Device,
		Settled: true, ObservedAt: first.now(),
	}
	if err := first.cycles.ApplyGateAck(ctx, ack); err != nil {
		t.Fatal(err)
	}
	if pos := first.gates.Snapshot(chamber)[model.DirectionUpstream]; pos != model.GateOpen {
		t.Fatalf("upstream after settle = %q, want open", pos)
	}

	// --- second "process": fresh components, same durable store ---
	second := newRig(t, store)
	if _, err := second.recovery.RestoreAll(ctx); err != nil {
		t.Fatal(err)
	}

	restoredCycle, ok := second.cycles.Current(chamber)
	if !ok {
		t.Fatal("cycle not recovered")
	}
	if restoredCycle.ID != cycleValue.ID || restoredCycle.Stage != model.StageAdmitting {
		t.Fatalf("recovered cycle id=%s stage=%s, want %s admitting", restoredCycle.ID, restoredCycle.Stage, cycleValue.ID)
	}

	// Upstream must be the committed open reading, NOT a default closed gate.
	if pos := second.gates.Snapshot(chamber)[model.DirectionUpstream]; pos != model.GateOpen {
		t.Fatalf("recovered upstream gate = %q, want open (must not reset to closed)", pos)
	}
	if pos := second.gates.Snapshot(chamber)[model.DirectionDownstream]; pos != model.GateClosed {
		t.Fatalf("recovered downstream gate = %q, want closed", pos)
	}

	entries := second.queue.List(chamber)
	if len(entries) != 1 {
		t.Fatalf("recovered queue len = %d, want 1", len(entries))
	}
	if !entries[0].Released {
		t.Fatal("recovered queue entry lost its released mark")
	}

	states := second.signals.List()
	if len(states) != 1 || states[0].Aspect != model.SignalProceed || states[0].VesselID != vessel.ID {
		t.Fatalf("recovered signals = %+v, want one proceed for the admitted vessel", states)
	}

	if got := len(second.commands.Pending(chamber)); got != 0 {
		t.Fatalf("recovered pending commands = %d, want 0 (command was settled)", got)
	}
	if _, ok := second.commands.Get(pending[0].ID); !ok {
		t.Fatal("settled command was not recovered")
	}

	// Epoch fencing counter must not reuse the settled command's epoch.
	if _, err := second.gateCtrl.Request(ctx, restoredCycle, model.DirectionUpstream, false); err != nil {
		t.Fatal(err)
	}
	if epoch := second.commands.Pending(chamber)[0].Epoch; epoch <= pending[0].Epoch {
		t.Fatalf("post-restart epoch %d must exceed recovered epoch %d", epoch, pending[0].Epoch)
	}
}

func TestUnobservedChamberStaysUnobservedAfterRecovery(t *testing.T) {
	ctx := context.Background()
	store := journal.NewMemoryStore()
	// A partition with events but no gate observations must recover with
	// unobserved gates, never default closed/sealed.
	if _, err := journal.NewRecorder(store, time.Now).
		Record(ctx, chamber, model.EventLeaseChanged, uuid.Nil, 0, map[string]any{"note": "bootstrap"}); err != nil {
		t.Fatal(err)
	}
	r := newRig(t, store)
	if _, err := r.recovery.RestoreAll(ctx); err != nil {
		t.Fatal(err)
	}
	for direction := range map[model.Direction]struct{}{
		model.DirectionUpstream: {}, model.DirectionDownstream: {},
	} {
		if got := r.gates.Snapshot(chamber)[direction]; got != model.GateUnobserved {
			t.Fatalf("recovered %s gate = %q, want unobserved", direction, got)
		}
	}
	if r.gates.Sealed(chamber) {
		t.Fatal("chamber with no gate observations must not report sealed")
	}
}
