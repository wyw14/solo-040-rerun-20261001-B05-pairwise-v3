package recovery_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/cycle"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/interlock"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/lease"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/recovery"
	"github.com/wyw14/cry-96/internal/safety"
	"github.com/wyw14/cry-96/internal/schedule"
	"github.com/wyw14/cry-96/internal/signal"
)

var testChambers = []string{"chamber-1", "chamber-2"}

func testNow() time.Time { return time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC) }

// world mirrors the api.Runtime component graph so tests can drive the live
// write path and then rebuild an identical graph from the journal alone.
type world struct {
	store       *journal.MemoryStore
	gates       *gate.State
	commands    *gate.Commands
	gateControl *gate.Controller
	signals     *signal.Store
	publisher   *signal.Publisher
	signalSvc   *signal.Service
	queue       *admission.Queue
	admission   *admission.Service
	planner     *schedule.Planner
	schedule    *schedule.Service
	hydraulics  *hydraulics.Controller
	cycles      *cycle.Coordinator
	cycleSvc    *cycle.Service
	safetyCtl   *safety.Coordinator
	safetySvc   *safety.Service
	leases      *lease.Registry
	leaseSvc    *lease.Service
	interlocks  *interlock.Coordinator
}

func newWorld(store journal.Store) *world {
	now := testNow
	recorder := journal.NewRecorder(store, now)
	gateState := gate.NewState(testChambers...)
	commands := gate.NewCommands()
	gateControl := gate.NewController(gateState, commands, recorder, now)
	signalStore := signal.NewStore()
	publisher := signal.NewPublisher(signalStore, recorder, now)
	signalSvc := signal.NewService(signalStore)
	queue := admission.NewQueue()
	admissionCtl := admission.NewController(queue, gateState, signalStore, now)
	admissionSvc := admission.NewService(admissionCtl, recorder, now)
	leaseRegistry := lease.NewRegistry()
	leaseSvc := lease.NewService(leaseRegistry, admissionCtl, recorder, now)
	planner := schedule.NewPlanner(admissionSvc, recorder, now)
	scheduleSvc := schedule.NewService(planner, publisher)
	hydraulicsCtl := hydraulics.NewController(commands, recorder, hydraulics.NewRamp(time.Millisecond, now), now)
	barrier := safety.NewBarrier()
	var safetyCtl *safety.Coordinator
	emergencyActive := func(chamberID string) bool {
		return safetyCtl != nil && safetyCtl.Active(chamberID)
	}
	interlocks := interlock.NewCoordinator(interlock.DefaultPolicy(), interlock.NewArbiter(), gateControl,
		func(string) float64 { return 0 }, emergencyActive)
	cycles := cycle.NewCoordinator(gateControl, hydraulicsCtl, recorder, now)
	cycleSvc := cycle.NewService(cycles, admissionSvc)
	safetyCtl = safety.NewCoordinator(commands, barrier, recorder, now)
	safetySvc := safety.NewService(safetyCtl, cycles)
	signalSvc.BindTransitLookup(cycles)
	return &world{
		gates: gateState, commands: commands, gateControl: gateControl,
		signals: signalStore, publisher: publisher, signalSvc: signalSvc,
		queue: queue, admission: admissionSvc, planner: planner, schedule: scheduleSvc,
		hydraulics: hydraulicsCtl, cycles: cycles, cycleSvc: cycleSvc,
		safetyCtl: safetyCtl, safetySvc: safetySvc,
		leases: leaseRegistry, leaseSvc: leaseSvc, interlocks: interlocks,
	}
}

func (w *world) projections() recovery.Projections {
	return recovery.Projections{
		Cycles: w.cycles, Gates: w.gates, Commands: w.commands,
		Queue: w.queue, Plans: w.planner, Signals: w.signals,
		Handoffs: w.signalSvc, Safety: w.safetyCtl, Leases: w.leases,
	}
}

func restore(t *testing.T, store journal.Store) *world {
	t.Helper()
	rebuilt := newWorld(store)
	restorer := recovery.NewService(journal.NewReplayer(store), rebuilt.projections())
	if _, err := restorer.RestoreAll(context.Background()); err != nil {
		t.Fatalf("restore: %v", err)
	}
	return rebuilt
}

func jsonOf(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(data)
}

func mustVessel(t *testing.T, name, callSign string) model.Vessel {
	t.Helper()
	vessel, err := model.NewVessel(name, callSign, 80)
	if err != nil {
		t.Fatalf("vessel: %v", err)
	}
	return vessel
}

func mustObserve(t *testing.T, w *world, chamberID string, direction model.Direction, position model.GatePosition) {
	t.Helper()
	if err := w.gateControl.Observe(context.Background(), chamberID, direction, position); err != nil {
		t.Fatalf("observe %s %s: %v", chamberID, direction, err)
	}
}

func mustAck(t *testing.T, w *world, command model.DeviceCommand) {
	t.Helper()
	ack := model.DeviceAck{
		CommandID: command.ID, CycleID: command.CycleID, ChamberID: command.ChamberID,
		Generation: command.Generation, Epoch: command.Epoch, Device: command.Device,
		Settled: true, ObservedAt: testNow(),
	}
	if err := w.cycles.ApplyGateAck(context.Background(), ack); err != nil {
		t.Fatalf("ack %s %s: %v", command.Device, command.Action, err)
	}
}

func mustPlan(t *testing.T, w *world, chamberID string, direction model.Direction, vessels []model.Vessel) (model.ConvoyPlan, model.TransitCycle) {
	t.Helper()
	plan, err := w.schedule.Create(context.Background(), chamberID, direction, vessels)
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	cycleValue, err := w.cycleSvc.Start(context.Background(), plan)
	if err != nil {
		t.Fatalf("start cycle: %v", err)
	}
	return plan, cycleValue
}

// TestRestoreMatchesLiveState drives a full lockage lifecycle plus an
// emergency on the live components, then verifies a restart rebuilds every
// projection identically from the committed events.
func TestRestoreMatchesLiveState(t *testing.T) {
	ctx := context.Background()
	store := journal.NewMemoryStore()
	live := newWorld(store)

	// chamber-1: field observations, plan, full gate lifecycle to completion.
	mustObserve(t, live, "chamber-1", model.DirectionUpstream, model.GateClosed)
	mustObserve(t, live, "chamber-1", model.DirectionDownstream, model.GateClosed)
	vessels := []model.Vessel{mustVessel(t, "River Star", "RS-204"), mustVessel(t, "Barge Two", "BT-117")}
	plan, cycleValue := mustPlan(t, live, "chamber-1", model.DirectionUpstream, vessels)

	openUp, err := live.interlocks.RequestOpen(ctx, cycleValue, model.DirectionUpstream, "test-owner")
	if err != nil {
		t.Fatalf("request open upstream: %v", err)
	}
	mustAck(t, live, openUp) // planned -> admitting
	if err := live.interlocks.Release("chamber-1", "test-owner"); err != nil {
		t.Fatalf("release reservation: %v", err)
	}
	closeUp, err := live.interlocks.RequestClose(ctx, cycleValue, model.DirectionUpstream)
	if err != nil {
		t.Fatalf("request close upstream: %v", err)
	}
	mustAck(t, live, closeUp) // admitting -> sealed
	openDown, err := live.interlocks.RequestOpen(ctx, cycleValue, model.DirectionDownstream, "test-owner")
	if err != nil {
		t.Fatalf("request open downstream: %v", err)
	}
	mustAck(t, live, openDown) // sealed -> leveling
	closeDown, err := live.interlocks.RequestClose(ctx, cycleValue, model.DirectionDownstream)
	if err != nil {
		t.Fatalf("request close downstream: %v", err)
	}
	mustAck(t, live, closeDown) // leveling -> releasing

	if _, err := live.schedule.Publish(ctx, plan.ID, cycleValue); err != nil {
		t.Fatalf("publish signals: %v", err)
	}
	if _, err := live.leaseSvc.Assign(ctx, "chamber-1", vessels[0].ID, "operator", time.Hour); err != nil {
		t.Fatalf("assign lease: %v", err)
	}
	handoff := signal.Handoff{
		ChamberID: "chamber-1", CycleID: cycleValue.ID,
		Generation: cycleValue.Generation, ObservedAt: testNow(),
	}
	if err := live.signalSvc.ConfirmHandoff(ctx, handoff); err != nil {
		t.Fatalf("handoff: %v", err)
	}

	// chamber-2: gate left mid-travel by an emergency stop.
	mustObserve(t, live, "chamber-2", model.DirectionUpstream, model.GateClosed)
	mustObserve(t, live, "chamber-2", model.DirectionDownstream, model.GateClosed)
	_, cycle2 := mustPlan(t, live, "chamber-2", model.DirectionDownstream, []model.Vessel{mustVessel(t, "Night Hawk", "NH-090")})
	if _, err := live.interlocks.RequestOpen(ctx, cycle2, model.DirectionDownstream, "owner-2"); err != nil {
		t.Fatalf("request open chamber-2: %v", err)
	}
	if _, err := live.safetySvc.EmergencyStop(ctx, "chamber-2"); err != nil {
		t.Fatalf("emergency stop: %v", err)
	}

	rebuilt := restore(t, store)

	// Cycles survive the restart with their committed stages.
	for _, chamberID := range testChambers {
		liveCycle, liveOK := live.cycles.Current(chamberID)
		rebuiltCycle, rebuiltOK := rebuilt.cycles.Current(chamberID)
		if liveOK != rebuiltOK {
			t.Fatalf("%s cycle presence live=%v rebuilt=%v", chamberID, liveOK, rebuiltOK)
		}
		if liveOK && jsonOf(t, liveCycle) != jsonOf(t, rebuiltCycle) {
			t.Errorf("%s cycle mismatch\nlive:    %s\nrebuilt: %s", chamberID, jsonOf(t, liveCycle), jsonOf(t, rebuiltCycle))
		}
	}

	// Gate observations are rebuilt, not defaulted.
	if got := jsonOf(t, live.gates.Snapshot("chamber-1")); got != jsonOf(t, rebuilt.gates.Snapshot("chamber-1")) {
		t.Errorf("chamber-1 gates mismatch live=%s rebuilt=%s", got, jsonOf(t, rebuilt.gates.Snapshot("chamber-1")))
	}
	if got := jsonOf(t, live.gates.Snapshot("chamber-2")); got != jsonOf(t, rebuilt.gates.Snapshot("chamber-2")) {
		t.Errorf("chamber-2 gates mismatch live=%s rebuilt=%s", got, jsonOf(t, rebuilt.gates.Snapshot("chamber-2")))
	}
	for _, chamberID := range testChambers {
		for _, direction := range []model.Direction{model.DirectionUpstream, model.DirectionDownstream} {
			liveRecord := live.gates.Position(chamberID, direction)
			rebuiltRecord := rebuilt.gates.Position(chamberID, direction)
			if liveRecord.Position != rebuiltRecord.Position || !liveRecord.UpdatedAt.Equal(rebuiltRecord.UpdatedAt) {
				t.Errorf("%s %s position live=%+v rebuilt=%+v", chamberID, direction, liveRecord, rebuiltRecord)
			}
		}
	}

	// The emergency-interrupted gate must not read as sealed after restart.
	if rebuilt.gates.Sealed("chamber-2") {
		t.Error("chamber-2 gates treated as sealed after restart despite a mid-travel gate")
	}
	if got := rebuilt.gates.Position("chamber-2", model.DirectionDownstream).Position; got != model.GateOpening {
		t.Errorf("chamber-2 downstream position = %s, want %s", got, model.GateOpening)
	}

	// Queue, signals, plans, commands, leases, handoffs and emergency records.
	if got, want := jsonOf(t, rebuilt.queue.List("chamber-1")), jsonOf(t, live.queue.List("chamber-1")); got != want {
		t.Errorf("chamber-1 queue mismatch\nlive:    %s\nrebuilt: %s", want, got)
	}
	if got, want := jsonOf(t, rebuilt.queue.List("chamber-2")), jsonOf(t, live.queue.List("chamber-2")); got != want {
		t.Errorf("chamber-2 queue mismatch\nlive:    %s\nrebuilt: %s", want, got)
	}
	if got, want := jsonOf(t, rebuilt.signals.List()), jsonOf(t, live.signals.List()); got != want {
		t.Errorf("signals mismatch\nlive:    %s\nrebuilt: %s", want, got)
	}
	for _, chamberID := range testChambers {
		if got, want := jsonOf(t, rebuilt.planner.Active(chamberID)), jsonOf(t, live.planner.Active(chamberID)); got != want {
			t.Errorf("%s plans mismatch\nlive:    %s\nrebuilt: %s", chamberID, want, got)
		}
		if got, want := jsonOf(t, rebuilt.commands.Pending(chamberID)), jsonOf(t, live.commands.Pending(chamberID)); got != want {
			t.Errorf("%s pending commands mismatch\nlive:    %s\nrebuilt: %s", chamberID, want, got)
		}
		if got, want := jsonOf(t, rebuilt.safetyCtl.Status(chamberID)), jsonOf(t, live.safetyCtl.Status(chamberID)); got != want {
			t.Errorf("%s emergency mismatch\nlive:    %s\nrebuilt: %s", chamberID, want, got)
		}
	}
	settled, ok := live.commands.Get(openUp.ID)
	if !ok {
		t.Fatal("live settled command missing")
	}
	rebuiltSettled, rebuiltOK := rebuilt.commands.Get(openUp.ID)
	if !rebuiltOK {
		t.Fatal("rebuilt settled command missing")
	}
	if got, want := jsonOf(t, rebuiltSettled), jsonOf(t, settled); got != want {
		t.Errorf("settled command mismatch\nlive:    %s\nrebuilt: %s", want, got)
	}
	liveLease, liveOK := live.leases.Get(vessels[0].ID)
	rebuiltLease, rebuiltOK := rebuilt.leases.Get(vessels[0].ID)
	if liveOK != rebuiltOK || jsonOf(t, liveLease) != jsonOf(t, rebuiltLease) {
		t.Errorf("lease mismatch live=%s(%v) rebuilt=%s(%v)", jsonOf(t, liveLease), liveOK, jsonOf(t, rebuiltLease), rebuiltOK)
	}
	if got, want := jsonOf(t, rebuilt.signalSvc.History()), jsonOf(t, live.signalSvc.History()); got != want {
		t.Errorf("handoff history mismatch\nlive:    %s\nrebuilt: %s", want, got)
	}

	// Epochs stay monotonic: a post-restart command never reuses a committed one.
	current, ok := rebuilt.cycles.Current("chamber-1")
	if !ok {
		t.Fatal("rebuilt cycle missing")
	}
	reissued, err := rebuilt.commands.Issue(current, model.DeviceUpstreamGate, "open", 0, testNow())
	if err != nil {
		t.Fatalf("issue after restore: %v", err)
	}
	if reissued.Epoch <= closeUp.Epoch {
		t.Errorf("reissued epoch %d does not exceed committed epoch %d", reissued.Epoch, closeUp.Epoch)
	}
}

// TestRestoreEmptyJournalLeavesGatesUnknown pins the boundary: without any
// committed observation a gate is unknown, never a fabricated sealed default.
func TestRestoreEmptyJournalLeavesGatesUnknown(t *testing.T) {
	rebuilt := restore(t, journal.NewMemoryStore())
	for _, chamberID := range testChambers {
		if rebuilt.gates.Sealed(chamberID) {
			t.Errorf("%s sealed without any committed observation", chamberID)
		}
		for _, direction := range []model.Direction{model.DirectionUpstream, model.DirectionDownstream} {
			if got := rebuilt.gates.Position(chamberID, direction).Position; got != model.GateUnknown {
				t.Errorf("%s %s = %s, want %s", chamberID, direction, got, model.GateUnknown)
			}
		}
		if len(rebuilt.queue.List(chamberID)) != 0 {
			t.Errorf("%s queue not empty after empty restore", chamberID)
		}
		if _, ok := rebuilt.cycles.Current(chamberID); ok {
			t.Errorf("%s cycle present after empty restore", chamberID)
		}
	}
	if len(rebuilt.signals.List()) != 0 {
		t.Error("signals not empty after empty restore")
	}
}

// TestRestoreLegacyCompensationMarker verifies journals written before
// compensation payloads carried the restored state still replay to a cleared
// signal instead of resurrecting a stale aspect.
func TestRestoreLegacyCompensationMarker(t *testing.T) {
	ctx := context.Background()
	store := journal.NewMemoryStore()
	live := newWorld(store)

	mustObserve(t, live, "chamber-1", model.DirectionUpstream, model.GateClosed)
	mustObserve(t, live, "chamber-1", model.DirectionDownstream, model.GateClosed)
	vessels := []model.Vessel{mustVessel(t, "River Star", "RS-204")}
	plan, cycleValue := mustPlan(t, live, "chamber-1", model.DirectionUpstream, vessels)
	if _, err := live.schedule.Publish(ctx, plan.ID, cycleValue); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Simulate a legacy journal entry: compensation marker without a
	// restored payload, plus the matching live removal.
	legacy, err := model.NewEvent("chamber-1", model.EventSignalChanged, cycleValue.ID, cycleValue.Generation, map[string]any{
		"vessel_id": vessels[0].ID, "compensated": true,
	}, testNow())
	if err != nil {
		t.Fatalf("legacy event: %v", err)
	}
	if _, err := store.Append(ctx, legacy); err != nil {
		t.Fatalf("append legacy event: %v", err)
	}
	live.signals.Remove(vessels[0].ID)

	rebuilt := restore(t, store)
	if got, want := jsonOf(t, rebuilt.signals.List()), jsonOf(t, live.signals.List()); got != want {
		t.Errorf("signals mismatch after legacy compensation\nlive:    %s\nrebuilt: %s", want, got)
	}
	if _, ok := rebuilt.signals.Get(vessels[0].ID); ok {
		t.Error("compensated signal resurrected after restart")
	}
}

// TestRestorePlanCancellation checks queue, signal, plan and cycle effects of
// a compensated cancellation survive the restart.
func TestRestorePlanCancellation(t *testing.T) {
	ctx := context.Background()
	store := journal.NewMemoryStore()
	live := newWorld(store)

	mustObserve(t, live, "chamber-1", model.DirectionUpstream, model.GateClosed)
	mustObserve(t, live, "chamber-1", model.DirectionDownstream, model.GateClosed)
	vessels := []model.Vessel{mustVessel(t, "River Star", "RS-204"), mustVessel(t, "Barge Two", "BT-117")}
	plan, cycleValue := mustPlan(t, live, "chamber-1", model.DirectionUpstream, vessels)
	if _, err := live.schedule.Publish(ctx, plan.ID, cycleValue); err != nil {
		t.Fatalf("publish: %v", err)
	}

	publication := signal.Publication{
		CycleID: cycleValue.ID, Generation: cycleValue.Generation,
		Previous: make(map[uuid.UUID]model.SignalState),
	}
	for _, vessel := range plan.Vessels {
		if _, ok := live.signals.Get(vessel.ID); ok {
			publication.Published = append(publication.Published, vessel.ID)
		}
	}
	if _, err := live.schedule.Cancel(ctx, plan.ID, "operator cancellation", publication); err != nil {
		t.Fatalf("cancel plan: %v", err)
	}
	if _, err := live.cycles.Cancel(ctx, "chamber-1", "operator cancellation"); err != nil {
		t.Fatalf("cancel cycle: %v", err)
	}

	rebuilt := restore(t, store)
	if got, want := jsonOf(t, rebuilt.queue.List("chamber-1")), jsonOf(t, live.queue.List("chamber-1")); got != want {
		t.Errorf("queue mismatch after cancellation\nlive:    %s\nrebuilt: %s", want, got)
	}
	if got, want := jsonOf(t, rebuilt.signals.List()), jsonOf(t, live.signals.List()); got != want {
		t.Errorf("signals mismatch after cancellation\nlive:    %s\nrebuilt: %s", want, got)
	}
	if got, want := jsonOf(t, rebuilt.planner.Active("chamber-1")), jsonOf(t, live.planner.Active("chamber-1")); got != want {
		t.Errorf("plans mismatch after cancellation\nlive:    %s\nrebuilt: %s", want, got)
	}
	liveCycle, _ := live.cycles.Current("chamber-1")
	rebuiltCycle, _ := rebuilt.cycles.Current("chamber-1")
	if jsonOf(t, liveCycle) != jsonOf(t, rebuiltCycle) {
		t.Errorf("cycle mismatch after cancellation\nlive:    %s\nrebuilt: %s", jsonOf(t, liveCycle), jsonOf(t, rebuiltCycle))
	}
}

// TestValveSettlementDoesNotFabricateGatePosition pins that settling a valve
// command never overwrites a real gate observation, live or after replay.
func TestValveSettlementDoesNotFabricateGatePosition(t *testing.T) {
	ctx := context.Background()
	store := journal.NewMemoryStore()
	live := newWorld(store)

	mustObserve(t, live, "chamber-1", model.DirectionUpstream, model.GateOpen)
	cycleValue, err := model.NewTransitCycle("chamber-1", uuid.New(), model.DirectionUpstream, 1, testNow())
	if err != nil {
		t.Fatalf("cycle: %v", err)
	}
	if err := live.hydraulics.IssueValve(ctx, cycleValue, 2.5); err != nil {
		t.Fatalf("issue valve: %v", err)
	}
	pending := live.commands.Pending("chamber-1")
	if len(pending) != 1 {
		t.Fatalf("pending commands = %d, want 1", len(pending))
	}
	valve := pending[0]
	ack := model.DeviceAck{
		CommandID: valve.ID, CycleID: valve.CycleID, ChamberID: valve.ChamberID,
		Generation: valve.Generation, Epoch: valve.Epoch, Device: valve.Device,
		Settled: true, ObservedAt: testNow(),
	}
	if _, err := live.gateControl.Confirm(ctx, ack); err != nil {
		t.Fatalf("confirm valve: %v", err)
	}
	if got := live.gates.Position("chamber-1", model.DirectionUpstream).Position; got != model.GateOpen {
		t.Fatalf("valve settlement overwrote gate observation: %s", got)
	}

	rebuilt := restore(t, store)
	if got := rebuilt.gates.Position("chamber-1", model.DirectionUpstream).Position; got != model.GateOpen {
		t.Errorf("replayed gate position = %s, want %s", got, model.GateOpen)
	}
	if got, want := jsonOf(t, rebuilt.gates.Snapshot("chamber-1")), jsonOf(t, live.gates.Snapshot("chamber-1")); got != want {
		t.Errorf("gates mismatch\nlive:    %s\nrebuilt: %s", want, got)
	}
}
