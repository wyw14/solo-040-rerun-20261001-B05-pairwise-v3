package lease

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// Registry owns monotonically increasing fencing tokens per vessel.
type Registry struct {
	mu        sync.RWMutex
	leases    map[uuid.UUID]model.Lease
	nextToken map[uuid.UUID]uint64
}

func NewRegistry() *Registry {
	return &Registry{
		leases:    make(map[uuid.UUID]model.Lease),
		nextToken: make(map[uuid.UUID]uint64),
	}
}

func (r *Registry) Assign(vesselID uuid.UUID, owner string, ttl time.Duration, now time.Time) (model.Lease, error) {
	if vesselID == uuid.Nil || owner == "" || ttl <= 0 {
		return model.Lease{}, errors.New("assignment requires vessel, owner and positive ttl")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextToken[vesselID]++
	lease := model.Lease{
		VesselID: vesselID, Owner: owner, FencingToken: r.nextToken[vesselID],
		ExpiresAt: now.UTC().Add(ttl),
	}
	r.leases[vesselID] = lease
	return lease, nil
}

func (r *Registry) Renew(vesselID uuid.UUID, owner string, token uint64, ttl time.Duration, now time.Time) (model.Lease, error) {
	if ttl <= 0 {
		return model.Lease{}, errors.New("renewal ttl must be positive")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	lease, ok := r.leases[vesselID]
	if !ok {
		return model.Lease{}, fmt.Errorf("no lease for vessel %s", vesselID)
	}
	if lease.Owner != owner || lease.FencingToken != token {
		return model.Lease{}, errors.New("lease owner or fencing token is stale")
	}
	if !lease.Active(now) {
		return model.Lease{}, errors.New("lease has expired")
	}
	lease.ExpiresAt = now.UTC().Add(ttl)
	r.leases[vesselID] = lease
	return lease, nil
}

func (r *Registry) Get(vesselID uuid.UUID) (model.Lease, bool) {
	r.mu.RLock()
	lease, ok := r.leases[vesselID]
	r.mu.RUnlock()
	return lease, ok
}

func (r *Registry) Release(vesselID uuid.UUID, owner string, token uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	lease, ok := r.leases[vesselID]
	if !ok || lease.Owner != owner || lease.FencingToken != token {
		return false
	}
	delete(r.leases, vesselID)
	return true
}

func (r *Registry) Snapshot() []model.Lease {
	r.mu.RLock()
	result := make([]model.Lease, 0, len(r.leases))
	for _, lease := range r.leases {
		result = append(result, lease)
	}
	r.mu.RUnlock()
	return result
}

// Restore rehydrates a lease from a committed event and seeds the fencing
// token counter so post-restart assignments never reuse a token.
func (r *Registry) Restore(lease model.Lease) {
	r.mu.Lock()
	r.leases[lease.VesselID] = lease
	if lease.FencingToken > r.nextToken[lease.VesselID] {
		r.nextToken[lease.VesselID] = lease.FencingToken
	}
	r.mu.Unlock()
}

// RestoreRelease rehydrates a committed lease release.
func (r *Registry) RestoreRelease(vesselID uuid.UUID) {
	r.mu.Lock()
	delete(r.leases, vesselID)
	r.mu.Unlock()
}
