package sludge

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

var (
	ErrPumpNotFound         = errors.New("return pump is not found")
	ErrPumpAssignment       = errors.New("pump assignment is invalid")
	ErrPumpReadyStale       = errors.New("pump ready confirmation is stale")
	ErrPumpReadyNoFlow      = errors.New("pump ready confirmation has no flow")
	ErrPumpOwnershipChanged = errors.New("pump ownership changed before stop")
)

type Pump struct {
	ID              string           `json:"id"`
	LineID          model.LineID     `json:"line_id"`
	Kind            model.DeviceKind `json:"kind"`
	Running         bool             `json:"running"`
	Primed          bool             `json:"primed"`
	Flow            float64          `json:"flow"`
	OwnerSequence   uuid.UUID        `json:"owner_sequence,omitempty"`
	OwnerGeneration uint64           `json:"owner_generation"`
	UpdatedAt       time.Time        `json:"updated_at"`
}

type PumpController struct {
	mu    sync.RWMutex
	pumps map[string]Pump
}

func NewPumpController(pumps []Pump) (*PumpController, error) {
	if len(pumps) < 2 {
		return nil, errors.New("at least two return pumps are required")
	}
	controller := &PumpController{pumps: make(map[string]Pump, len(pumps))}
	for _, pump := range pumps {
		if pump.ID == "" || pump.LineID == "" {
			return nil, errors.New("pump identity is incomplete")
		}
		controller.pumps[pump.ID] = pump
	}
	return controller, nil
}

func (c *PumpController) Assign(pumpID string, sequenceID uuid.UUID, generation uint64, now time.Time) (Pump, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pump, ok := c.pumps[pumpID]
	if !ok || sequenceID == uuid.Nil || generation == 0 {
		return Pump{}, fmt.Errorf("%w: pump %s", ErrPumpAssignment, pumpID)
	}
	pump.OwnerSequence = sequenceID
	pump.OwnerGeneration = generation
	pump.Primed = false
	pump.Running = false
	pump.Flow = 0
	pump.UpdatedAt = now.UTC()
	c.pumps[pumpID] = pump
	return pump, nil
}

func (c *PumpController) Ready(pumpID string, sequenceID uuid.UUID, generation uint64, flow float64, now time.Time) (Pump, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pump, ok := c.pumps[pumpID]
	if !ok {
		return Pump{}, fmt.Errorf("%w: pump %s", ErrPumpNotFound, pumpID)
	}
	if pump.OwnerSequence != sequenceID || pump.OwnerGeneration != generation {
		return Pump{}, fmt.Errorf("%w: pump %s now owned by %s/g%d",
			ErrPumpReadyStale, pumpID, pump.OwnerSequence, pump.OwnerGeneration)
	}
	if flow <= 0 {
		return Pump{}, ErrPumpReadyNoFlow
	}
	pump.Primed = true
	pump.Running = true
	pump.Flow = flow
	pump.UpdatedAt = now.UTC()
	c.pumps[pumpID] = pump
	return pump, nil
}

func (c *PumpController) StopOwned(pumpID string, sequenceID uuid.UUID, generation uint64, now time.Time) (Pump, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pump, ok := c.pumps[pumpID]
	if !ok {
		// 记录里的设备已不存在，无法安全操作，转人工复核。
		return Pump{}, fmt.Errorf("%w: pump %s", ErrPumpNotFound, pumpID)
	}
	if pump.OwnerSequence != sequenceID || pump.OwnerGeneration != generation {
		// 设备已被其他序列接管，绝不能按旧归属停新任务，转人工复核。
		return Pump{}, fmt.Errorf("%w: pump %s now owned by %s/g%d, stale owner %s/g%d",
			ErrPumpOwnershipChanged, pumpID,
			pump.OwnerSequence, pump.OwnerGeneration, sequenceID, generation)
	}
	pump.Running = false
	pump.Flow = 0
	pump.UpdatedAt = now.UTC()
	c.pumps[pumpID] = pump
	return pump, nil
}

func (c *PumpController) Snapshot() []Pump {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]Pump, 0, len(c.pumps))
	for _, pump := range c.pumps {
		result = append(result, pump)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
