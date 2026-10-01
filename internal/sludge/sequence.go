package sludge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

type SequenceStatus string

const (
	SequenceSwitching SequenceStatus = "switching"
	SequenceRunning   SequenceStatus = "running"
	SequenceFailed    SequenceStatus = "failed"
)

type Sequence struct {
	ID         uuid.UUID      `json:"id"`
	LineID     model.LineID   `json:"line_id"`
	Number     uint64         `json:"number"`
	Generation uint64         `json:"generation"`
	PumpID     string         `json:"pump_id"`
	Status     SequenceStatus `json:"status"`
	StartedAt  time.Time      `json:"started_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

type SequenceBook struct {
	mu     sync.RWMutex
	byLine map[model.LineID]Sequence
	pumps  *PumpController
	queue  *journal.CompensationQueue
}

func NewSequenceBook(pumps *PumpController, queue *journal.CompensationQueue) (*SequenceBook, error) {
	if pumps == nil || queue == nil {
		return nil, errors.New("sludge sequence dependencies are required")
	}
	return &SequenceBook{byLine: make(map[model.LineID]Sequence), pumps: pumps, queue: queue}, nil
}

func (b *SequenceBook) Start(lineID model.LineID, pumpID string, number, generation uint64, now time.Time) (Sequence, error) {
	if lineID == "" || pumpID == "" || number == 0 || generation == 0 {
		return Sequence{}, errors.New("sludge sequence is invalid")
	}
	sequence := Sequence{
		ID: uuid.New(), LineID: lineID, Number: number, Generation: generation,
		PumpID: pumpID, Status: SequenceSwitching, StartedAt: now.UTC(), UpdatedAt: now.UTC(),
	}
	if _, err := b.pumps.Assign(pumpID, sequence.ID, generation, now); err != nil {
		return Sequence{}, err
	}
	b.mu.Lock()
	b.byLine[lineID] = sequence
	b.mu.Unlock()
	return sequence, nil
}

func (b *SequenceBook) ConfirmFlow(lineID model.LineID, sequenceID uuid.UUID, generation uint64, flow float64, now time.Time) (Sequence, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sequence, ok := b.byLine[lineID]
	if !ok || sequence.ID != sequenceID || sequence.Generation != generation {
		return Sequence{}, errors.New("sludge sequence confirmation is stale")
	}
	if _, err := b.pumps.Ready(sequence.PumpID, sequence.ID, sequence.Generation, flow, now); err != nil {
		return Sequence{}, err
	}
	sequence.Status = SequenceRunning
	sequence.UpdatedAt = now.UTC()
	b.byLine[lineID] = sequence
	return sequence, nil
}

func (b *SequenceBook) Fail(lineID model.LineID, now time.Time) (Sequence, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sequence, ok := b.byLine[lineID]
	if !ok {
		return Sequence{}, errors.New("sludge sequence is not active")
	}
	sequence.Status = SequenceFailed
	sequence.UpdatedAt = now.UTC()
	b.byLine[lineID] = sequence
	compensation, err := journal.NewCompensation(lineID, sequence.ID, sequence.Generation, sequence.PumpID, "stop", now.Add(time.Second))
	if err != nil {
		return Sequence{}, err
	}
	// Schedule 按 (sequence, action, device) 幂等：同一故障重复上报不会叠加停泵任务。
	if _, err := b.queue.Schedule(compensation); err != nil {
		return Sequence{}, fmt.Errorf("persist stop-pump compensation: %w", err)
	}
	return sequence, nil
}

func (b *SequenceBook) ExecuteCompensation(ctx context.Context, compensation journal.Compensation, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if compensation.Action != "stop" {
		// 未知动作无法安全执行，也无法靠重试修复，转人工复核。
		return fmt.Errorf("%w: unsupported sludge compensation action %q",
			journal.ErrManualReviewRequired, compensation.Action)
	}
	_, err := b.pumps.StopOwned(compensation.DeviceID, compensation.SequenceID, compensation.Generation, now)
	if errors.Is(err, ErrPumpOwnershipChanged) || errors.Is(err, ErrPumpNotFound) {
		// 泵已被其他序列接管或设备已不存在：误停会伤到新任务，标记人工复核。
		return fmt.Errorf("%w: %w: line %s device %s",
			journal.ErrManualReviewRequired, err, compensation.LineID, compensation.DeviceID)
	}
	return err
}

func (b *SequenceBook) Current(lineID model.LineID) (Sequence, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	sequence, ok := b.byLine[lineID]
	return sequence, ok
}
