package sludge

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

func newTestBook(t *testing.T) *SequenceBook {
	t.Helper()
	pumps, err := NewPumpController([]Pump{
		{ID: "pump-line-1-A", LineID: model.LineID("line-1"), Kind: model.DevicePump},
		{ID: "pump-line-1-B", LineID: model.LineID("line-1"), Kind: model.DevicePump},
	})
	if err != nil {
		t.Fatalf("NewPumpController: %v", err)
	}
	store, err := journal.NewFileCompensationStore(filepath.Join(t.TempDir(), "compensations.json"))
	if err != nil {
		t.Fatalf("NewFileCompensationStore: %v", err)
	}
	queue, err := journal.NewCompensationQueue(store)
	if err != nil {
		t.Fatalf("NewCompensationQueue: %v", err)
	}
	book, err := NewSequenceBook(pumps, queue)
	if err != nil {
		t.Fatalf("NewSequenceBook: %v", err)
	}
	return book
}

func startFailedSequence(t *testing.T, book *SequenceBook, now time.Time, pumpID string) Sequence {
	t.Helper()
	seq, err := book.Start("line-1", pumpID, 1, 7, now)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := book.Fail("line-1", now); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	return seq
}

func TestFailSchedulesSingleStopCompensation(t *testing.T) {
	book := newTestBook(t)
	now := time.Now().UTC()
	seq := startFailedSequence(t, book, now, "pump-line-1-A")

	pending := book.queue.Pending()
	if len(pending) != 1 {
		t.Fatalf("expected 1 compensation after failure, got %d", len(pending))
	}
	task := pending[0]
	if task.SequenceID != seq.ID || task.DeviceID != "pump-line-1-A" || task.Action != "stop" {
		t.Fatalf("unexpected compensation: %+v", task)
	}
	if task.Status != journal.CompensationPending || task.Attempts != 0 {
		t.Fatalf("new compensation should be pending with 0 attempts, got %+v", task)
	}

	// 同一故障重复上报不得叠加相同停泵任务。
	if _, err := book.Fail("line-1", now.Add(time.Second)); err != nil {
		t.Fatalf("second Fail: %v", err)
	}
	if again := book.queue.Pending(); len(again) != 1 || again[0].ID != task.ID {
		t.Fatalf("duplicate failure must not stack compensation tasks, got %+v", again)
	}
}

func TestCompensationStopsOwnedPumpAndCompletes(t *testing.T) {
	book := newTestBook(t)
	now := time.Now().UTC()
	startFailedSequence(t, book, now, "pump-line-1-A")

	due := now.Add(time.Second)
	count, err := book.queue.ExecuteDue(context.Background(), due,
		func(ctx context.Context, item journal.Compensation) error {
			return book.ExecuteCompensation(ctx, item, due)
		})
	if err != nil {
		t.Fatalf("ExecuteDue: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 successful stop, got %d", count)
	}
	if len(book.queue.Pending()) != 0 {
		t.Fatalf("completed compensation must be removed, got %+v", book.queue.Pending())
	}
	for _, pump := range book.pumps.Snapshot() {
		if pump.ID == "pump-line-1-A" && pump.Running {
			t.Fatal("failed sequence pump should be stopped after compensation")
		}
	}
}

func TestOwnershipChangeRoutesToManualReview(t *testing.T) {
	book := newTestBook(t)
	now := time.Now().UTC()
	failed := startFailedSequence(t, book, now, "pump-line-1-A")

	// 故障后同一台泵被新序列接管并已带流量运行（归属变化）。
	newOwner := uuid.New()
	reassignAt := now.Add(2 * time.Second)
	if _, err := book.pumps.Assign("pump-line-1-A", newOwner, 8, reassignAt); err != nil {
		t.Fatalf("reassign pump: %v", err)
	}
	if _, err := book.pumps.Ready("pump-line-1-A", newOwner, 8, 120.0, reassignAt); err != nil {
		t.Fatalf("new owner ready: %v", err)
	}

	due := now.Add(3 * time.Second)
	if _, err := book.queue.ExecuteDue(context.Background(), due,
		func(ctx context.Context, item journal.Compensation) error {
			return book.ExecuteCompensation(ctx, item, due)
		}); err != nil {
		t.Fatalf("ExecuteDue: %v", err)
	}

	pending := book.queue.Pending()
	if len(pending) != 1 {
		t.Fatalf("expected compensation to remain for review, got %+v", pending)
	}
	task := pending[0]
	if task.Status != journal.CompensationManualReview {
		t.Fatalf("expected manual_review when ownership changed, got %s", task.Status)
	}
	if task.LastError == "" {
		t.Fatal("manual review task must carry the failure reason")
	}
	if task.SequenceID != failed.ID {
		t.Fatalf("manual review task identity changed: %+v", task)
	}

	// 新归属的泵绝不能被旧补偿停掉：归属与运行状态都应保持新任务的样子。
	for _, pump := range book.pumps.Snapshot() {
		if pump.ID == "pump-line-1-A" {
			if pump.OwnerSequence != newOwner || !pump.Running || pump.Flow != 120.0 {
				t.Fatalf("stale compensation altered the new owner's pump: %+v", pump)
			}
		}
	}

	// 人工复核任务之后不再自动执行。
	if _, err := book.queue.ExecuteDue(context.Background(), due.Add(time.Hour),
		func(_ context.Context, _ journal.Compensation) error {
			t.Fatal("manual review compensation must not be retried")
			return nil
		}); err != nil {
		t.Fatalf("ExecuteDue after review: %v", err)
	}
}

func TestUnknownCompensationActionRequiresReview(t *testing.T) {
	book := newTestBook(t)
	now := time.Now().UTC()
	err := book.ExecuteCompensation(context.Background(), journal.Compensation{
		ID: uuid.New(), LineID: "line-1", SequenceID: uuid.New(), Generation: 1,
		DeviceID: "pump-line-1-A", Action: "purge",
	}, now)
	if !errors.Is(err, journal.ErrManualReviewRequired) {
		t.Fatalf("expected ErrManualReviewRequired for unknown action, got %v", err)
	}
}
