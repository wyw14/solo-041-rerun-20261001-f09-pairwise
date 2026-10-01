package journal

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

type memStore struct {
	items []Compensation
	saves int
}

func (m *memStore) Load() ([]Compensation, error) {
	return append([]Compensation(nil), m.items...), nil
}

func (m *memStore) Save(items []Compensation) error {
	m.saves++
	m.items = append([]Compensation(nil), items...)
	return nil
}

func newTestCompensation(t *testing.T, seqID uuid.UUID, dueAt time.Time) Compensation {
	t.Helper()
	item, err := NewCompensation("line-1", seqID, 1, "pump-line-1-A", "stop", dueAt)
	if err != nil {
		t.Fatalf("NewCompensation: %v", err)
	}
	return item
}

func TestScheduleDeduplicatesSameFault(t *testing.T) {
	store := &memStore{}
	queue, err := NewCompensationQueue(store)
	if err != nil {
		t.Fatalf("NewCompensationQueue: %v", err)
	}
	seqID := uuid.New()
	base := time.Now().UTC()
	first := newTestCompensation(t, seqID, base.Add(time.Second))
	second := newTestCompensation(t, seqID, base.Add(2*time.Second))

	scheduled, err := queue.Schedule(first)
	if err != nil {
		t.Fatalf("first Schedule: %v", err)
	}
	if _, err := queue.Schedule(second); err != nil {
		t.Fatalf("second Schedule: %v", err)
	}
	if pending := queue.Pending(); len(pending) != 1 {
		t.Fatalf("expected 1 task after duplicate fault, got %d", len(pending))
	} else if pending[0].ID != scheduled.ID {
		t.Fatal("duplicate fault replaced the existing compensation instead of being ignored")
	}
}

func TestDifferentActionsOrSequencesCoexist(t *testing.T) {
	queue, err := NewCompensationQueue(&memStore{})
	if err != nil {
		t.Fatalf("NewCompensationQueue: %v", err)
	}
	now := time.Now().UTC()
	a := newTestCompensation(t, uuid.New(), now.Add(time.Second))
	b := newTestCompensation(t, uuid.New(), now.Add(time.Second))
	if _, err := queue.Schedule(a); err != nil {
		t.Fatalf("schedule a: %v", err)
	}
	if _, err := queue.Schedule(b); err != nil {
		t.Fatalf("schedule b: %v", err)
	}
	if pending := queue.Pending(); len(pending) != 2 {
		t.Fatalf("expected 2 distinct tasks, got %d", len(pending))
	}
}

func TestExecuteDueRemovesSucceededAndPersists(t *testing.T) {
	store := &memStore{}
	queue, err := NewCompensationQueue(store)
	if err != nil {
		t.Fatalf("NewCompensationQueue: %v", err)
	}
	now := time.Now().UTC()
	if _, err := queue.Schedule(newTestCompensation(t, uuid.New(), now.Add(-time.Second))); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	executed := 0
	count, err := queue.ExecuteDue(context.Background(), now, func(_ context.Context, _ Compensation) error {
		executed++
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteDue: %v", err)
	}
	if count != 1 || executed != 1 {
		t.Fatalf("expected 1 executed, got count=%d executed=%d", count, executed)
	}
	if len(queue.Pending()) != 0 {
		t.Fatal("succeeded compensation should be removed")
	}
	if store.saves < 2 {
		t.Fatalf("expected schedule and completion to be persisted, saves=%d", store.saves)
	}
	if len(store.items) != 0 {
		t.Fatalf("expected empty persisted set after success, got %d", len(store.items))
	}
}

func TestTransientFailureBacksOffAndRetries(t *testing.T) {
	store := &memStore{}
	queue, err := NewCompensationQueue(store)
	if err != nil {
		t.Fatalf("NewCompensationQueue: %v", err)
	}
	now := time.Now().UTC()
	item := newTestCompensation(t, uuid.New(), now)
	item.MaxAttempts = 3
	if _, err := queue.Schedule(item); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	transient := errors.New("modbus timeout")

	count, err := queue.ExecuteDue(context.Background(), now, func(_ context.Context, _ Compensation) error {
		return transient
	})
	if err != nil {
		t.Fatalf("ExecuteDue should not return task-level failure: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 succeeded, got %d", count)
	}
	pending := queue.Pending()
	if len(pending) != 1 {
		t.Fatalf("expected task to remain pending, got %d", len(pending))
	}
	task := pending[0]
	if task.Status != CompensationPending || task.Attempts != 1 {
		t.Fatalf("expected pending with 1 attempt, got status=%s attempts=%d", task.Status, task.Attempts)
	}
	if !task.DueAt.After(now) {
		t.Fatalf("expected retry to be scheduled in the future, due=%s now=%s", task.DueAt, now)
	}
	if task.LastError != transient.Error() {
		t.Fatalf("expected failure reason %q, got %q", transient.Error(), task.LastError)
	}

	// 立即再次执行不应触发（退避未到）。
	if _, err := queue.ExecuteDue(context.Background(), now, func(_ context.Context, _ Compensation) error {
		t.Fatal("task must not run before its next retry time")
		return nil
	}); err != nil {
		t.Fatalf("ExecuteDue early: %v", err)
	}

	// 到达下次重试时间后成功执行，任务移除。
	retryAt := task.DueAt
	if count, err := queue.ExecuteDue(context.Background(), retryAt, func(_ context.Context, _ Compensation) error {
		return nil
	}); err != nil || count != 1 {
		t.Fatalf("expected retry success count=1, got count=%d err=%v", count, err)
	}
	if len(queue.Pending()) != 0 {
		t.Fatal("task should be removed after successful retry")
	}
}

func TestManualReviewStopsRetrying(t *testing.T) {
	queue, err := NewCompensationQueue(&memStore{})
	if err != nil {
		t.Fatalf("NewCompensationQueue: %v", err)
	}
	now := time.Now().UTC()
	if _, err := queue.Schedule(newTestCompensation(t, uuid.New(), now)); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	runs := 0
	if _, err := queue.ExecuteDue(context.Background(), now, func(_ context.Context, _ Compensation) error {
		runs++
		return ErrManualReviewRequired
	}); err != nil {
		t.Fatalf("ExecuteDue: %v", err)
	}
	pending := queue.Pending()
	if len(pending) != 1 || pending[0].Status != CompensationManualReview {
		t.Fatalf("expected manual_review task, got %+v", pending)
	}
	// 无论时间过去多久都不再自动执行。
	if _, err := queue.ExecuteDue(context.Background(), now.Add(time.Hour), func(_ context.Context, _ Compensation) error {
		t.Fatal("manual review task must never run again")
		return nil
	}); err != nil {
		t.Fatalf("ExecuteDue after review: %v", err)
	}
	if runs != 1 {
		t.Fatalf("expected exactly 1 execution attempt, got %d", runs)
	}
}

func TestAttemptsExhaustionMarksFailed(t *testing.T) {
	queue, err := NewCompensationQueue(&memStore{})
	if err != nil {
		t.Fatalf("NewCompensationQueue: %v", err)
	}
	now := time.Now().UTC()
	item := newTestCompensation(t, uuid.New(), now)
	item.MaxAttempts = 2
	if _, err := queue.Schedule(item); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	exec := func(_ context.Context, _ Compensation) error { return errors.New("device offline") }
	if _, err := queue.ExecuteDue(context.Background(), now, exec); err != nil {
		t.Fatalf("ExecuteDue 1: %v", err)
	}
	first := queue.Pending()[0]
	if first.Status != CompensationPending {
		t.Fatalf("expected pending after first failure, got %s", first.Status)
	}
	if _, err := queue.ExecuteDue(context.Background(), first.DueAt, exec); err != nil {
		t.Fatalf("ExecuteDue 2: %v", err)
	}
	final := queue.Pending()
	if len(final) != 1 || final[0].Status != CompensationDead {
		t.Fatalf("expected failed status after max attempts, got %+v", final)
	}
	if final[0].Attempts != 2 || final[0].LastError == "" {
		t.Fatalf("expected 2 attempts and a failure reason, got %+v", final[0])
	}
}

func TestOneTaskFailureDoesNotBlockOthers(t *testing.T) {
	queue, err := NewCompensationQueue(&memStore{})
	if err != nil {
		t.Fatalf("NewCompensationQueue: %v", err)
	}
	now := time.Now().UTC()
	failing := newTestCompensation(t, uuid.New(), now)
	ok := newTestCompensation(t, uuid.New(), now)
	ok.DeviceID = "pump-line-1-B"
	if _, err := queue.Schedule(failing); err != nil {
		t.Fatalf("Schedule failing: %v", err)
	}
	if _, err := queue.Schedule(ok); err != nil {
		t.Fatalf("Schedule ok: %v", err)
	}
	count, err := queue.ExecuteDue(context.Background(), now, func(_ context.Context, item Compensation) error {
		if item.DeviceID == failing.DeviceID {
			return errors.New("boom")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteDue: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected the healthy task to still execute, count=%d", count)
	}
	statuses := queue.Pending()
	if len(statuses) != 1 || statuses[0].DeviceID != failing.DeviceID {
		t.Fatalf("expected only the failing task to remain, got %+v", statuses)
	}
}

func TestQueueRestoresFromFileStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compensations.json")
	store, err := NewFileCompensationStore(path)
	if err != nil {
		t.Fatalf("NewFileCompensationStore: %v", err)
	}
	queue, err := NewCompensationQueue(store)
	if err != nil {
		t.Fatalf("NewCompensationQueue: %v", err)
	}
	now := time.Now().UTC()
	item := newTestCompensation(t, uuid.New(), now.Add(time.Minute))
	item.MaxAttempts = 4
	if _, err := queue.Schedule(item); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// 模拟进程重启：用同一个文件重建存储与队列。
	reopenedStore, err := NewFileCompensationStore(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	reopened, err := NewCompensationQueue(reopenedStore)
	if err != nil {
		t.Fatalf("reopen queue: %v", err)
	}
	restored := reopened.Pending()
	if len(restored) != 1 {
		t.Fatalf("expected 1 restored task, got %d", len(restored))
	}
	got := restored[0]
	if got.ID != item.ID || got.Attempts != 0 || got.Status != CompensationPending ||
		got.MaxAttempts != 4 || !got.DueAt.Equal(item.DueAt) {
		t.Fatalf("restored task does not match scheduled task: %+v", got)
	}

	// 重启前尚未到期，不应执行。
	if _, err := reopened.ExecuteDue(context.Background(), now, func(_ context.Context, _ Compensation) error {
		t.Fatal("task not due yet")
		return nil
	}); err != nil {
		t.Fatalf("ExecuteDue before due: %v", err)
	}
	// 到期后在新进程中继续处理。
	if count, err := reopened.ExecuteDue(context.Background(), item.DueAt, func(_ context.Context, _ Compensation) error {
		return nil
	}); err != nil || count != 1 {
		t.Fatalf("expected restored task to execute after restart, count=%d err=%v", count, err)
	}
}

func TestManualReviewSurvivesRestartAndStaysBlocked(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compensations.json")
	store, _ := NewFileCompensationStore(path)
	queue, _ := NewCompensationQueue(store)
	now := time.Now().UTC()
	if _, err := queue.Schedule(newTestCompensation(t, uuid.New(), now)); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if _, err := queue.ExecuteDue(context.Background(), now, func(_ context.Context, _ Compensation) error {
		return ErrManualReviewRequired
	}); err != nil {
		t.Fatalf("ExecuteDue: %v", err)
	}

	reopenedStore, _ := NewFileCompensationStore(path)
	reopened, _ := NewCompensationQueue(reopenedStore)
	tasks := reopened.Pending()
	if len(tasks) != 1 || tasks[0].Status != CompensationManualReview || tasks[0].LastError == "" {
		t.Fatalf("expected manual_review with reason restored, got %+v", tasks)
	}
	if _, err := reopened.ExecuteDue(context.Background(), now.Add(24*time.Hour), func(_ context.Context, _ Compensation) error {
		t.Fatal("restored manual review task must not run")
		return nil
	}); err != nil {
		t.Fatalf("ExecuteDue: %v", err)
	}
}

func TestBackoffCappedAndMonotonic(t *testing.T) {
	previous := time.Duration(0)
	for attempt := 1; attempt <= 8; attempt++ {
		delay := backoff(attempt)
		if delay <= 0 {
			t.Fatalf("attempt %d: non-positive backoff", attempt)
		}
		if delay < previous && previous != compensationMaxBackoff {
			t.Fatalf("backoff decreased: attempt %d %s < %s", attempt, delay, previous)
		}
		previous = delay
	}
	if backoff(20) != compensationMaxBackoff {
		t.Fatalf("expected backoff cap of %s, got %s", compensationMaxBackoff, backoff(20))
	}
}
