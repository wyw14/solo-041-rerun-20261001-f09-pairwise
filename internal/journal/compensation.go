package journal

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

type CompensationStatus string

const (
	CompensationPending      CompensationStatus = "pending"
	CompensationManualReview CompensationStatus = "manual_review"
	CompensationDead         CompensationStatus = "failed"
)

// ErrManualReviewRequired 表示执行器确认现场状态已经偏离补偿前提（例如设备归属
// 已变化），任务绝不能再自动重试，必须转人工复核。
var ErrManualReviewRequired = errors.New("compensation requires manual review")

const (
	compensationMaxAttempts = 5
	compensationBaseBackoff = time.Second
	compensationMaxBackoff  = time.Minute
)

type Compensation struct {
	ID          uuid.UUID          `json:"id"`
	LineID      model.LineID       `json:"line_id"`
	SequenceID  uuid.UUID          `json:"sequence_id"`
	Generation  uint64             `json:"generation"`
	DeviceID    string             `json:"device_id"`
	Action      string             `json:"action"`
	Status      CompensationStatus `json:"status"`
	Attempts    int                `json:"attempts"`
	MaxAttempts int                `json:"max_attempts"`
	DueAt       time.Time          `json:"due_at"`
	LastError   string             `json:"last_error,omitempty"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
}

func NewCompensation(lineID model.LineID, sequenceID uuid.UUID, generation uint64, deviceID, action string, dueAt time.Time) (Compensation, error) {
	if lineID == "" || sequenceID == uuid.Nil || generation == 0 || deviceID == "" || action == "" {
		return Compensation{}, errors.New("compensation identity is incomplete")
	}
	now := time.Now().UTC()
	return Compensation{
		ID: uuid.New(), LineID: lineID, SequenceID: sequenceID, Generation: generation,
		DeviceID: deviceID, Action: action, Status: CompensationPending,
		MaxAttempts: compensationMaxAttempts, DueAt: dueAt.UTC(),
		CreatedAt: now, UpdatedAt: now,
	}, nil
}

// dedupKey 同一故障序列上的同设备同动作补偿只允许存在一条。
func (c Compensation) dedupKey() string {
	return c.SequenceID.String() + "|" + c.Action + "|" + c.DeviceID
}

type CompensationExecutor func(context.Context, Compensation) error

// CompensationStore 持久化补偿任务，保证进程重启后任务不丢失。
type CompensationStore interface {
	Load() ([]Compensation, error)
	Save([]Compensation) error
}

type CompensationQueue struct {
	mu    sync.Mutex
	runMu sync.Mutex
	store CompensationStore
	tasks map[uuid.UUID]Compensation
}

func NewCompensationQueue(store CompensationStore) (*CompensationQueue, error) {
	if store == nil {
		return nil, errors.New("compensation store is required")
	}
	loaded, err := store.Load()
	if err != nil {
		return nil, err
	}
	queue := &CompensationQueue{
		store: store,
		tasks: make(map[uuid.UUID]Compensation, len(loaded)),
	}
	for _, item := range loaded {
		queue.tasks[item.ID] = queue.normalize(item)
	}
	return queue, nil
}

// normalize 兼容历史数据与缺省字段。
func (q *CompensationQueue) normalize(item Compensation) Compensation {
	if item.Status == "" {
		item.Status = CompensationPending
	}
	if item.MaxAttempts <= 0 {
		item.MaxAttempts = compensationMaxAttempts
	}
	return item
}

// Schedule 幂等地登记补偿任务：同一故障序列上相同的设备停泵任务已经存在时
// （无论是否仍在重试），保留既有任务，不叠加新任务。落盘失败时任务仍保留在
// 内存中并在下次状态变更时重试落盘，但会把错误返回给调用方。
func (q *CompensationQueue) Schedule(item Compensation) (Compensation, error) {
	item = q.normalize(item)
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, existing := range q.tasks {
		if existing.dedupKey() == item.dedupKey() {
			return existing, nil
		}
	}
	q.tasks[item.ID] = item
	if err := q.persistLocked(); err != nil {
		return item, err
	}
	return item, nil
}

// ExecuteDue 执行所有到期的待处理任务。单个任务失败不影响其他任务：
// 临时性错误按指数退避安排下次重试并累计执行次数，超出上限后转入 failed；
// 执行器返回 ErrManualReviewRequired 时转入 manual_review，不再自动重试。
// 返回成功执行的任务数。
func (q *CompensationQueue) ExecuteDue(ctx context.Context, now time.Time, execute CompensationExecutor) (int, error) {
	if execute == nil {
		return 0, errors.New("compensation executor is required")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	q.runMu.Lock()
	defer q.runMu.Unlock()

	q.mu.Lock()
	due := make([]Compensation, 0)
	for _, item := range q.tasks {
		if item.Status == CompensationPending && !item.DueAt.After(now) {
			due = append(due, item)
		}
	}
	sort.SliceStable(due, func(i, j int) bool { return due[i].DueAt.Before(due[j].DueAt) })
	q.mu.Unlock()

	succeeded := 0
	changed := false
	for _, item := range due {
		if err := ctx.Err(); err != nil {
			q.mu.Lock()
			if changed {
				_ = q.persistLocked()
			}
			q.mu.Unlock()
			return succeeded, err
		}
		err := execute(ctx, item)
		q.mu.Lock()
		current, ok := q.tasks[item.ID]
		if !ok {
			// 任务在执行期间被外部移除，状态以存储为准。
			q.mu.Unlock()
			continue
		}
		current.UpdatedAt = now.UTC()
		switch {
		case err == nil:
			delete(q.tasks, item.ID)
			succeeded++
		case errors.Is(err, ErrManualReviewRequired):
			current.Status = CompensationManualReview
			current.LastError = err.Error()
			q.tasks[current.ID] = current
		default:
			current.Attempts++
			current.LastError = err.Error()
			if current.Attempts >= current.MaxAttempts {
				current.Status = CompensationDead
			} else {
				current.DueAt = now.Add(backoff(current.Attempts)).UTC()
			}
			q.tasks[current.ID] = current
		}
		changed = true
		q.mu.Unlock()
	}

	if changed {
		q.mu.Lock()
		err := q.persistLocked()
		q.mu.Unlock()
		if err != nil {
			return succeeded, err
		}
	}
	return succeeded, nil
}

func backoff(attempts int) time.Duration {
	shift := attempts - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 16 {
		return compensationMaxBackoff
	}
	delay := compensationBaseBackoff << shift
	if delay > compensationMaxBackoff || delay <= 0 {
		return compensationMaxBackoff
	}
	return delay
}

// Pending 返回全部补偿任务（含待重试、人工复核、重试耗尽），按到期时间排序。
func (q *CompensationQueue) Pending() []Compensation {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]Compensation, 0, len(q.tasks))
	for _, item := range q.tasks {
		result = append(result, item)
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].DueAt.Equal(result[j].DueAt) {
			return result[i].CreatedAt.Before(result[j].CreatedAt)
		}
		return result[i].DueAt.Before(result[j].DueAt)
	})
	return result
}

// persistLocked 调用方必须持有 q.mu。落盘失败不改变内存状态，
// 下一次状态变更会再次尝试落盘。
func (q *CompensationQueue) persistLocked() error {
	items := make([]Compensation, 0, len(q.tasks))
	for _, item := range q.tasks {
		items = append(items, item)
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].ID.String() < items[j].ID.String() })
	return q.store.Save(items)
}
