package journal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// FileCompensationStore 把补偿队列完整快照写入单个 JSON 文件，
// 写入采用临时文件 + rename 的原子替换，避免进程崩溃留下半截文件。
type FileCompensationStore struct {
	path string
}

func NewFileCompensationStore(path string) (*FileCompensationStore, error) {
	if path == "" {
		return nil, errors.New("compensation store path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create compensation directory: %w", err)
	}
	return &FileCompensationStore{path: path}, nil
}

func (s *FileCompensationStore) Load() ([]Compensation, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return []Compensation{}, nil
	}
	if err != nil {
		return nil, err
	}
	var items []Compensation
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, fmt.Errorf("decode compensation file: %w", err)
	}
	return items, nil
}

func (s *FileCompensationStore) Save(items []Compensation) error {
	data, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	temporary := s.path + ".next"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o640); err != nil {
		return err
	}
	return os.Rename(temporary, s.path)
}
