package models

import (
	"time"
)

// TaskStatus 任务执行状态
type TaskStatus string

const (
	TaskStatusPending TaskStatus = "pending"
	TaskStatusRunning TaskStatus = "running"
	TaskStatusSuccess TaskStatus = "success"
	TaskStatusFailed  TaskStatus = "failed"
)

// MigrationTask 迁移任务执行记录
type MigrationTask struct {
	ID    uint         `json:"id" gorm:"primaryKey"`
	JobID uint         `json:"job_id"`
	Job   MigrationJob `json:"job" gorm:"foreignKey:JobID"`

	TaskID string     `json:"task_id" gorm:"size:100;uniqueIndex"` // 执行ID
	Status TaskStatus `json:"status" gorm:"size:20;default:'pending'"`

	// 执行详情
	ConfigFile string `json:"config_file"`              // DataX配置文件路径
	Command    string `json:"command" gorm:"type:text"` // 执行的命令
	Output     string `json:"output" gorm:"type:text"`  // 命令输出

	// 统计信息
	TotalRecords int64 `json:"total_records" gorm:"default:0"`
	ReadRecords  int64 `json:"read_records" gorm:"default:0"`
	WriteRecords int64 `json:"write_records" gorm:"default:0"`
	ErrorRecords int64 `json:"error_records" gorm:"default:0"`

	// 性能指标
	StartTime  *time.Time `json:"start_time"`
	EndTime    *time.Time `json:"end_time"`
	Duration   int64      `json:"duration" gorm:"default:0"`    // 毫秒
	ReadSpeed  float64    `json:"read_speed" gorm:"default:0"`  // 条/秒
	WriteSpeed float64    `json:"write_speed" gorm:"default:0"` // 条/秒

	// 错误信息
	ErrorMessage string `json:"error_message" gorm:"type:text"`
	ErrorDetails string `json:"error_details" gorm:"type:text"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TaskLog 任务日志
type TaskLog struct {
	ID     uint          `json:"id" gorm:"primaryKey"`
	TaskID uint          `json:"task_id"`
	Task   MigrationTask `json:"task" gorm:"foreignKey:TaskID"`

	Level     string    `json:"level" gorm:"size:20"` // INFO, WARN, ERROR, DEBUG
	Message   string    `json:"message" gorm:"type:text"`
	Timestamp time.Time `json:"timestamp"`

	CreatedAt time.Time `json:"created_at"`
}
