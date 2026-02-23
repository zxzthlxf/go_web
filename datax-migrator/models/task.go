package models

import (
	"time"
)

// TaskStatus 任务执行状态
type TaskStatus string

const (
	TaskStatusPending TaskStatus = "pending" // 等待执行
	TaskStatusRunning TaskStatus = "running" // 执行中
	TaskStatusSuccess TaskStatus = "success" // 成功
	TaskStatusFailed  TaskStatus = "failed"  // 失败
)

// MigrationTask 迁移任务执行记录
type MigrationTask struct {
	ID    uint         `json:"id" gorm:"primaryKey"`
	JobID uint         `json:"job_id" gorm:"index;not null"`              // 关联的作业ID
	Job   MigrationJob `json:"job" gorm:"foreignKey:JobID;references:ID"` // 作业详情（预加载用）

	TaskID string     `json:"task_id" gorm:"size:100;uniqueIndex"`           // 全局唯一任务执行ID
	Status TaskStatus `json:"status" gorm:"size:20;default:'pending';index"` // 当前状态

	// 执行详情
	ConfigFile string `json:"config_file" gorm:"size:255"` // DataX配置文件路径
	Command    string `json:"command" gorm:"type:text"`    // 执行的完整命令
	Output     string `json:"output" gorm:"type:text"`     // 命令标准输出

	// 统计信息
	ReadRecords  int64 `json:"read_records" gorm:"default:0"`  // 读取记录数
	WriteRecords int64 `json:"write_records" gorm:"default:0"` // 写入记录数
	ErrorRecords int64 `json:"error_records" gorm:"default:0"` // 错误记录数

	// 性能指标
	StartTime  *time.Time `json:"start_time"`                   // 开始时间
	EndTime    *time.Time `json:"end_time"`                     // 结束时间
	Duration   int64      `json:"duration" gorm:"default:0"`    // 执行耗时（毫秒）
	ReadSpeed  float64    `json:"read_speed" gorm:"default:0"`  // 读取速度（条/秒）
	WriteSpeed float64    `json:"write_speed" gorm:"default:0"` // 写入速度（条/秒）

	// 错误信息
	ErrorMessage string `json:"error_message" gorm:"type:text"` // 简要错误
	ErrorDetails string `json:"error_details" gorm:"type:text"` // 详细错误堆栈或stderr

	CreatedAt time.Time `json:"created_at"` // 记录创建时间
	UpdatedAt time.Time `json:"updated_at"` // 最后更新时间
}

// TaskLog 任务日志（用于记录DataX执行过程中的详细日志）
type TaskLog struct {
	ID     uint          `json:"id" gorm:"primaryKey"`
	TaskID uint          `json:"task_id" gorm:"index;not null"`               // 关联的任务ID
	Task   MigrationTask `json:"task" gorm:"foreignKey:TaskID;references:ID"` // 任务详情

	Level     string    `json:"level" gorm:"size:20;index"` // 日志级别（INFO/WARN/ERROR/DEBUG）
	Message   string    `json:"message" gorm:"type:text"`   // 日志内容
	Timestamp time.Time `json:"timestamp" gorm:"index"`     // 日志时间

	CreatedAt time.Time `json:"created_at"` // 记录创建时间
}
