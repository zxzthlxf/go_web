package models

import (
	"encoding/json"
	"time"
)

// JobStatus 任务状态
type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusPaused    JobStatus = "paused"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
	JobStatusStopped   JobStatus = "stopped"
)

// JobType 任务类型
type JobType string

const (
	JobTypeFull        JobType = "full"        // 全量迁移
	JobTypeIncremental JobType = "incremental" // 增量迁移
)

// MigrationJob 迁移任务
type MigrationJob struct {
	ID          uint   `json:"id" gorm:"primaryKey"`
	Name        string `json:"name" gorm:"size:100;not null"`
	Description string `json:"description" gorm:"type:text"`

	SourceID    uint       `json:"source_id"`
	Source      DataSource `json:"source" gorm:"foreignKey:SourceID"`
	SourceTable string     `json:"source_table" gorm:"size:200"`
	SourceQuery string     `json:"source_query" gorm:"type:text"` // 自定义查询

	TargetID    uint       `json:"target_id"`
	Target      DataSource `json:"target" gorm:"foreignKey:TargetID"`
	TargetTable string     `json:"target_table" gorm:"size:200"`

	Type   JobType   `json:"type" gorm:"size:20;default:'full'"`
	Status JobStatus `json:"status" gorm:"size:20;default:'pending'"`

	// 配置参数
	Config        string `json:"config" gorm:"type:text"` // JSON格式配置
	BatchSize     int    `json:"batch_size" gorm:"default:1000"`
	MaxErrorCount int    `json:"max_error_count" gorm:"default:0"` // 0表示不限制

	// 调度配置
	CronExpression  string `json:"cron_expression"` // cron表达式
	ScheduleEnabled bool   `json:"schedule_enabled" gorm:"default:false"`

	// 执行统计
	TotalRecords     int64      `json:"total_records" gorm:"default:0"`
	ProcessedRecords int64      `json:"processed_records" gorm:"default:0"`
	ErrorRecords     int64      `json:"error_records" gorm:"default:0"`
	StartTime        *time.Time `json:"start_time"`
	EndTime          *time.Time `json:"end_time"`
	Duration         int64      `json:"duration" gorm:"default:0"` // 秒

	// 字段映射
	ColumnMappings string `json:"column_mappings" gorm:"type:text"` // JSON格式

	// 增量配置
	IncrementColumn string `json:"increment_column"`
	IncrementType   string `json:"increment_type"` // timestamp, id, date
	LastValue       string `json:"last_value"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// JobConfig 任务配置
type JobConfig struct {
	Speed struct {
		Channel int `json:"channel"`
		Bytes   int `json:"bytes,omitempty"`
		Records int `json:"records,omitempty"`
	} `json:"speed"`

	ErrorLimit struct {
		Record     int     `json:"record"`
		Percentage float64 `json:"percentage,omitempty"`
	} `json:"errorLimit"`
}

// ColumnMapping 字段映射
type ColumnMapping struct {
	SourceColumn string `json:"source_column"`
	TargetColumn string `json:"target_column"`
	DataType     string `json:"data_type,omitempty"`
	Transform    string `json:"transform,omitempty"` // 转换规则
	IsPrimaryKey bool   `json:"is_primary_key,omitempty"`
}

// GetConfig 获取任务配置
func (j *MigrationJob) GetConfig() JobConfig {
	var config JobConfig
	if j.Config != "" {
		json.Unmarshal([]byte(j.Config), &config)
	}
	return config
}

// GetColumnMappings 获取字段映射
func (j *MigrationJob) GetColumnMappings() []ColumnMapping {
	var mappings []ColumnMapping
	if j.ColumnMappings != "" {
		json.Unmarshal([]byte(j.ColumnMappings), &mappings)
	}
	return mappings
}

// SetColumnMappings 设置字段映射
func (j *MigrationJob) SetColumnMappings(mappings []ColumnMapping) error {
	data, err := json.Marshal(mappings)
	if err != nil {
		return err
	}
	j.ColumnMappings = string(data)
	return nil
}
