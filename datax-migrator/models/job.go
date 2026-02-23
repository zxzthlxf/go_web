package models

import (
	"encoding/json"
	"log"
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
	Name        string `json:"name" gorm:"size:100;not null;index"` // 增加索引，加速搜索
	Description string `json:"description,omitempty" gorm:"type:text"`

	SourceID    uint       `json:"source_id" gorm:"index;not null"` // 外键索引
	Source      DataSource `json:"source,omitempty" gorm:"foreignKey:SourceID;references:ID"`
	SourceTable string     `json:"source_table" gorm:"size:200;not null"`   // 源表名不能为空
	SourceQuery string     `json:"source_query,omitempty" gorm:"type:text"` // 自定义查询

	TargetID    uint       `json:"target_id" gorm:"index;not null"`
	Target      DataSource `json:"target,omitempty" gorm:"foreignKey:TargetID;references:ID"`
	TargetTable string     `json:"target_table" gorm:"size:200;not null"`

	Type   JobType   `json:"type" gorm:"size:20;default:'full'"`
	Status JobStatus `json:"status" gorm:"size:20;default:'pending';index"`

	// 配置参数
	Config        string `json:"config,omitempty" gorm:"type:text"` // JSON格式配置
	BatchSize     int    `json:"batch_size" gorm:"default:1000"`
	MaxErrorCount int    `json:"max_error_count" gorm:"default:0"` // 0表示不限制

	// 调度配置
	CronExpression  string `json:"cron_expression,omitempty" gorm:"size:100"` // cron表达式
	ScheduleEnabled bool   `json:"schedule_enabled" gorm:"default:false"`

	// 执行统计
	TotalRecords     int64      `json:"total_records" gorm:"default:0"`
	ProcessedRecords int64      `json:"processed_records" gorm:"default:0"`
	ErrorRecords     int64      `json:"error_records" gorm:"default:0"`
	StartTime        *time.Time `json:"start_time,omitempty"`
	EndTime          *time.Time `json:"end_time,omitempty"`
	Duration         int64      `json:"duration" gorm:"default:0"` // 秒

	// 字段映射
	ColumnMappings string `json:"column_mappings,omitempty" gorm:"type:text"` // JSON格式

	// 增量配置
	IncrementColumn string `json:"increment_column,omitempty" gorm:"size:100"`
	IncrementType   string `json:"increment_type,omitempty" gorm:"size:20"` // timestamp, id, date
	LastValue       string `json:"last_value,omitempty" gorm:"size:255"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// JobConfig 任务配置
type JobConfig struct {
	Speed struct {
		Channel int `json:"channel"`           // 并发通道数
		Bytes   int `json:"bytes,omitempty"`   // 字节限速
		Records int `json:"records,omitempty"` // 记录数限速
	} `json:"speed"`

	ErrorLimit struct {
		Record     int     `json:"record"`               // 错误记录数上限
		Percentage float64 `json:"percentage,omitempty"` // 错误百分比上限
	} `json:"errorLimit"`
}

// ColumnMapping 字段映射
type ColumnMapping struct {
	SourceColumn string `json:"source_column"`            // 源字段名
	TargetColumn string `json:"target_column"`            // 目标字段名
	DataType     string `json:"data_type,omitempty"`      // 数据类型（可选）
	Transform    string `json:"transform,omitempty"`      // 转换规则
	IsPrimaryKey bool   `json:"is_primary_key,omitempty"` // 是否为主键
}

// GetConfig 获取任务配置（解析 JSON）
func (j *MigrationJob) GetConfig() JobConfig {
	var config JobConfig
	if j.Config != "" {
		if err := json.Unmarshal([]byte(j.Config), &config); err != nil {
			// 解析失败时记录日志，并返回空配置
			log.Printf("解析任务配置失败: jobID=%d, error=%v", j.ID, err)
		}
	}
	return config
}

// GetColumnMappings 获取字段映射（解析 JSON）
func (j *MigrationJob) GetColumnMappings() []ColumnMapping {
	var mappings []ColumnMapping
	if j.ColumnMappings != "" {
		if err := json.Unmarshal([]byte(j.ColumnMappings), &mappings); err != nil {
			log.Printf("解析字段映射失败: jobID=%d, error=%v", j.ID, err)
		}
	}
	return mappings
}

// SetColumnMappings 设置字段映射（序列化为 JSON）
func (j *MigrationJob) SetColumnMappings(mappings []ColumnMapping) error {
	data, err := json.Marshal(mappings)
	if err != nil {
		return err
	}
	j.ColumnMappings = string(data)
	return nil
}
