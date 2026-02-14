package services

import (
	"fmt"
	"time"

	"gorm.io/gorm"

	"datax-migrator/models"
)

// JobService 任务服务
type JobService struct {
	db           *gorm.DB
	dataxService *DataXService
}

// NewJobService 创建任务服务
func NewJobService(db *gorm.DB, dataxService *DataXService) *JobService {
	return &JobService{
		db:           db,
		dataxService: dataxService,
	}
}

// CreateJob 创建迁移任务
func (s *JobService) CreateJob(job *models.MigrationJob) error {
	// 验证数据源
	var source, target models.DataSource
	if err := s.db.First(&source, job.SourceID).Error; err != nil {
		return fmt.Errorf("源数据源不存在")
	}
	if err := s.db.First(&target, job.TargetID).Error; err != nil {
		return fmt.Errorf("目标数据源不存在")
	}

	job.Source = source
	job.Target = target
	job.Status = models.JobStatusPending
	job.CreatedAt = time.Now()
	job.UpdatedAt = time.Now()

	return s.db.Create(job).Error
}

// UpdateJob 更新迁移任务
func (s *JobService) UpdateJob(job *models.MigrationJob) error {
	// 检查任务是否存在
	var existingJob models.MigrationJob
	if err := s.db.First(&existingJob, job.ID).Error; err != nil {
		return fmt.Errorf("任务不存在")
	}

	// 不能修改正在运行的任务
	if existingJob.Status == models.JobStatusRunning {
		return fmt.Errorf("任务正在运行，无法修改")
	}

	job.UpdatedAt = time.Now()
	return s.db.Save(job).Error
}

// DeleteJob 删除迁移任务
func (s *JobService) DeleteJob(id uint) error {
	// 检查任务是否存在
	var job models.MigrationJob
	if err := s.db.First(&job, id).Error; err != nil {
		return fmt.Errorf("任务不存在")
	}

	// 不能删除正在运行的任务
	if job.Status == models.JobStatusRunning {
		return fmt.Errorf("任务正在运行，无法删除")
	}

	// 删除关联的任务执行记录
	s.db.Where("job_id = ?", id).Delete(&models.MigrationTask{})

	return s.db.Delete(&job).Error
}

// GetJob 获取任务详情
func (s *JobService) GetJob(id uint) (*models.MigrationJob, error) {
	var job models.MigrationJob
	if err := s.db.Preload("Source").Preload("Target").First(&job, id).Error; err != nil {
		return nil, fmt.Errorf("任务不存在")
	}
	return &job, nil
}

// ListJobs 获取任务列表
func (s *JobService) ListJobs(page, pageSize int, filters map[string]interface{}) ([]models.MigrationJob, int64, error) {
	var jobs []models.MigrationJob
	var total int64

	query := s.db.Model(&models.MigrationJob{})

	// 应用过滤器
	if status, ok := filters["status"]; ok {
		query = query.Where("status = ?", status)
	}
	if name, ok := filters["name"]; ok {
		query = query.Where("name LIKE ?", "%"+name.(string)+"%")
	}

	// 计算总数
	query.Count(&total)

	// 分页查询
	offset := (page - 1) * pageSize
	err := query.Preload("Source").Preload("Target").
		Order("created_at DESC").
		Offset(offset).Limit(pageSize).
		Find(&jobs).Error

	return jobs, total, err
}

// RunJob 执行迁移任务
func (s *JobService) RunJob(id uint) (*models.MigrationTask, error) {
	// 获取任务
	job, err := s.GetJob(id)
	if err != nil {
		return nil, err
	}

	// 检查任务状态
	if job.Status == models.JobStatusRunning {
		return nil, fmt.Errorf("任务已在运行中")
	}

	// 更新任务状态
	job.Status = models.JobStatusRunning
	now := time.Now()
	job.StartTime = &now
	s.db.Save(job)

	// 执行DataX任务
	task, err := s.dataxService.ExecuteJob(job)
	if err != nil {
		// 执行失败，更新任务状态
		job.Status = models.JobStatusFailed
		job.EndTime = &now
		s.db.Save(job)
		return nil, err
	}

	return task, nil
}

// StopJob 停止迁移任务
func (s *JobService) StopJob(id uint) error {
	// 获取任务
	job, err := s.GetJob(id)
	if err != nil {
		return err
	}

	// 检查任务状态
	if job.Status != models.JobStatusRunning {
		return fmt.Errorf("任务未在运行中")
	}

	// 停止DataX任务
	err = s.dataxService.StopJob(id)
	if err != nil {
		return err
	}

	// 更新任务状态
	job.Status = models.JobStatusStopped
	now := time.Now()
	job.EndTime = &now
	s.db.Save(job)

	return nil
}

// GetJobTasks 获取任务执行记录
func (s *JobService) GetJobTasks(jobID uint, page, pageSize int) ([]models.MigrationTask, int64, error) {
	var tasks []models.MigrationTask
	var total int64

	query := s.db.Where("job_id = ?", jobID)
	query.Model(&models.MigrationTask{}).Count(&total)

	err := query.Order("created_at DESC").
		Offset((page - 1) * pageSize).
		Limit(pageSize).
		Find(&tasks).Error

	return tasks, total, err
}

// GetTask 获取任务执行详情
func (s *JobService) GetTask(id uint) (*models.MigrationTask, error) {
	var task models.MigrationTask
	if err := s.db.Preload("Job").First(&task, id).Error; err != nil {
		return nil, fmt.Errorf("任务执行记录不存在")
	}
	return &task, nil
}

// GetTaskLogs 获取任务日志
func (s *JobService) GetTaskLogs(taskID uint) ([]models.TaskLog, error) {
	var logs []models.TaskLog
	err := s.db.Where("task_id = ?", taskID).
		Order("timestamp ASC").
		Find(&logs).Error
	return logs, err
}
