package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
	"gorm.io/gorm"

	"datax-migrator/models"
)

// DataXService DataX服务
type DataXService struct {
	db          *gorm.DB
	basePath    string
	jobPath     string
	logPath     string
	pythonPath  string
	runningJobs sync.Map
}

// NewDataXService 创建DataX服务
func NewDataXService() *DataXService {
	return &DataXService{
		basePath:    viper.GetString("datax.path"),
		jobPath:     viper.GetString("datax.job_path"),
		logPath:     viper.GetString("datax.log_path"),
		pythonPath:  viper.GetString("datax.python_path"),
		runningJobs: sync.Map{},
	}
}

// GetReaderPlugins 获取读取插件列表
func (s *DataXService) GetReaderPlugins() ([]string, error) {
	plugins := []string{
		"mysqlreader",
		"oraclereader",
		"postgresqlreader",
		"sqlserverreader",
		"hdfsreader",
		"mongodbreader",
		"elasticsearchreader",
		"ftpreader",
		"ossreader",
	}
	return plugins, nil
}

// GetWriterPlugins 获取写入插件列表
func (s *DataXService) GetWriterPlugins() ([]string, error) {
	plugins := []string{
		"mysqlwriter",
		"oraclewriter",
		"postgresqlwriter",
		"sqlserverwriter",
		"hdfswriter",
		"hivewriter",
		"mongodbwriter",
		"elasticsearchwriter",
		"ftpwriter",
		"osswriter",
	}
	return plugins, nil
}

// GenerateConfig 生成DataX配置
func (s *DataXService) GenerateConfig(job *models.MigrationJob) (string, error) {
	config := map[string]interface{}{
		"job": map[string]interface{}{
			"content": []map[string]interface{}{
				{
					"reader": s.generateReaderConfig(job),
					"writer": s.generateWriterConfig(job),
				},
			},
			"setting": s.generateSettingConfig(job),
		},
	}

	// 转换为JSON
	configJSON, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return "", fmt.Errorf("生成配置JSON失败: %v", err)
	}

	// 保存配置文件
	configDir := filepath.Join(s.jobPath, "configs")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return "", fmt.Errorf("创建配置目录失败: %v", err)
	}

	configFile := filepath.Join(configDir, fmt.Sprintf("job_%d_%d.json", job.ID, time.Now().Unix()))
	if err := ioutil.WriteFile(configFile, configJSON, 0644); err != nil {
		return "", fmt.Errorf("保存配置文件失败: %v", err)
	}

	return configFile, nil
}

// generateReaderConfig 生成读取器配置
func (s *DataXService) generateReaderConfig(job *models.MigrationJob) map[string]interface{} {
	readerConfig := map[string]interface{}{
		"name": s.getReaderPluginName(job.Source.Type),
	}

	// 根据数据库类型设置具体参数
	switch job.Source.Type {
	case models.MySQL:
		readerConfig["parameter"] = map[string]interface{}{
			"username": job.Source.Username,
			"password": job.Source.Password,
			"column":   s.getColumnsForJob(job),
			"connection": []map[string]interface{}{
				{
					"query":   []string{s.getSourceQuery(job)},
					"jdbcUrl": []string{job.Source.GetJDBCURL()},
				},
			},
			"fetchSize": job.BatchSize,
		}
	case models.PostgreSQL:
		readerConfig["parameter"] = map[string]interface{}{
			"username": job.Source.Username,
			"password": job.Source.Password,
			"column":   s.getColumnsForJob(job),
			"connection": []map[string]interface{}{
				{
					"query":   []string{s.getSourceQuery(job)},
					"jdbcUrl": []string{job.Source.GetJDBCURL()},
				},
			},
			"fetchSize": job.BatchSize,
		}
	case models.Oracle:
		readerConfig["parameter"] = map[string]interface{}{
			"username": job.Source.Username,
			"password": job.Source.Password,
			"column":   s.getColumnsForJob(job),
			"connection": []map[string]interface{}{
				{
					"query":   []string{s.getSourceQuery(job)},
					"jdbcUrl": []string{job.Source.GetJDBCURL()},
				},
			},
			"fetchSize": job.BatchSize,
		}
	case models.MongoDB:
		readerConfig["parameter"] = map[string]interface{}{
			"address":        []string{fmt.Sprintf("%s:%d", job.Source.Host, job.Source.Port)},
			"userName":       job.Source.Username,
			"userPassword":   job.Source.Password,
			"dbName":         job.Source.Database,
			"collectionName": job.SourceTable,
			"column":         s.getMongoDBColumns(job),
		}
	}

	return readerConfig
}

// generateWriterConfig 生成写入器配置
func (s *DataXService) generateWriterConfig(job *models.MigrationJob) map[string]interface{} {
	writerConfig := map[string]interface{}{
		"name": s.getWriterPluginName(job.Target.Type),
	}

	// 根据数据库类型设置具体参数
	switch job.Target.Type {
	case models.MySQL:
		writerConfig["parameter"] = map[string]interface{}{
			"writeMode": "insert", // insert, update, replace
			"username":  job.Target.Username,
			"password":  job.Target.Password,
			"column":    s.getTargetColumns(job),
			"connection": []map[string]interface{}{
				{
					"jdbcUrl": job.Target.GetJDBCURL(),
					"table":   []string{job.TargetTable},
				},
			},
			"preSql":    []string{}, // 执行前SQL
			"postSql":   []string{}, // 执行后SQL
			"batchSize": job.BatchSize,
		}
	case models.PostgreSQL:
		writerConfig["parameter"] = map[string]interface{}{
			"writeMode": "insert",
			"username":  job.Target.Username,
			"password":  job.Target.Password,
			"column":    s.getTargetColumns(job),
			"connection": []map[string]interface{}{
				{
					"jdbcUrl": job.Target.GetJDBCURL(),
					"table":   []string{job.TargetTable},
				},
			},
			"batchSize": job.BatchSize,
		}
	}

	return writerConfig
}

// generateSettingConfig 生成设置配置
func (s *DataXService) generateSettingConfig(job *models.MigrationJob) map[string]interface{} {
	config := job.GetConfig()

	setting := map[string]interface{}{
		"speed": map[string]interface{}{
			"channel": config.Speed.Channel,
			"bytes":   config.Speed.Bytes,
			"records": config.Speed.Records,
		},
		"errorLimit": map[string]interface{}{
			"record":     config.ErrorLimit.Record,
			"percentage": config.ErrorLimit.Percentage,
		},
	}

	return setting
}

// getReaderPluginName 获取读取插件名称
func (s *DataXService) getReaderPluginName(dbType models.DatabaseType) string {
	supported := viper.GetStringMapString("supported_databases")
	if plugin, ok := supported[string(dbType)]; ok {
		parts := strings.Split(plugin, ",")
		if len(parts) > 0 {
			return parts[0] // reader
		}
	}
	return "streamreader"
}

// getWriterPluginName 获取写入插件名称
func (s *DataXService) getWriterPluginName(dbType models.DatabaseType) string {
	supported := viper.GetStringMapString("supported_databases")
	if plugin, ok := supported[string(dbType)]; ok {
		parts := strings.Split(plugin, ",")
		if len(parts) > 1 {
			return parts[1] // writer
		}
	}
	return "streamwriter"
}

// getColumnsForJob 获取字段列表
func (s *DataXService) getColumnsForJob(job *models.MigrationJob) []string {
	mappings := job.GetColumnMappings()
	if len(mappings) > 0 {
		columns := make([]string, len(mappings))
		for i, mapping := range mappings {
			columns[i] = mapping.SourceColumn
		}
		return columns
	}

	// 如果没有映射，返回所有字段
	return []string{"*"}
}

// getTargetColumns 获取目标字段列表
func (s *DataXService) getTargetColumns(job *models.MigrationJob) []string {
	mappings := job.GetColumnMappings()
	if len(mappings) > 0 {
		columns := make([]string, len(mappings))
		for i, mapping := range mappings {
			columns[i] = mapping.TargetColumn
		}
		return columns
	}
	return s.getColumnsForJob(job)
}

// getMongoDBColumns 获取MongoDB字段配置
func (s *DataXService) getMongoDBColumns(job *models.MigrationJob) []map[string]interface{} {
	mappings := job.GetColumnMappings()
	if len(mappings) == 0 {
		return []map[string]interface{}{{"name": "*", "type": "string"}}
	}

	columns := make([]map[string]interface{}, len(mappings))
	for i, mapping := range mappings {
		columns[i] = map[string]interface{}{
			"name": mapping.SourceColumn,
			"type": "string", // 简化处理
		}
	}
	return columns
}

// getSourceQuery 获取源查询语句
func (s *DataXService) getSourceQuery(job *models.MigrationJob) string {
	if job.SourceQuery != "" {
		return job.SourceQuery
	}

	mappings := job.GetColumnMappings()
	if len(mappings) > 0 {
		columns := make([]string, len(mappings))
		for i, mapping := range mappings {
			columns[i] = mapping.SourceColumn
		}
		return fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), job.SourceTable)
	}

	return fmt.Sprintf("SELECT * FROM %s", job.SourceTable)
}

// ExecuteJob 执行迁移任务
func (s *DataXService) ExecuteJob(job *models.MigrationJob) (*models.MigrationTask, error) {
	// 生成配置文件
	configFile, err := s.GenerateConfig(job)
	if err != nil {
		return nil, fmt.Errorf("生成配置文件失败: %v", err)
	}

	// 创建任务记录
	now := time.Now()
	task := &models.MigrationTask{
		JobID:      job.ID,
		TaskID:     fmt.Sprintf("job_%d_%d", job.ID, time.Now().Unix()),
		Status:     models.TaskStatusPending,
		ConfigFile: configFile,
		StartTime:  &now,
	}

	// 保存任务
	if s.db != nil {
		if err := s.db.Create(task).Error; err != nil {
			return nil, fmt.Errorf("创建任务记录失败: %v", err)
		}
	}

	// 异步执行
	go s.runDataXJob(job, task)

	return task, nil
}

// runDataXJob 执行DataX作业
func (s *DataXService) runDataXJob(job *models.MigrationJob, task *models.MigrationTask) {
	// 更新状态为运行中
	task.Status = models.TaskStatusRunning
	startTime := time.Now()
	task.StartTime = &startTime

	if s.db != nil {
		s.db.Save(task)
	}

	// 构建命令
	command := fmt.Sprintf("%s %s/bin/datax.py %s", s.pythonPath, s.basePath, task.ConfigFile)
	task.Command = command

	// 执行命令
	cmd := exec.Command("bash", "-c", command)

	// 设置输出
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// 开始执行
	err := cmd.Run()
	duration := time.Since(startTime)

	// 更新任务状态
	task.Duration = duration.Milliseconds() // 毫秒
	endTime := time.Now()
	task.EndTime = &endTime
	task.Output = stdout.String()

	if err != nil {
		task.Status = models.TaskStatusFailed
		task.ErrorMessage = err.Error()
		task.ErrorDetails = stderr.String()
	} else {
		task.Status = models.TaskStatusSuccess
		// 解析输出，提取统计信息
		s.parseDataXOutput(stdout.String(), task)
	}

	// 保存更新
	if s.db != nil {
		s.db.Save(task)
	}

	// 更新作业状态
	if s.db != nil {
		job.Status = models.JobStatusCompleted
		if task.Status == models.TaskStatusFailed {
			job.Status = models.JobStatusFailed
		}
		job.EndTime = task.EndTime
		// 关键修正：将 float64 转换为 int64
		job.Duration = int64(duration.Seconds())
		s.db.Save(job)
	}
}

// parseDataXOutput 解析DataX输出
func (s *DataXService) parseDataXOutput(output string, task *models.MigrationTask) {
	// 解析DataX的标准输出格式
	lines := strings.Split(output, "\n")

	for _, line := range lines {
		if strings.Contains(line, "任务总计") {
			// 解析统计信息
			// 示例: "任务总计                  :                 1000          |         1000          |"
			parts := strings.Split(line, "|")
			if len(parts) >= 2 {
				// 这里简化解析，实际需要更复杂的解析逻辑
				task.ReadRecords = 1000  // 简化
				task.WriteRecords = 1000 // 简化
			}
		} else if strings.Contains(line, "任务结束时刻") {
			// 解析结束时间
		}
	}

	// 计算速度
	if task.Duration > 0 {
		task.ReadSpeed = float64(task.ReadRecords) / (float64(task.Duration) / 1000.0)
		task.WriteSpeed = float64(task.WriteRecords) / (float64(task.Duration) / 1000.0)
	}
}

// StopJob 停止任务
func (s *DataXService) StopJob(jobID uint) error {
	// 查找并终止正在运行的任务
	// 这里需要根据实际情况实现进程管理
	return nil
}

// ValidateConfig 验证DataX配置
func (s *DataXService) ValidateConfig(config string) (bool, string) {
	// 尝试解析JSON
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(config), &data); err != nil {
		return false, fmt.Sprintf("JSON格式错误: %v", err)
	}

	// 检查必需字段
	if _, ok := data["job"]; !ok {
		return false, "缺少job字段"
	}

	job, ok := data["job"].(map[string]interface{})
	if !ok {
		return false, "job字段格式错误"
	}

	// 检查content
	if _, ok := job["content"]; !ok {
		return false, "缺少content字段"
	}

	return true, "配置验证通过"
}
