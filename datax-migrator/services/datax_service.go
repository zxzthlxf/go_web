package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
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
	// 可扩展为从DataX目录动态读取
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

	log.Printf("生成配置文件: %s", configFile)
	return configFile, nil
}

// generateReaderConfig 生成读取器配置
func (s *DataXService) generateReaderConfig(job *models.MigrationJob) map[string]interface{} {
	readerConfig := map[string]interface{}{
		"name": s.getReaderPluginName(job.Source.Type),
	}

	// 根据数据库类型设置具体参数
	switch job.Source.Type {
	case models.MySQL, models.PostgreSQL, models.Oracle:
		readerConfig["parameter"] = map[string]interface{}{
			"username": job.Source.Username,
			"password": job.Source.Password,
			"column":   s.getColumnsForJob(job),
			"connection": []map[string]interface{}{
				{
					"query":   s.getSourceQuery(job),   // DataX中query应为字符串
					"jdbcUrl": job.Source.GetJDBCURL(), // 字符串而非数组
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

	switch job.Target.Type {
	case models.MySQL, models.PostgreSQL, models.Oracle:
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
			"preSql":    []string{},
			"postSql":   []string{},
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
		if len(parts) >= 1 && parts[0] != "" {
			return parts[0]
		}
		if len(parts) == 1 && strings.HasSuffix(parts[0], "reader") {
			return parts[0]
		}
	}
	// 默认映射
	switch dbType {
	case models.MySQL:
		return "mysqlreader"
	case models.PostgreSQL:
		return "postgresqlreader"
	case models.Oracle:
		return "oraclereader"
	case models.SQLServer:
		return "sqlserverreader"
	case models.MongoDB:
		return "mongodbreader"
	default:
		log.Printf("未知数据库类型 %s，使用streamreader", dbType)
		return "streamreader"
	}
}

// getWriterPluginName 获取写入插件名称
func (s *DataXService) getWriterPluginName(dbType models.DatabaseType) string {
	supported := viper.GetStringMapString("supported_databases")
	if plugin, ok := supported[string(dbType)]; ok {
		parts := strings.Split(plugin, ",")
		if len(parts) >= 2 && parts[1] != "" {
			return parts[1]
		}
		if len(parts) == 1 && strings.HasSuffix(parts[0], "writer") {
			return parts[0]
		}
	}
	switch dbType {
	case models.MySQL:
		return "mysqlwriter"
	case models.PostgreSQL:
		return "postgresqlwriter"
	case models.Oracle:
		return "oraclewriter"
	case models.SQLServer:
		return "sqlserverwriter"
	case models.MongoDB:
		return "mongodbwriter"
	default:
		log.Printf("未知数据库类型 %s，使用streamwriter", dbType)
		return "streamwriter"
	}
}

// getColumnsForJob 获取字段列表（源）
func (s *DataXService) getColumnsForJob(job *models.MigrationJob) []string {
	mappings := job.GetColumnMappings()
	if len(mappings) > 0 {
		columns := make([]string, 0, len(mappings))
		for _, m := range mappings {
			if m.SourceColumn != "" {
				columns = append(columns, m.SourceColumn)
			}
		}
		if len(columns) > 0 {
			return columns
		}
	}
	return []string{"*"}
}

// getTargetColumns 获取目标字段列表
func (s *DataXService) getTargetColumns(job *models.MigrationJob) []string {
	mappings := job.GetColumnMappings()
	if len(mappings) > 0 {
		columns := make([]string, 0, len(mappings))
		for _, m := range mappings {
			if m.TargetColumn != "" {
				columns = append(columns, m.TargetColumn)
			}
		}
		if len(columns) > 0 {
			return columns
		}
	}
	return s.getColumnsForJob(job)
}

// getMongoDBColumns 获取MongoDB字段配置
func (s *DataXService) getMongoDBColumns(job *models.MigrationJob) []map[string]interface{} {
	mappings := job.GetColumnMappings()
	if len(mappings) == 0 {
		return []map[string]interface{}{{"name": "*", "type": "string"}}
	}
	cols := make([]map[string]interface{}, len(mappings))
	for i, m := range mappings {
		cols[i] = map[string]interface{}{
			"name": m.SourceColumn,
			"type": "string",
		}
	}
	return cols
}

// getSourceQuery 获取源查询语句
func (s *DataXService) getSourceQuery(job *models.MigrationJob) string {
	if job.SourceQuery != "" {
		return job.SourceQuery
	}
	columns := s.getColumnsForJob(job)
	if len(columns) == 0 || (len(columns) == 1 && columns[0] == "*") {
		return fmt.Sprintf("SELECT * FROM %s", job.SourceTable)
	}
	// 根据数据库类型决定是否加引号
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = "`" + col + "`" // MySQL风格，其他数据库可能需要双引号
	}
	return fmt.Sprintf("SELECT %s FROM %s", strings.Join(quoted, ", "), job.SourceTable)
}

// ExecuteJob 执行迁移任务
func (s *DataXService) ExecuteJob(job *models.MigrationJob) (*models.MigrationTask, error) {
	configFile, err := s.GenerateConfig(job)
	if err != nil {
		return nil, fmt.Errorf("生成配置文件失败: %v", err)
	}

	now := time.Now()
	task := &models.MigrationTask{
		JobID:      job.ID,
		TaskID:     fmt.Sprintf("job_%d_%d", job.ID, time.Now().Unix()),
		Status:     models.TaskStatusPending,
		ConfigFile: configFile,
		StartTime:  &now,
	}

	if s.db != nil {
		if err := s.db.Create(task).Error; err != nil {
			return nil, fmt.Errorf("创建任务记录失败: %v", err)
		}
	}

	go s.runDataXJob(job, task)
	return task, nil
}

// runDataXJob 执行DataX作业
func (s *DataXService) runDataXJob(job *models.MigrationJob, task *models.MigrationTask) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("runDataXJob panic: %v", r)
			task.Status = models.TaskStatusFailed
			task.ErrorMessage = fmt.Sprintf("panic: %v", r)
			now := time.Now()
			task.EndTime = &now
			if s.db != nil {
				s.db.Save(task)
			}
		}
	}()

	// 更新状态为运行中
	task.Status = models.TaskStatusRunning
	startTime := time.Now()
	task.StartTime = &startTime
	if s.db != nil {
		if err := s.db.Save(task).Error; err != nil {
			log.Printf("保存任务运行状态失败: %v", err)
		}
	}

	// 构建命令
	dataxPy := filepath.Join(s.basePath, "bin", "datax.py")
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/c", s.pythonPath, dataxPy, task.ConfigFile)
	} else {
		cmd = exec.Command(s.pythonPath, dataxPy, task.ConfigFile)
	}

	task.Command = strings.Join(cmd.Args, " ")
	log.Printf("执行命令: %s", task.Command)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// 执行命令
	err := cmd.Run()
	duration := time.Since(startTime)

	task.Duration = duration.Milliseconds()
	endTime := time.Now()
	task.EndTime = &endTime
	task.Output = stdout.String()
	task.ErrorDetails = stderr.String()

	if err != nil {
		task.Status = models.TaskStatusFailed
		task.ErrorMessage = err.Error()
		log.Printf("命令执行失败: %v, stderr: %s", err, stderr.String())
	} else {
		task.Status = models.TaskStatusSuccess
		s.parseDataXOutput(stdout.String(), task)
	}

	// 保存任务更新
	if s.db != nil {
		if err := s.db.Save(task).Error; err != nil {
			log.Printf("保存任务最终状态失败: %v", err)
		}
	}

	// 更新作业状态
	if s.db != nil {
		if task.Status == models.TaskStatusSuccess {
			job.Status = models.JobStatusCompleted
		} else {
			job.Status = models.JobStatusFailed
		}
		job.EndTime = &endTime
		job.Duration = int64(duration.Seconds())
		if err := s.db.Save(job).Error; err != nil {
			log.Printf("保存作业状态失败: %v", err)
		}
	}
}

// parseDataXOutput 解析DataX输出
func (s *DataXService) parseDataXOutput(output string, task *models.MigrationTask) {
	lines := strings.Split(output, "\n")
	read, write := int64(0), int64(0)

	for _, line := range lines {
		if strings.Contains(line, "任务总计") {
			// 简单提取记录数，可根据实际输出格式调整
			parts := strings.Fields(line)
			for i, part := range parts {
				if part == "|" && i+1 < len(parts) {
					if read == 0 {
						read, _ = strconv.ParseInt(parts[i+1], 10, 64)
					} else if write == 0 {
						write, _ = strconv.ParseInt(parts[i+1], 10, 64)
						break
					}
				}
			}
		}
	}

	task.ReadRecords = read
	task.WriteRecords = write
	if task.Duration > 0 {
		task.ReadSpeed = float64(read) / (float64(task.Duration) / 1000.0)
		task.WriteSpeed = float64(write) / (float64(task.Duration) / 1000.0)
	}

	// 可选的日志记录
	if s.db != nil && task.ID > 0 {
		s.db.Create(&models.TaskLog{
			TaskID:    task.ID,
			Level:     "INFO",
			Message:   "DataX执行完成，读取记录：" + strconv.FormatInt(read, 10),
			Timestamp: time.Now(),
		})
	}
}

// StopJob 停止任务（待实现）
func (s *DataXService) StopJob(jobID uint) error {
	// 此处可实现进程管理
	return nil
}

// ValidateConfig 验证DataX配置
func (s *DataXService) ValidateConfig(config string) (bool, string) {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(config), &data); err != nil {
		return false, fmt.Sprintf("JSON格式错误: %v", err)
	}
	if _, ok := data["job"]; !ok {
		return false, "缺少job字段"
	}
	job, ok := data["job"].(map[string]interface{})
	if !ok {
		return false, "job字段格式错误"
	}
	if _, ok := job["content"]; !ok {
		return false, "缺少content字段"
	}
	return true, "配置验证通过"
}
