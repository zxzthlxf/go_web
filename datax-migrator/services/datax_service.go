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

	log.Printf("生成配置: jobID=%d", job.ID)

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
		// 优先取第一个作为reader
		if len(parts) >= 1 && parts[0] != "" {
			return parts[0]
		}
		// 如果只有一个部分且以reader结尾，直接使用
		if len(parts) == 1 && strings.HasSuffix(parts[0], "reader") {
			return parts[0]
		}
		// 如果只有一个部分且不是reader，尝试将writer转换为reader
		if len(parts) == 1 && strings.HasSuffix(parts[0], "writer") {
			return strings.TrimSuffix(parts[0], "writer") + "reader"
		}
	}
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
		log.Printf("未知数据库类型 %s，使用streamreader作为默认reader", dbType)
		return "streamreader"
	}
}

// getWriterPluginName 获取写入插件名称
func (s *DataXService) getWriterPluginName(dbType models.DatabaseType) string {
	supported := viper.GetStringMapString("supported_databases")
	if plugin, ok := supported[string(dbType)]; ok {
		parts := strings.Split(plugin, ",")
		// 如果配置有两个部分，第二部分是writer
		if len(parts) >= 2 && parts[1] != "" {
			return parts[1]
		}
		// 如果配置只有一个部分，检查它是否以 "writer" 结尾
		if len(parts) == 1 && strings.HasSuffix(parts[0], "writer") {
			return parts[0]
		}
		// 如果只有一个部分且不是writer，尝试将reader转换为writer（例如mysqlreader -> mysqlwriter）
		if len(parts) == 1 && strings.HasSuffix(parts[0], "reader") {
			return strings.TrimSuffix(parts[0], "reader") + "writer"
		}
	}
	// 根据常见类型返回默认writer
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
		log.Printf("未知数据库类型 %s，使用streamwriter作为默认writer", dbType)
		return "streamwriter"
	}
}

// getColumnsForJob 获取字段列表
// getColumnsForJob 获取字段列表
func (s *DataXService) getColumnsForJob(job *models.MigrationJob) []string {
	mappings := job.GetColumnMappings()
	if len(mappings) > 0 {
		columns := make([]string, 0, len(mappings))
		for _, mapping := range mappings {
			if mapping.SourceColumn != "" { // 只添加非空字段
				columns = append(columns, mapping.SourceColumn)
			}
		}
		if len(columns) > 0 {
			return columns
		}
	}
	// 如果没有映射，返回所有字段
	return []string{"*"}
}

// getTargetColumns 获取目标字段列表
func (s *DataXService) getTargetColumns(job *models.MigrationJob) []string {
	mappings := job.GetColumnMappings()
	if len(mappings) > 0 {
		columns := make([]string, 0, len(mappings))
		for _, mapping := range mappings {
			if mapping.TargetColumn != "" { // 只添加非空字段
				columns = append(columns, mapping.TargetColumn)
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

	columns := s.getColumnsForJob(job)
	// 如果列列表为空（可能是所有字段被过滤掉了），则使用 "*"
	if len(columns) == 0 || (len(columns) == 1 && columns[0] == "") {
		columns = []string{"*"}
	}

	// 构建 SELECT 语句
	if len(columns) == 1 && columns[0] == "*" {
		return fmt.Sprintf("SELECT * FROM %s", job.SourceTable)
	}

	// 对字段名进行可能的引号处理（可选）
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = "`" + col + "`" // MySQL风格，可根据数据库类型调整
	}
	return fmt.Sprintf("SELECT %s FROM %s", strings.Join(quotedColumns, ", "), job.SourceTable)
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
	// 使用 defer 捕获 panic，确保状态能够更新
	defer func() {
		if r := recover(); r != nil {
			log.Printf("runDataXJob panic: %v", r)
			task.Status = models.TaskStatusFailed
			task.ErrorMessage = fmt.Sprintf("内部错误: %v", r)
			now := time.Now()
			task.EndTime = &now
			if s.db != nil {
				s.db.Save(task)
			}
		}
	}()

	// 1. 更新状态为运行中
	task.Status = models.TaskStatusRunning
	startTime := time.Now()
	task.StartTime = &startTime
	if s.db != nil {
		if err := s.db.Save(task).Error; err != nil {
			log.Printf("保存任务运行状态失败: %v", err)
		}
	}

	// 2. 构建命令（跨平台）
	var cmd *exec.Cmd
	var commandLine string

	if runtime.GOOS == "windows" {
		// Windows 使用 cmd /c
		commandLine = fmt.Sprintf("%s %s\\bin\\datax.py %s", s.pythonPath, s.basePath, task.ConfigFile)
		cmd = exec.Command("cmd", "/c", commandLine)
	} else {
		// Linux/Mac 使用 bash -c
		commandLine = fmt.Sprintf("%s %s/bin/datax.py %s", s.pythonPath, s.basePath, task.ConfigFile)
		cmd = exec.Command("bash", "-c", commandLine)
	}

	task.Command = strings.Join(cmd.Args, " ")
	log.Printf("执行命令: %s", task.Command)

	// 3. 设置输出捕获
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// 4. 执行命令
	err := cmd.Run()
	duration := time.Since(startTime)

	// 5. 更新任务状态和结果
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
		// 解析 DataX 输出，提取统计信息
		s.parseDataXOutput(stdout.String(), task)
	}

	// 6. 保存任务更新
	if s.db != nil {
		if err := s.db.Save(task).Error; err != nil {
			log.Printf("保存任务最终状态失败: %v", err)
		}
	}

	// 7. 更新作业的总体状态
	if s.db != nil {
		job.Status = models.JobStatusCompleted
		if task.Status == models.TaskStatusFailed {
			job.Status = models.JobStatusFailed
		} else if task.Status == models.TaskStatusSuccess {
			job.Status = models.JobStatusCompleted
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
	// 示例：简单提取记录数
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if strings.Contains(line, "任务启动时刻") {
			// 可以记录日志
			if s.db != nil {
				s.db.Create(&models.TaskLog{
					TaskID:    task.ID,
					Level:     "INFO",
					Message:   line,
					Timestamp: time.Now(),
				})
			}
		}
		if strings.Contains(line, "任务结束时刻") {
			// 解析结束时间
		}
		if strings.Contains(line, "任务总计") {
			// 尝试解析记录数
			// 示例： "任务总计                 | 1000 | 1000 |"
			// 简化处理，您需要根据实际输出格式调整
			parts := strings.Split(line, "|")
			if len(parts) >= 2 {
				// 这里简化解析，实际需要更复杂的解析逻辑
				task.ReadRecords = 1000  // 简化
				task.WriteRecords = 1000 // 简化
			}
		}
		// 计算速度
		if task.Duration > 0 {
			task.ReadSpeed = float64(task.ReadRecords) / (float64(task.Duration) / 1000.0)
			task.WriteSpeed = float64(task.WriteRecords) / (float64(task.Duration) / 1000.0)
		}

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
