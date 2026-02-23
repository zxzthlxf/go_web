package services

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/sijms/go-ora/v2"
	"gorm.io/gorm"

	"datax-migrator/models"
)

// DataSourceService 数据源服务
type DataSourceService struct {
	db *gorm.DB
}

// NewDataSourceService 创建数据源服务
func NewDataSourceService(db *gorm.DB) *DataSourceService {
	return &DataSourceService{
		db: db,
	}
}

// CreateDataSource 创建数据源
func (s *DataSourceService) CreateDataSource(ds *models.DataSource) error {
	// 验证数据源连接
	if err := s.testConnection(ds); err != nil {
		return fmt.Errorf("数据源连接测试失败: %v", err)
	}

	ds.CreatedAt = time.Now()
	ds.UpdatedAt = time.Now()

	return s.db.Create(ds).Error
}

// UpdateDataSource 更新数据源
func (s *DataSourceService) UpdateDataSource(ds *models.DataSource) error {
	// 验证数据源连接
	if err := s.testConnection(ds); err != nil {
		return fmt.Errorf("数据源连接测试失败: %v", err)
	}

	ds.UpdatedAt = time.Now()

	return s.db.Save(ds).Error
}

// DeleteDataSource 删除数据源
func (s *DataSourceService) DeleteDataSource(id uint) error {
	// 检查是否有任务使用此数据源
	var count int64
	s.db.Model(&models.MigrationJob{}).
		Where("source_id = ? OR target_id = ?", id, id).
		Count(&count)

	if count > 0 {
		return fmt.Errorf("数据源被%d个任务使用，无法删除", count)
	}

	return s.db.Delete(&models.DataSource{}, id).Error
}

// GetDataSource 获取数据源
func (s *DataSourceService) GetDataSource(id uint) (*models.DataSource, error) {
	var ds models.DataSource
	if err := s.db.First(&ds, id).Error; err != nil {
		return nil, fmt.Errorf("数据源不存在")
	}
	return &ds, nil
}

// ListDataSources 获取数据源列表
func (s *DataSourceService) ListDataSources() ([]models.DataSource, error) {
	var dataSources []models.DataSource
	err := s.db.Order("created_at DESC").Find(&dataSources).Error
	return dataSources, err
}

// TestDataSource 测试数据源连接
func (s *DataSourceService) TestDataSource(ds *models.DataSource) error {
	return s.testConnection(ds)
}

// testConnection 测试数据库连接
func (s *DataSourceService) testConnection(ds *models.DataSource) error {
	db, err := s.openDB(ds)
	if err != nil {
		return err
	}
	defer db.Close()

	// 设置超时
	db.SetConnMaxLifetime(5 * time.Second)

	// 测试连接
	return db.Ping()
}

// openDB 根据数据源类型打开数据库连接
func (s *DataSourceService) openDB(ds *models.DataSource) (*sql.DB, error) {
	var driverName, connStr string
	var err error

	switch ds.Type {
	case models.MySQL:
		driverName = "mysql"
		connStr, err = s.buildMySQLConnStr(ds)
	case models.PostgreSQL:
		driverName = "postgres"
		connStr, err = s.buildPostgreSQLConnStr(ds)
	case models.Oracle:
		driverName = "oracle"
		connStr, err = s.buildOracleConnStr(ds)
	case models.SQLServer:
		driverName = "sqlserver"
		connStr, err = s.buildSQLServerConnStr(ds)
	default:
		return nil, fmt.Errorf("不支持的数据库类型: %s", ds.Type)
	}

	if err != nil {
		return nil, err
	}

	return sql.Open(driverName, connStr)
}

// buildMySQLConnStr 构建 MySQL 连接字符串（对密码进行编码）
func (s *DataSourceService) buildMySQLConnStr(ds *models.DataSource) (string, error) {
	// 对密码进行 URL 编码，避免特殊字符破坏 DSN
	encodedPassword := url.QueryEscape(ds.Password)
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=True&loc=Local",
		ds.Username, encodedPassword, ds.Host, ds.Port, ds.Database, ds.Charset)
	return connStr, nil
}

// buildPostgreSQLConnStr 构建 PostgreSQL 连接字符串
func (s *DataSourceService) buildPostgreSQLConnStr(ds *models.DataSource) (string, error) {
	encodedPassword := url.QueryEscape(ds.Password)
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		ds.Username, encodedPassword, ds.Host, ds.Port, ds.Database)
	return connStr, nil
}

// buildOracleConnStr 构建 Oracle 连接字符串
func (s *DataSourceService) buildOracleConnStr(ds *models.DataSource) (string, error) {
	// go-ora 驱动密码不需要编码，但为了安全，可以编码
	encodedPassword := url.QueryEscape(ds.Password)
	connStr := fmt.Sprintf("oracle://%s:%s@%s:%d/%s",
		ds.Username, encodedPassword, ds.Host, ds.Port, ds.Database)
	return connStr, nil
}

// buildSQLServerConnStr 构建 SQL Server 连接字符串
func (s *DataSourceService) buildSQLServerConnStr(ds *models.DataSource) (string, error) {
	// SQL Server 连接字符串中的密码需要特殊处理：如果包含 ; 等字符可能导致问题，但一般不会
	// 这里简单拼接，如果有问题可考虑使用 url.Values 构建
	connStr := fmt.Sprintf("server=%s;port=%d;database=%s;user id=%s;password=%s",
		ds.Host, ds.Port, ds.Database, ds.Username, ds.Password)
	return connStr, nil
}

// GetDatabaseTables 获取数据库表列表
func (s *DataSourceService) GetDatabaseTables(dataSourceID uint) ([]string, error) {
	ds, err := s.GetDataSource(dataSourceID)
	if err != nil {
		return nil, err
	}

	db, err := s.openDB(ds)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var query string
	switch ds.Type {
	case models.MySQL:
		query = "SHOW TABLES"
	case models.PostgreSQL:
		query = "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
	case models.Oracle:
		query = "SELECT table_name FROM user_tables"
	case models.SQLServer:
		query = "SELECT name FROM sys.tables"
	default:
		return nil, fmt.Errorf("不支持的数据库类型")
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err == nil {
			tables = append(tables, table)
		}
	}
	return tables, rows.Err()
}

// GetTableColumns 获取表字段信息
func (s *DataSourceService) GetTableColumns(dataSourceID uint, tableName string) ([]map[string]interface{}, error) {
	ds, err := s.GetDataSource(dataSourceID)
	if err != nil {
		return nil, err
	}

	db, err := s.openDB(ds)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var rows *sql.Rows
	var query string
	var args []interface{}

	switch ds.Type {
	case models.MySQL:
		// 使用 INFORMATION_SCHEMA 避免直接拼接表名
		query = `
			SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT, EXTRA
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
			ORDER BY ORDINAL_POSITION
		`
		args = []interface{}{ds.Database, tableName}
		rows, err = db.Query(query, args...)

	case models.PostgreSQL:
		query = `
			SELECT column_name, data_type, is_nullable, column_default
			FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = $1
			ORDER BY ordinal_position
		`
		args = []interface{}{tableName}
		rows, err = db.Query(query, args...)

	case models.Oracle:
		// Oracle 类似，使用 ALL_TAB_COLUMNS
		query = `
			SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_DEFAULT
			FROM ALL_TAB_COLUMNS
			WHERE OWNER = UPPER(?) AND TABLE_NAME = UPPER(?)
			ORDER BY COLUMN_ID
		`
		args = []interface{}{ds.Username, tableName}
		rows, err = db.Query(query, args...)

	case models.SQLServer:
		query = `
			SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_NAME = @p1
			ORDER BY ORDINAL_POSITION
		`
		args = []interface{}{tableName}
		rows, err = db.Query(query, args...)

	default:
		return nil, fmt.Errorf("暂不支持该数据库类型的字段查询")
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make([]map[string]interface{}, 0)
	for rows.Next() {
		switch ds.Type {
		case models.MySQL:
			var name, dataType, isNullable, columnKey, extra string
			var defaultValue sql.NullString
			if err := rows.Scan(&name, &dataType, &isNullable, &columnKey, &defaultValue, &extra); err == nil {
				columns = append(columns, map[string]interface{}{
					"name":        name,
					"type":        dataType,
					"nullable":    isNullable == "YES",
					"primary_key": strings.Contains(columnKey, "PRI"),
					"default":     defaultValue.String,
				})
			}
		case models.PostgreSQL:
			var name, dataType, isNullable string
			var defaultValue sql.NullString
			if err := rows.Scan(&name, &dataType, &isNullable, &defaultValue); err == nil {
				columns = append(columns, map[string]interface{}{
					"name":        name,
					"type":        dataType,
					"nullable":    isNullable == "YES",
					"primary_key": false, // PostgreSQL 需要额外查询主键，简化处理
					"default":     defaultValue.String,
				})
			}
		case models.Oracle:
			var name, dataType, nullable string
			var defaultValue sql.NullString
			if err := rows.Scan(&name, &dataType, &nullable, &defaultValue); err == nil {
				columns = append(columns, map[string]interface{}{
					"name":        name,
					"type":        dataType,
					"nullable":    nullable == "Y",
					"primary_key": false,
					"default":     defaultValue.String,
				})
			}
		case models.SQLServer:
			var name, dataType, isNullable string
			var defaultValue sql.NullString
			if err := rows.Scan(&name, &dataType, &isNullable, &defaultValue); err == nil {
				columns = append(columns, map[string]interface{}{
					"name":        name,
					"type":        dataType,
					"nullable":    isNullable == "YES",
					"primary_key": false,
					"default":     defaultValue.String,
				})
			}
		}
	}
	return columns, rows.Err()
}

// ExecuteQuery 执行SQL查询
func (s *DataSourceService) ExecuteQuery(dataSourceID uint, query string, limit int) ([]map[string]interface{}, error) {
	ds, err := s.GetDataSource(dataSourceID)
	if err != nil {
		return nil, err
	}

	db, err := s.openDB(ds)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// 添加 LIMIT 子句（如果支持且用户未提供）
	if limit > 0 {
		switch ds.Type {
		case models.MySQL, models.PostgreSQL, models.SQLServer:
			if !strings.Contains(strings.ToUpper(query), "LIMIT") {
				query += fmt.Sprintf(" LIMIT %d", limit)
			}
		case models.Oracle:
			if !strings.Contains(strings.ToUpper(query), "ROWNUM") {
				query = fmt.Sprintf("SELECT * FROM (%s) WHERE ROWNUM <= %d", query, limit)
			}
		}
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := make([]map[string]interface{}, 0)
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}
	return results, rows.Err()
}
