package services

import (
	"database/sql"
	"fmt"
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
	var db *sql.DB
	var err error

	switch ds.Type {
	case models.MySQL:
		connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=True&loc=Local",
			ds.Username, ds.Password, ds.Host, ds.Port, ds.Database, ds.Charset)
		db, err = sql.Open("mysql", connStr)

	case models.PostgreSQL:
		connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
			ds.Username, ds.Password, ds.Host, ds.Port, ds.Database)
		db, err = sql.Open("postgres", connStr)

	case models.Oracle:
		connStr := fmt.Sprintf("oracle://%s:%s@%s:%d/%s",
			ds.Username, ds.Password, ds.Host, ds.Port, ds.Database)
		db, err = sql.Open("oracle", connStr)

	case models.SQLServer:
		// SQL Server连接字符串
		connStr := fmt.Sprintf("server=%s;port=%d;database=%s;user id=%s;password=%s",
			ds.Host, ds.Port, ds.Database, ds.Username, ds.Password)
		db, err = sql.Open("sqlserver", connStr)

	default:
		return fmt.Errorf("不支持的数据库类型: %s", ds.Type)
	}

	if err != nil {
		return err
	}
	defer db.Close()

	// 设置超时
	db.SetConnMaxLifetime(5 * time.Second)

	// 测试连接
	return db.Ping()
}

// GetDatabaseTables 获取数据库表列表
func (s *DataSourceService) GetDatabaseTables(dataSourceID uint) ([]string, error) {
	ds, err := s.GetDataSource(dataSourceID)
	if err != nil {
		return nil, err
	}

	var db *sql.DB
	var query string

	switch ds.Type {
	case models.MySQL:
		db, err = s.connectMySQL(ds)
		query = "SHOW TABLES"
	case models.PostgreSQL:
		db, err = s.connectPostgreSQL(ds)
		query = "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
	case models.Oracle:
		db, err = s.connectOracle(ds)
		query = "SELECT table_name FROM user_tables"
	case models.SQLServer:
		db, err = s.connectSQLServer(ds)
		query = "SELECT name FROM sys.tables"
	default:
		return nil, fmt.Errorf("不支持的数据库类型")
	}

	if err != nil {
		return nil, err
	}
	defer db.Close()

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

	return tables, nil
}

// GetTableColumns 获取表字段信息
func (s *DataSourceService) GetTableColumns(dataSourceID uint, tableName string) ([]map[string]interface{}, error) {
	ds, err := s.GetDataSource(dataSourceID)
	if err != nil {
		return nil, err
	}

	var db *sql.DB
	var query string

	switch ds.Type {
	case models.MySQL:
		db, err = s.connectMySQL(ds)
		query = fmt.Sprintf("DESCRIBE %s", tableName)
	case models.PostgreSQL:
		db, err = s.connectPostgreSQL(ds)
		query = fmt.Sprintf(`
			SELECT column_name, data_type, is_nullable, column_default
			FROM information_schema.columns
			WHERE table_name = '%s'
			ORDER BY ordinal_position
		`, tableName)
	default:
		return nil, fmt.Errorf("暂不支持该数据库类型的字段查询")
	}

	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make([]map[string]interface{}, 0)

	for rows.Next() {
		switch ds.Type {
		case models.MySQL:
			var field, typeStr, null, key, extra string
			var defaultValue sql.NullString
			if err := rows.Scan(&field, &typeStr, &null, &key, &defaultValue, &extra); err == nil {
				columns = append(columns, map[string]interface{}{
					"name":        field,
					"type":        typeStr,
					"nullable":    null == "YES",
					"primary_key": strings.Contains(key, "PRI"),
					"default":     defaultValue.String,
				})
			}
		case models.PostgreSQL:
			var columnName, dataType, isNullable string
			var columnDefault sql.NullString
			if err := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault); err == nil {
				columns = append(columns, map[string]interface{}{
					"name":     columnName,
					"type":     dataType,
					"nullable": isNullable == "YES",
					"default":  columnDefault.String,
				})
			}
		}
	}

	return columns, nil
}

// ExecuteQuery 执行SQL查询
func (s *DataSourceService) ExecuteQuery(dataSourceID uint, query string, limit int) ([]map[string]interface{}, error) {
	ds, err := s.GetDataSource(dataSourceID)
	if err != nil {
		return nil, err
	}

	var db *sql.DB

	switch ds.Type {
	case models.MySQL:
		db, err = s.connectMySQL(ds)
	case models.PostgreSQL:
		db, err = s.connectPostgreSQL(ds)
	default:
		return nil, fmt.Errorf("不支持的数据库类型")
	}

	if err != nil {
		return nil, err
	}
	defer db.Close()

	// 添加限制
	if limit > 0 {
		if strings.Contains(strings.ToUpper(query), "LIMIT") {
			// 如果已有LIMIT，不添加
		} else if ds.Type == models.MySQL {
			query += fmt.Sprintf(" LIMIT %d", limit)
		} else if ds.Type == models.PostgreSQL {
			query += fmt.Sprintf(" LIMIT %d", limit)
		}
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// 准备结果切片
	results := make([]map[string]interface{}, 0)

	for rows.Next() {
		// 创建值的切片
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 扫描行
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// 创建行映射
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

	return results, nil
}

// 连接辅助方法
func (s *DataSourceService) connectMySQL(ds *models.DataSource) (*sql.DB, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s",
		ds.Username, ds.Password, ds.Host, ds.Port, ds.Database, ds.Charset)
	return sql.Open("mysql", connStr)
}

func (s *DataSourceService) connectPostgreSQL(ds *models.DataSource) (*sql.DB, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		ds.Username, ds.Password, ds.Host, ds.Port, ds.Database)
	return sql.Open("postgres", connStr)
}

func (s *DataSourceService) connectOracle(ds *models.DataSource) (*sql.DB, error) {
	connStr := fmt.Sprintf("oracle://%s:%s@%s:%d/%s",
		ds.Username, ds.Password, ds.Host, ds.Port, ds.Database)
	return sql.Open("oracle", connStr)
}

func (s *DataSourceService) connectSQLServer(ds *models.DataSource) (*sql.DB, error) {
	connStr := fmt.Sprintf("server=%s;port=%d;database=%s;user id=%s;password=%s",
		ds.Host, ds.Port, ds.Database, ds.Username, ds.Password)
	return sql.Open("sqlserver", connStr)
}
