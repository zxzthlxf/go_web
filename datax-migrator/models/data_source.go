package models

import (
	"encoding/json"
	"time"
)

// 数据库类型
type DatabaseType string

const (
	MySQL         DatabaseType = "mysql"
	Oracle        DatabaseType = "oracle"
	PostgreSQL    DatabaseType = "postgresql"
	SQLServer     DatabaseType = "sqlserver"
	MongoDB       DatabaseType = "mongodb"
	Redis         DatabaseType = "redis"
	ElasticSearch DatabaseType = "elasticsearch"
	HDFS          DatabaseType = "hdfs"
	Hive          DatabaseType = "hive"
	HBase         DatabaseType = "hbase"
	FTP           DatabaseType = "ftp"
)

// DataSource 数据源配置
type DataSource struct {
	ID        uint         `json:"id" gorm:"primaryKey"`
	Name      string       `json:"name" gorm:"size:100;not null"`
	Type      DatabaseType `json:"type" gorm:"size:50;not null"`
	Host      string       `json:"host" gorm:"size:100"`
	Port      int          `json:"port"`
	Database  string       `json:"database" gorm:"size:100"`
	Username  string       `json:"username" gorm:"size:100"`
	Password  string       `json:"password,omitempty" gorm:"size:255"`
	Charset   string       `json:"charset" gorm:"size:50;default:'utf8mb4'"`
	Extra     string       `json:"extra" gorm:"type:text"` // JSON格式的额外参数
	CreatedAt time.Time    `json:"created_at"`
	UpdatedAt time.Time    `json:"updated_at"`
}

// GetConnectionString 获取连接字符串
func (ds *DataSource) GetConnectionString() string {
	switch ds.Type {
	case MySQL:
		return ds.Username + ":" + ds.Password + "@tcp(" + ds.Host + ":" + string(rune(ds.Port)) + ")/" + ds.Database + "?charset=" + ds.Charset
	case PostgreSQL:
		return "postgres://" + ds.Username + ":" + ds.Password + "@" + ds.Host + ":" + string(rune(ds.Port)) + "/" + ds.Database + "?sslmode=disable"
	case Oracle:
		return ds.Username + "/" + ds.Password + "@" + ds.Host + ":" + string(rune(ds.Port)) + "/" + ds.Database
	case SQLServer:
		return "sqlserver://" + ds.Username + ":" + ds.Password + "@" + ds.Host + ":" + string(rune(ds.Port)) + "?database=" + ds.Database
	default:
		return ""
	}
}

// GetJDBCURL 获取JDBC URL
func (ds *DataSource) GetJDBCURL() string {
	switch ds.Type {
	case MySQL:
		return "jdbc:mysql://" + ds.Host + ":" + string(rune(ds.Port)) + "/" + ds.Database + "?useUnicode=true&characterEncoding=" + ds.Charset
	case PostgreSQL:
		return "jdbc:postgresql://" + ds.Host + ":" + string(rune(ds.Port)) + "/" + ds.Database
	case Oracle:
		return "jdbc:oracle:thin:@" + ds.Host + ":" + string(rune(ds.Port)) + ":" + ds.Database
	case SQLServer:
		return "jdbc:sqlserver://" + ds.Host + ":" + string(rune(ds.Port)) + ";databaseName=" + ds.Database
	default:
		return ""
	}
}

// GetExtraParams 获取额外参数
func (ds *DataSource) GetExtraParams() map[string]interface{} {
	var params map[string]interface{}
	if ds.Extra != "" {
		json.Unmarshal([]byte(ds.Extra), &params)
	}
	if params == nil {
		params = make(map[string]interface{})
	}
	return params
}
