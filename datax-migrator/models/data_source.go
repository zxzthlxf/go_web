package models

import (
	"encoding/json"
	"strconv"
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
	Name      string       `json:"name" gorm:"size:100;not null;index"`
	Type      DatabaseType `json:"type" gorm:"size:50;not null;index"`
	Host      string       `json:"host" gorm:"size:100;not null"`
	Port      int          `json:"port" gorm:"not null"`
	Database  string       `json:"database" gorm:"size:100;not null"`
	Username  string       `json:"username" gorm:"size:100;not null"`
	Password  string       `json:"password,omitempty" gorm:"size:255"`
	Charset   string       `json:"charset" gorm:"size:50;default:'utf8mb4'"`
	Extra     string       `json:"extra,omitempty" gorm:"type:text"` // JSON格式的额外参数
	CreatedAt time.Time    `json:"created_at"`
	UpdatedAt time.Time    `json:"updated_at"`
}

// GetConnectionString 获取连接字符串（注意：密码可能包含特殊字符，建议上层进行URL编码）
func (ds *DataSource) GetConnectionString() string {
	portStr := strconv.Itoa(ds.Port) // 修正：将整数端口转换为字符串
	switch ds.Type {
	case MySQL:
		return ds.Username + ":" + ds.Password + "@tcp(" + ds.Host + ":" + portStr + ")/" + ds.Database + "?charset=" + ds.Charset
	case PostgreSQL:
		return "postgres://" + ds.Username + ":" + ds.Password + "@" + ds.Host + ":" + portStr + "/" + ds.Database + "?sslmode=disable"
	case Oracle:
		return ds.Username + "/" + ds.Password + "@" + ds.Host + ":" + portStr + "/" + ds.Database
	case SQLServer:
		return "sqlserver://" + ds.Username + ":" + ds.Password + "@" + ds.Host + ":" + portStr + "?database=" + ds.Database
	default:
		return ""
	}
}

// GetJDBCURL 获取JDBC URL
func (ds *DataSource) GetJDBCURL() string {
	portStr := strconv.Itoa(ds.Port) // 修正：将整数端口转换为字符串
	switch ds.Type {
	case MySQL:
		return "jdbc:mysql://" + ds.Host + ":" + portStr + "/" + ds.Database + "?useUnicode=true&characterEncoding=" + ds.Charset
	case PostgreSQL:
		return "jdbc:postgresql://" + ds.Host + ":" + portStr + "/" + ds.Database
	case Oracle:
		return "jdbc:oracle:thin:@" + ds.Host + ":" + portStr + ":" + ds.Database
	case SQLServer:
		return "jdbc:sqlserver://" + ds.Host + ":" + portStr + ";databaseName=" + ds.Database
	default:
		return ""
	}
}

// GetExtraParams 获取额外参数（解析JSON）
func (ds *DataSource) GetExtraParams() map[string]interface{} {
	var params map[string]interface{}
	if ds.Extra != "" {
		if err := json.Unmarshal([]byte(ds.Extra), &params); err != nil {
			// 解析失败时记录日志（可改为返回错误或空map）
			// 此处简单返回空map，并忽略错误
			params = make(map[string]interface{})
		}
	}
	if params == nil {
		params = make(map[string]interface{})
	}
	return params
}
