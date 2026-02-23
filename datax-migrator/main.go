package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"datax-migrator/handlers"
	"datax-migrator/models"
	"datax-migrator/services"
)

func initConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")

	// 设置默认配置
	viper.SetDefault("server.port", "8080")
	viper.SetDefault("datax.path", "/home/koca/datax")
	viper.SetDefault("database.path", "data/migrator.db")
	viper.SetDefault("log.path", "logs")
	viper.SetDefault("max_workers", 5)

	if err := viper.ReadInConfig(); err != nil {
		log.Printf("配置文件读取失败，使用默认配置: %v", err)
	}
}

func initDatabase() *gorm.DB {
	dbPath := viper.GetString("database.path")

	// 确保目录存在
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal("创建数据库目录失败:", err)
	}

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		log.Fatal("数据库连接失败:", err)
	}

	// 自动迁移表结构
	db.AutoMigrate(
		&models.DataSource{},
		&models.MigrationJob{},
		&models.MigrationTask{},
		&models.TaskLog{},
	)

	return db
}

func main() {
	// 初始化配置
	initConfig()

	// 初始化数据库
	db := initDatabase()

	// 初始化服务
	dataxService := services.NewDataXService()
	jobService := services.NewJobService(db, dataxService)
	dataSourceService := services.NewDataSourceService(db)

	// 设置Gin模式
	if viper.GetString("mode") == "release" {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.Default()

	// 加载模板
	r.LoadHTMLGlob("templates/*")

	// 静态文件
	r.Static("/static", "./static")
	r.Static("/uploads", "./uploads")

	// API路由
	api := r.Group("/api/v1")
	{
		// 数据源管理
		api.GET("/data-sources", handlers.ListDataSources(dataSourceService))
		api.GET("/data-sources/:id", handlers.GetDataSource(dataSourceService))
		api.POST("/data-sources", handlers.CreateDataSource(dataSourceService))
		api.PUT("/data-sources/:id", handlers.UpdateDataSource(dataSourceService))
		api.DELETE("/data-sources/:id", handlers.DeleteDataSource(dataSourceService))
		api.POST("/data-sources/:id/test", handlers.TestDataSource(dataSourceService))
		api.POST("/data-sources/test", handlers.TestDataSourceConnection(dataSourceService))

		// 迁移任务管理
		api.GET("/jobs", handlers.ListJobs(jobService))
		api.GET("/jobs/:id", handlers.GetJob(jobService))
		api.POST("/jobs", handlers.CreateJob(jobService))
		api.PUT("/jobs/:id", handlers.UpdateJob(jobService))
		api.DELETE("/jobs/:id", handlers.DeleteJob(jobService))
		api.POST("/jobs/:id/run", handlers.RunJob(jobService))
		api.POST("/jobs/:id/stop", handlers.StopJob(jobService))
		api.GET("/jobs/:id/tasks", handlers.GetJobTasks(jobService))

		// 任务执行
		api.GET("/tasks", handlers.ListTasks(jobService))
		api.GET("/tasks/:id", handlers.GetTask(jobService))
		api.GET("/tasks/:id/logs", handlers.GetTaskLogs(jobService))

		// DataX相关
		api.GET("/datax/reader-plugins", handlers.GetReaderPlugins(dataxService))
		api.GET("/datax/writer-plugins", handlers.GetWriterPlugins(dataxService))
		api.POST("/datax/generate-config", handlers.GenerateDataXConfig(dataxService))
		api.POST("/datax/validate-config", handlers.ValidateDataXConfig(dataxService))

		// 数据库操作
		api.GET("/databases/tables", handlers.GetDatabaseTables(dataSourceService))
		api.GET("/databases/columns", handlers.GetTableColumns(dataSourceService))
		api.POST("/databases/query", handlers.ExecuteQuery(dataSourceService))
	}

	// 页面路由
	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", gin.H{
			"title": "DataX 数据迁移工具",
		})
	})

	r.GET("/job/:id", func(c *gin.Context) {
		c.HTML(http.StatusOK, "job_detail.html", gin.H{})
	})

	// 启动服务器
	port := viper.GetString("server.port")
	log.Printf("DataX迁移工具启动在 http://localhost:%s", port)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil {
		log.Fatal("服务器启动失败:", err)
	}
}
