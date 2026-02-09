package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"zk-manager/handlers"
	"zk-manager/zkclient"

	"github.com/gin-gonic/gin"
)

func main() {
	var port int
	flag.IntVar(&port, "port", 8012, "web service port")
	flag.Parse()

	if port < 1 || port > 65535 {
		log.Fatalf("invalid port: %d", port)
	}

	gin.SetMode(gin.ReleaseMode)
	// 初始化Gin
	r := gin.Default()

	// 加载模板文件
	r.LoadHTMLGlob("templates/*")

	// 提供静态文件服务
	r.Static("/static", "./static")

	// 初始化ZooKeeper客户端
	zkClient := zkclient.NewZKClient()
	defer zkClient.Close()

	// 注册路由
	registerRoutes(r, zkClient)

	// 启动服务器

	listenAddr := fmt.Sprintf(":%d", port)
	log.Printf("ZooKeeper Web管理工具启动成功！")
	log.Printf("监听端口: %d", port)
	log.Printf("访问地址: http://localhost:%d", port)

	if err := r.Run(listenAddr); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}

func registerRoutes(r *gin.Engine, zkClient *zkclient.ZKClient) {
	// 首页
	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", gin.H{})
	})

	// API路由组
	api := r.Group("/api")
	{
		// 连接管理
		api.POST("/connect", handlers.ConnectHandler(zkClient))
		api.POST("/disconnect", handlers.DisconnectHandler(zkClient))
		api.GET("/status", handlers.StatusHandler(zkClient))
		api.GET("/connection/stats", handlers.ConnectionStatsHandler(zkClient))

		// 节点操作
		api.GET("/nodes", handlers.GetNodesHandler(zkClient))
		api.GET("/node/*path", handlers.GetNodeHandler(zkClient))
		api.POST("/node/*path", handlers.CreateNodeHandler(zkClient))
		api.PUT("/node/*path", handlers.SetNodeHandler(zkClient))
		api.DELETE("/node/*path", handlers.DeleteNodeHandler(zkClient))

		// 监控
		api.GET("/watch/*path", handlers.WatchNodeHandler(zkClient))
		api.GET("/stats", handlers.StatsHandler(zkClient))

		// ACL操作
		api.GET("/acl/*path", handlers.GetACLHandler(zkClient))
		api.POST("/acl/*path", handlers.SetACLHandler(zkClient))

		// 四字命令
		api.GET("/four-letter/:cmd", handlers.FourLetterCommandHandler(zkClient))
	}

	// Web页面路由
	r.GET("/view/*path", func(c *gin.Context) {
		path := c.Param("path")
		if path == "" {
			path = "/"
		}
		c.HTML(http.StatusOK, "node.html", gin.H{
			"path": path,
		})
	})
}
