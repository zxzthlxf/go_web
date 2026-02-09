package handlers

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"zk-manager/zkclient"

	"github.com/gin-gonic/gin"
	"github.com/samuel/go-zookeeper/zk"
)

type ConnectRequest struct {
	Servers []string `json:"servers"`
	Timeout int      `json:"timeout"` // 秒
}

type CreateNodeRequest struct {
	Data string `json:"data"`
	Mode string `json:"mode"` // "persistent", "ephemeral", "sequential"
}

type SetNodeRequest struct {
	Data    string `json:"data"`
	Version int32  `json:"version"`
}

type ACLRequest struct {
	Scheme string `json:"scheme"`
	ID     string `json:"id"`
	Perms  int32  `json:"perms"`
}

func ConnectHandler(zkClient *zkclient.ZKClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req ConnectRequest
		if err := c.BindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的请求参数"})
			return
		}

		if len(req.Servers) == 0 {
			req.Servers = []string{"localhost:2181"}
		}

		timeout := time.Duration(req.Timeout) * time.Second
		if timeout == 0 {
			timeout = 5 * time.Second
		}

		err := zkClient.Connect(req.Servers, timeout)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message": "连接成功",
			"servers": req.Servers,
			"timeout": timeout.Seconds(),
		})
	}
}

func DisconnectHandler(zkClient *zkclient.ZKClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		zkClient.Close()
		c.JSON(http.StatusOK, gin.H{"message": "已断开连接"})
	}
}

func StatusHandler(zkClient *zkclient.ZKClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"connected": zkClient.IsConnected(),
			"servers":   zkClient.GetServers(),
			"state":     zkClient.GetState(),
		})
	}
}

func GetNodesHandler(zkClient *zkclient.ZKClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.DefaultQuery("path", "/")

		if !zkClient.IsConnected() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "未连接到ZooKeeper"})
			return
		}

		children, err := zkClient.GetChildren(path)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// 获取节点详情
		nodes := make([]map[string]interface{}, 0)
		for _, child := range children {
			nodePath := path
			if !strings.HasSuffix(nodePath, "/") {
				nodePath += "/"
			}
			nodePath += child

			exists, stat, err := zkClient.Exists(nodePath)
			if err == nil && exists {
				nodes = append(nodes, map[string]interface{}{
					"name":        child,
					"path":        nodePath,
					"czxid":       stat.Czxid,
					"mzxid":       stat.Mzxid,
					"ctime":       time.Unix(stat.Ctime/1000, 0).Format("2006-01-02 15:04:05"),
					"mtime":       time.Unix(stat.Mtime/1000, 0).Format("2006-01-02 15:04:05"),
					"version":     stat.Version,
					"numChildren": stat.NumChildren,
					"ephemeral":   stat.EphemeralOwner != 0,
				})
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"path":  path,
			"nodes": nodes,
			"count": len(nodes),
		})
	}
}

func GetNodeHandler(zkClient *zkclient.ZKClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Param("path")
		if path == "" {
			path = "/"
		}

		if !zkClient.IsConnected() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "未连接到ZooKeeper"})
			return
		}

		data, stat, err := zkClient.Get(path)
		if err != nil {
			if err == zk.ErrNoNode {
				c.JSON(http.StatusNotFound, gin.H{"error": "节点不存在"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// 获取子节点
		children, _ := zkClient.GetChildren(path)

		c.JSON(http.StatusOK, gin.H{
			"path":     path,
			"data":     string(data),
			"dataHex":  fmt.Sprintf("%x", data),
			"children": children,
			"stat": map[string]interface{}{
				"czxid":          stat.Czxid,
				"mzxid":          stat.Mzxid,
				"ctime":          time.Unix(stat.Ctime/1000, 0).Format("2006-01-02 15:04:05"),
				"mtime":          time.Unix(stat.Mtime/1000, 0).Format("2006-01-02 15:04:05"),
				"version":        stat.Version,
				"cversion":       stat.Cversion,
				"aversion":       stat.Aversion,
				"ephemeralOwner": stat.EphemeralOwner,
				"dataLength":     stat.DataLength,
				"numChildren":    stat.NumChildren,
				"pzxid":          stat.Pzxid,
			},
		})
	}
}

func CreateNodeHandler(zkClient *zkclient.ZKClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Param("path")
		if path == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "路径不能为空"})
			return
		}

		var req CreateNodeRequest
		if err := c.BindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的请求参数"})
			return
		}

		if !zkClient.IsConnected() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "未连接到ZooKeeper"})
			return
		}

		// 确定节点类型
		var flags int32 = 0
		switch strings.ToLower(req.Mode) {
		case "ephemeral":
			flags = zk.FlagEphemeral
		case "sequential":
			flags = zk.FlagSequence
		case "ephemeral_sequential":
			flags = zk.FlagEphemeral | zk.FlagSequence
		}

		acl := zk.WorldACL(zk.PermAll)
		createdPath, err := zkClient.Create(path, []byte(req.Data), flags, acl)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message": "节点创建成功",
			"path":    createdPath,
		})
	}
}

func SetNodeHandler(zkClient *zkclient.ZKClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Param("path")
		if path == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "路径不能为空"})
			return
		}

		var req SetNodeRequest
		if err := c.BindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的请求参数"})
			return
		}

		if !zkClient.IsConnected() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "未连接到ZooKeeper"})
			return
		}

		stat, err := zkClient.Set(path, []byte(req.Data), req.Version)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message": "节点数据更新成功",
			"stat":    stat,
		})
	}
}

func DeleteNodeHandler(zkClient *zkclient.ZKClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Param("path")
		if path == "" || path == "/" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "不能删除根节点"})
			return
		}

		version := c.DefaultQuery("version", "-1")
		var versionInt int32
		fmt.Sscanf(version, "%d", &versionInt)

		if !zkClient.IsConnected() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "未连接到ZooKeeper"})
			return
		}

		err := zkClient.Delete(path, versionInt)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message": "节点删除成功",
			"path":    path,
		})
	}
}

func WatchNodeHandler(zkClient *zkclient.ZKClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Param("path")
		if path == "" {
			path = "/"
		}

		if !zkClient.IsConnected() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "未连接到ZooKeeper"})
			return
		}

		// 获取连接对象
		conn := zkClient.Conn()
		if conn == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "ZooKeeper连接不可用"})
			return
		}

		// 设置监视器
		_, _, eventChan, err := conn.GetW(path)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// 等待事件或超时
		select {
		case event := <-eventChan:
			c.JSON(http.StatusOK, gin.H{
				"event": event.Type.String(),
				"path":  event.Path,
				"state": event.State.String(),
				"err":   event.Err,
			})
		case <-time.After(30 * time.Second):
			c.JSON(http.StatusOK, gin.H{
				"event":   "timeout",
				"message": "监听超时（30秒内无事件）",
			})
		}
	}
}

func StatsHandler(zkClient *zkclient.ZKClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !zkClient.IsConnected() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "未连接到ZooKeeper"})
			return
		}

		// 获取根节点的统计信息
		rootTree, err := zkClient.GetTree("/")
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"tree":      rootTree,
			"server":    zkClient.GetServers(),
			"connected": zkClient.IsConnected(),
		})
	}
}

func GetACLHandler(zkClient *zkclient.ZKClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Param("path")
		if path == "" {
			path = "/"
		}

		if !zkClient.IsConnected() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "未连接到ZooKeeper"})
			return
		}

		acls, stat, err := zkClient.GetACL(path)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		aclList := make([]map[string]interface{}, 0)
		for _, acl := range acls {
			aclList = append(aclList, map[string]interface{}{
				"scheme":   acl.Scheme,
				"id":       acl.ID,
				"perms":    acl.Perms,
				"permsStr": permsToString(acl.Perms),
			})
		}

		c.JSON(http.StatusOK, gin.H{
			"path": path,
			"acls": aclList,
			"stat": stat,
		})
	}
}

func SetACLHandler(zkClient *zkclient.ZKClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Param("path")
		if path == "" {
			path = "/"
		}

		var aclRequests []ACLRequest
		if err := c.BindJSON(&aclRequests); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无效的ACL参数"})
			return
		}

		if !zkClient.IsConnected() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "未连接到ZooKeeper"})
			return
		}

		acls := make([]zk.ACL, 0)
		for _, req := range aclRequests {
			acls = append(acls, zk.ACL{
				Scheme: req.Scheme,
				ID:     req.ID,
				Perms:  req.Perms,
			})
		}

		version := c.DefaultQuery("version", "-1")
		var versionInt int32
		fmt.Sscanf(version, "%d", &versionInt)

		stat, err := zkClient.SetACL(path, acls, versionInt)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message": "ACL设置成功",
			"stat":    stat,
		})
	}
}

// 移除有问题的 getSessionInfo 函数，直接用简单的状态检查
func getConnectionStatus(conn *zk.Conn) map[string]interface{} {
	info := make(map[string]interface{})

	if conn == nil {
		info["valid"] = false
		info["error"] = "连接为空"
		return info
	}

	// 尝试简单的操作来检查连接
	_, _, err := conn.Exists("/")
	if err != nil {
		info["valid"] = false
		info["error"] = err.Error()
	} else {
		info["valid"] = true
	}

	return info
}

func ConnectionStatsHandler(zkClient *zkclient.ZKClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !zkClient.IsConnected() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "未连接到ZooKeeper"})
			return
		}

		info := zkClient.GetConnectionInfo()

		// 添加连接状态检查
		conn := zkClient.Conn()
		connStatus := getConnectionStatus(conn)
		info["connection_status"] = connStatus

		c.JSON(http.StatusOK, gin.H{
			"connection": info,
			"timestamp":  time.Now().Format(time.RFC3339),
		})
	}
}

func sendFourLetterCommand(host string, port int, cmd string) (string, error) {
	address := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	// 设置超时
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// 发送四字命令
	_, err = conn.Write([]byte(cmd))
	if err != nil {
		return "", err
	}

	// 读取响应
	response, err := ioutil.ReadAll(conn)
	if err != nil {
		return "", err
	}

	return string(response), nil
}

func FourLetterCommandHandler(zkClient *zkclient.ZKClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		cmd := c.Param("cmd")
		host := c.DefaultQuery("host", "localhost")
		port := c.DefaultQuery("port", "2181")

		// 支持的四字命令
		validCommands := map[string]bool{
			"stat": true, "srvr": true, "cons": true,
			"wchs": true, "wchc": true, "wchp": true,
			"mntr": true, "conf": true, "envi": true,
			"crst": true, "dump": true, "ruok": true,
		}

		if !validCommands[cmd] {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "不支持的命令",
				"valid_commands": []string{
					"stat", "srvr", "cons", "wchs", "wchc",
					"wchp", "mntr", "conf", "envi", "crst",
					"dump", "ruok",
				},
			})
			return
		}

		// 解析端口
		portInt, err := strconv.Atoi(port)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "无效的端口号",
			})
			return
		}

		// 发送四字命令
		response, err := sendFourLetterCommand(host, portInt, cmd)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   err.Error(),
				"command": cmd,
				"host":    host,
				"port":    port,
			})
			return
		}

		// 解析响应（如果是ruok命令，特殊处理）
		if cmd == "ruok" {
			if strings.TrimSpace(response) == "imok" {
				c.JSON(http.StatusOK, gin.H{
					"command":  cmd,
					"response": response,
					"status":   "OK",
					"message":  "服务器正常运行",
				})
				return
			} else {
				c.JSON(http.StatusOK, gin.H{
					"command":  cmd,
					"response": response,
					"status":   "ERROR",
					"message":  "服务器可能有问题",
				})
				return
			}
		}

		// 解析其他命令的响应
		result := parseFourLetterResponse(cmd, response)

		c.JSON(http.StatusOK, gin.H{
			"command":  cmd,
			"response": response,
			"parsed":   result,
		})
	}
}

// 解析四字命令响应
func parseFourLetterResponse(cmd, response string) map[string]interface{} {
	result := make(map[string]interface{})
	lines := strings.Split(response, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// 根据不同命令解析
		switch cmd {
		case "stat":
			if strings.Contains(line, ":") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					key := strings.TrimSpace(parts[0])
					value := strings.TrimSpace(parts[1])
					result[key] = value
				}
			}
		case "srvr":
			if strings.Contains(line, ":") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					key := strings.TrimSpace(parts[0])
					value := strings.TrimSpace(parts[1])
					result[key] = value
				}
			}
		case "mntr":
			if strings.Contains(line, "\t") {
				parts := strings.Split(line, "\t")
				if len(parts) == 2 {
					result[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
				}
			}
		}
	}

	return result
}

// 辅助函数
func permsToString(perms int32) string {
	var result []string
	if perms&zk.PermRead != 0 {
		result = append(result, "READ")
	}
	if perms&zk.PermWrite != 0 {
		result = append(result, "WRITE")
	}
	if perms&zk.PermCreate != 0 {
		result = append(result, "CREATE")
	}
	if perms&zk.PermDelete != 0 {
		result = append(result, "DELETE")
	}
	if perms&zk.PermAdmin != 0 {
		result = append(result, "ADMIN")
	}
	return strings.Join(result, "|")
}
