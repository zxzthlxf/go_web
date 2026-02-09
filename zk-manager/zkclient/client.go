package zkclient

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type ZKClient struct {
	conn        *zk.Conn
	servers     []string
	timeout     time.Duration
	isConnected bool
	eventChan   <-chan zk.Event
	state       zk.State // 保存当前状态
}

func NewZKClient() *ZKClient {
	return &ZKClient{
		servers:     []string{"localhost:2181"},
		timeout:     5 * time.Second,
		isConnected: false,
		state:       zk.StateUnknown,
	}
}

func (c *ZKClient) Connect(servers []string, timeout time.Duration) error {
	var err error
	c.servers = servers
	c.timeout = timeout

	conn, eventChan, err := zk.Connect(servers, timeout)
	if err != nil {
		return fmt.Errorf("连接失败: %v", err)
	}

	c.conn = conn
	c.eventChan = eventChan
	c.isConnected = true
	c.state = zk.StateConnected

	// 监听连接事件
	go c.monitorEvents()

	return nil
}

func (c *ZKClient) monitorEvents() {
	for event := range c.eventChan {
		log.Printf("ZooKeeper事件: %v", event)
		c.state = event.State

		if event.State == zk.StateDisconnected ||
			event.State == zk.StateExpired ||
			event.State == zk.StateAuthFailed {
			c.isConnected = false
			log.Printf("连接状态变化: %v", event.State.String())
		} else if event.State == zk.StateConnected ||
			event.State == zk.StateHasSession {
			c.isConnected = true
			log.Printf("连接状态变化: %v", event.State.String())
		}
	}
}

func (c *ZKClient) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
	c.isConnected = false
	c.state = zk.StateDisconnected
	log.Println("ZooKeeper客户端已关闭")
}

func (c *ZKClient) IsConnected() bool {
	return c.isConnected && c.conn != nil
}

func (c *ZKClient) GetState() string {
	return c.state.String()
}

func (c *ZKClient) GetServers() []string {
	return c.servers
}

func (c *ZKClient) GetChildren(path string) ([]string, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("未连接到ZooKeeper")
	}

	children, _, err := c.conn.Children(path)
	return children, err
}

func (c *ZKClient) Get(path string) ([]byte, *zk.Stat, error) {
	if !c.IsConnected() {
		return nil, nil, fmt.Errorf("未连接到ZooKeeper")
	}

	data, stat, err := c.conn.Get(path)
	return data, stat, err
}

func (c *ZKClient) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	if !c.IsConnected() {
		return "", fmt.Errorf("未连接到ZooKeeper")
	}

	return c.conn.Create(path, data, flags, acl)
}

func (c *ZKClient) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("未连接到ZooKeeper")
	}

	return c.conn.Set(path, data, version)
}

func (c *ZKClient) Delete(path string, version int32) error {
	if !c.IsConnected() {
		return fmt.Errorf("未连接到ZooKeeper")
	}

	return c.conn.Delete(path, version)
}

func (c *ZKClient) Exists(path string) (bool, *zk.Stat, error) {
	if !c.IsConnected() {
		return false, nil, fmt.Errorf("未连接到ZooKeeper")
	}

	return c.conn.Exists(path)
}

func (c *ZKClient) GetACL(path string) ([]zk.ACL, *zk.Stat, error) {
	if !c.IsConnected() {
		return nil, nil, fmt.Errorf("未连接到ZooKeeper")
	}

	return c.conn.GetACL(path)
}

func (c *ZKClient) SetACL(path string, acl []zk.ACL, version int32) (*zk.Stat, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("未连接到ZooKeeper")
	}

	return c.conn.SetACL(path, acl, version)
}

func (c *ZKClient) GetTree(path string) (map[string]interface{}, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("未连接到ZooKeeper")
	}

	result := make(map[string]interface{})

	exists, stat, err := c.Exists(path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return result, nil
	}

	children, err := c.GetChildren(path)
	if err != nil {
		return nil, err
	}

	data, _, _ := c.Get(path)

	result["path"] = path
	result["data"] = string(data)
	result["stat"] = stat
	result["children"] = children

	// 只获取直接子节点，避免递归过深
	childDetails := make([]map[string]interface{}, 0)
	for _, child := range children {
		childPath := path
		if !strings.HasSuffix(childPath, "/") {
			childPath += "/"
		}
		childPath += child

		exists, childStat, err := c.Exists(childPath)
		if err == nil && exists {
			childDetails = append(childDetails, map[string]interface{}{
				"name":        child,
				"path":        childPath,
				"czxid":       childStat.Czxid,
				"ephemeral":   childStat.EphemeralOwner != 0,
				"numChildren": childStat.NumChildren,
			})
		}
	}
	result["childDetails"] = childDetails

	return result, nil
}

// 添加 Conn 方法
func (c *ZKClient) Conn() *zk.Conn {
	return c.conn
}

// 添加 GetEventChannel 方法
func (c *ZKClient) GetEventChannel() <-chan zk.Event {
	return c.eventChan
}

// 添加 GetConnectionInfo 方法
func (c *ZKClient) GetConnectionInfo() map[string]interface{} {
	info := map[string]interface{}{
		"connected": c.isConnected,
		"servers":   c.servers,
		"timeout":   c.timeout.String(),
		"state":     c.state.String(),
	}

	// 获取会话信息
	if c.conn != nil {
		// 尝试获取会话ID - 使用反射或其他方式
		// 注意：go-zookeeper 库可能没有直接导出 SessionID
		// 我们可以通过其他方式获取或跳过
		info["state_description"] = c.state.String()
	}

	return info
}

// 添加健康检查方法
func (c *ZKClient) HealthCheck() (bool, string) {
	if !c.IsConnected() {
		return false, "未连接到ZooKeeper"
	}

	// 尝试获取根节点来验证连接
	_, _, err := c.Exists("/")
	if err != nil {
		return false, fmt.Sprintf("健康检查失败: %v", err)
	}

	return true, "连接正常"
}

// 添加重新连接方法
func (c *ZKClient) Reconnect() error {
	if c.conn != nil {
		c.conn.Close()
	}

	return c.Connect(c.servers, c.timeout)
}

// 添加服务器信息获取方法
func (c *ZKClient) GetServerInfo() string {
	if c.conn == nil {
		return "未连接"
	}

	// 尝试获取当前连接的服务器
	// 注意：go-zookeeper 库可能没有直接提供这个方法
	// 我们可以返回配置的服务器列表
	return strings.Join(c.servers, ", ")
}
