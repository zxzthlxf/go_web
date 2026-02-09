package main

import (
	"context"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

// ------------------------------
// 数据结构定义（优化版）
// ------------------------------

// KafkaConfig Kafka配置
type KafkaConfig struct {
	BrokerList   string `form:"broker_list" json:"broker_list" binding:"required"`
	KafkaVersion string `form:"kafka_version" json:"kafka_version" default:"2.8.0"`
	TimeoutMS    int    `form:"timeout_ms" json:"timeout_ms" default:"5000"`
}

// TopicConfig 主题配置
type TopicConfig struct {
	KafkaConfig
	Topic string `form:"topic" json:"topic" binding:"required"`
}

// CreateTopicRequest 创建Topic请求
type CreateTopicRequest struct {
	KafkaConfig
	Topic             string `form:"topic" json:"topic" binding:"required"`
	NumPartitions     int32  `form:"num_partitions" json:"num_partitions" default:"1"`
	ReplicationFactor int16  `form:"replication_factor" json:"replication_factor" default:"1"`
	RetentionHours    int    `form:"retention_hours" json:"retention_hours" default:"24"`
	CleanupPolicy     string `form:"cleanup_policy" json:"cleanup_policy" default:"delete"`
}

// ProducerRequest 生产消息请求
type ProducerRequest struct {
	TopicConfig
	Partition int32  `form:"partition" json:"partition" default:"0"`
	Key       string `form:"key" json:"key"`
	Value     string `form:"value" json:"value" binding:"required"`
}

// ConsumerRequest 消费消息请求
type ConsumerRequest struct {
	TopicConfig
	Partition   int32 `form:"partition" json:"partition" default:"0"`
	StartOffset int64 `form:"start_offset" json:"start_offset" default:"0"`
	MaxNum      int   `form:"max_num" json:"max_num" default:"10"`
}

// ConsumerGroupRequest 消费组请求
type ConsumerGroupRequest struct {
	TopicConfig
	GroupName  string `form:"group_name" json:"group_name" binding:"required"`
	MaxNum     int    `form:"max_num" json:"max_num" default:"10"`
	AutoCommit bool   `form:"auto_commit" json:"auto_commit"`
}

// GroupStatusRequest 消费组状态请求
type GroupStatusRequest struct {
	TopicConfig
	GroupName string `form:"group_name" json:"group_name" binding:"required"`
}

// ProducerMsgQueryRequest 查询消息请求
type ProducerMsgQueryRequest struct {
	TopicConfig
	Partition   int32 `form:"partition" json:"partition" default:"0"`
	StartOffset int64 `form:"start_offset" json:"start_offset" default:"0"`
	EndOffset   int64 `form:"end_offset" json:"end_offset" default:"-1"`
	MaxNum      int   `form:"max_num" json:"max_num" default:"20"`
}

// ListTopicsRequest 列表主题请求
type ListTopicsRequest struct {
	KafkaConfig
	FilterInternal bool `form:"filter_internal" json:"filter_internal"`
}

// DeleteTopicsRequest 删除主题请求
type DeleteTopicsRequest struct {
	KafkaConfig
	Topics []string `form:"topics[]" json:"topics" binding:"required,min=1"`
}

// ListConsumerGroupsRequest 列表消费组请求
type ListConsumerGroupsRequest struct {
	KafkaConfig
}

// GroupTopicsRequest 消费组主题请求
type GroupTopicsRequest struct {
	KafkaConfig
	GroupName string `form:"group_name" json:"group_name" binding:"required"`
}

// Response 统一响应
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

// TopicListItem 主题列表项
type TopicListItem struct {
	Name              string `json:"name"`
	NumPartitions     int32  `json:"num_partitions"`
	ReplicationFactor int16  `json:"replication_factor"`
	CreatedTime       string `json:"created_time"`
	TopicType         string `json:"topic_type"`
}

// ConsumerGroupListItem 消费组列表项（增强版）
type ConsumerGroupListItem struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	State       string   `json:"state"`
	Topics      []string `json:"topics"`       // 新增：消费的主题列表
	MemberCount int      `json:"member_count"` // 新增：成员数量
	Lag         int64    `json:"lag"`          // 新增：总延迟
	QueryTime   string   `json:"query_time"`
	Description string   `json:"description"`
}

// TopicDetail 主题详情
type TopicDetail struct {
	Name              string `json:"name"`
	NumPartitions     int32  `json:"num_partitions"`
	ReplicationFactor int16  `json:"replication_factor"`
	RetentionHours    int    `json:"retention_hours"`
	CleanupPolicy     string `json:"cleanup_policy"`
	CreatedTime       string `json:"created_time"`
}

// ProducerMessageDetail 消息详情
type ProducerMessageDetail struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Timestamp string `json:"timestamp"`
}

// ConsumerGroupMessageDetail 消费组消息详情
type ConsumerGroupMessageDetail struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Timestamp string `json:"timestamp"`
	GroupName string `json:"group_name"`
	Committed bool   `json:"committed"`
}

// ConsumerStatus 消费状态
type ConsumerStatus struct {
	GroupName  string `json:"group_name"`
	Topic      string `json:"topic"`
	Partition  int32  `json:"partition"`
	Offset     int64  `json:"offset"`
	HighWater  int64  `json:"high_water"`
	Lag        int64  `json:"lag"`
	LastUpdate string `json:"last_update"`
}

// ------------------------------
// 工具函数
// ------------------------------

var kafkaVersionMap = map[string]sarama.KafkaVersion{
	"2.0.0": sarama.V2_0_0_0,
	"2.5.0": sarama.V2_5_0_0,
	"2.8.0": sarama.V2_8_0_0,
	"3.0.0": sarama.V3_0_0_0,
}

func getKafkaVersion(versionStr string) sarama.KafkaVersion {
	if v, ok := kafkaVersionMap[versionStr]; ok {
		return v
	}
	return sarama.V2_8_0_0
}

func parseBrokerList(brokerStr string) []string {
	if brokerStr == "" {
		return nil
	}
	brokerStr = strings.ReplaceAll(strings.TrimSpace(brokerStr), " ", "")
	return strings.Split(brokerStr, ",")
}

func getPartitionHighWater(brokerList []string, kafkaVersion sarama.KafkaVersion, timeoutMS int, topic string, partition int32) (int64, error) {
	if len(brokerList) == 0 {
		return 0, fmt.Errorf("broker list is empty")
	}

	config := sarama.NewConfig()
	config.Version = kafkaVersion
	config.Net.DialTimeout = time.Duration(timeoutMS) * time.Millisecond

	broker := sarama.NewBroker(brokerList[0])
	if err := broker.Open(config); err != nil {
		return 0, fmt.Errorf("failed to open broker: %v", err)
	}
	defer broker.Close()

	connected, err := broker.Connected()
	if err != nil || !connected {
		return 0, fmt.Errorf("broker not connected")
	}

	req := &sarama.OffsetRequest{Version: 1}
	req.AddBlock(topic, partition, sarama.OffsetNewest, 1)

	resp, err := broker.GetAvailableOffsets(req)
	if err != nil {
		return 0, fmt.Errorf("failed to get offsets: %v", err)
	}

	block := resp.GetBlock(topic, partition)
	if block == nil || block.Err != sarama.ErrNoError {
		return 0, fmt.Errorf("block error: %v", block.Err)
	}

	if len(block.Offsets) == 0 {
		return 0, nil
	}
	return block.Offsets[0] - 1, nil
}

func isInternalTopic(topicName string) bool {
	return strings.HasPrefix(topicName, "__")
}

func getConsumerGroupTypeDesc(groupTypeStr string) string {
	switch strings.ToLower(groupTypeStr) {
	case "consumer":
		return "consumer"
	case "connect":
		return "connect"
	case "mirror-maker":
		return "mirror-maker"
	default:
		return "unknown"
	}
}

func getConsumerGroupStateDesc(state string) string {
	switch state {
	case "Stable":
		return "稳定"
	case "PreparingRebalance":
		return "准备重平衡"
	case "CompletingRebalance":
		return "完成重平衡"
	case "Dead":
		return "死亡"
	case "Empty":
		return "空"
	default:
		return state
	}
}

// ------------------------------
// Kafka客户端管理器
// ------------------------------

type KafkaClientManager struct{}

func (kcm *KafkaClientManager) NewClient(brokerList []string, kafkaVersionStr string, timeoutMS int) (sarama.Client, error) {
	config := sarama.NewConfig()
	config.Version = getKafkaVersion(kafkaVersionStr)
	config.Net.DialTimeout = time.Duration(timeoutMS) * time.Millisecond

	return sarama.NewClient(brokerList, config)
}

func (kcm *KafkaClientManager) NewAdminClient(brokerList []string, kafkaVersionStr string, timeoutMS int) (sarama.ClusterAdmin, error) {
	config := sarama.NewConfig()
	config.Version = getKafkaVersion(kafkaVersionStr)
	config.Net.DialTimeout = time.Duration(timeoutMS) * time.Millisecond
	config.Admin.Timeout = time.Duration(timeoutMS) * time.Millisecond

	return sarama.NewClusterAdmin(brokerList, config)
}

var kafkaClientManager = &KafkaClientManager{}

// ------------------------------
// 核心业务逻辑（优化版）
// ------------------------------

func createTopic(c *gin.Context, req CreateTopicRequest) (interface{}, error) {
	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("broker list format error")
	}

	if req.ReplicationFactor > int16(len(brokerList)) {
		return nil, fmt.Errorf("replication factor cannot exceed broker count")
	}

	adminClient, err := kafkaClientManager.NewAdminClient(brokerList, req.KafkaVersion, req.TimeoutMS)
	if err != nil {
		return nil, fmt.Errorf("create admin client failed: %v", err)
	}
	defer adminClient.Close()

	topics, err := adminClient.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("list topics failed: %v", err)
	}
	if _, exists := topics[req.Topic]; exists {
		return nil, fmt.Errorf("topic %s already exists", req.Topic)
	}

	retentionMS := strconv.Itoa(req.RetentionHours * 3600 * 1000)
	cleanupPolicy := req.CleanupPolicy
	compressionType := "producer"

	topicConfig := sarama.TopicDetail{
		NumPartitions:     req.NumPartitions,
		ReplicationFactor: req.ReplicationFactor,
		ConfigEntries: map[string]*string{
			"retention.ms":     &retentionMS,
			"cleanup.policy":   &cleanupPolicy,
			"compression.type": &compressionType,
		},
	}

	err = adminClient.CreateTopic(req.Topic, &topicConfig, false)
	if err != nil {
		return nil, fmt.Errorf("create topic failed: %v", err)
	}

	return TopicDetail{
		Name:              req.Topic,
		NumPartitions:     req.NumPartitions,
		ReplicationFactor: req.ReplicationFactor,
		RetentionHours:    req.RetentionHours,
		CleanupPolicy:     req.CleanupPolicy,
		CreatedTime:       time.Now().Format("2006-01-02 15:04:05"),
	}, nil
}

// getAllConsumerGroups 获取所有消费者组（增强版）
func getAllConsumerGroups(c *gin.Context, req ListConsumerGroupsRequest) (interface{}, error) {
	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("broker list format error")
	}

	adminClient, err := kafkaClientManager.NewAdminClient(brokerList, req.KafkaVersion, req.TimeoutMS)
	if err != nil {
		return nil, fmt.Errorf("create admin client failed: %v", err)
	}
	defer adminClient.Close()

	// 获取消费者组列表
	groups, err := adminClient.ListConsumerGroups()
	if err != nil {
		return nil, fmt.Errorf("list consumer groups failed: %v", err)
	}

	// 如果没有消费者组，直接返回空列表
	if len(groups) == 0 {
		return []ConsumerGroupListItem{}, nil
	}

	// 获取消费者组详细信息
	groupNames := make([]string, 0, len(groups))
	for groupName := range groups {
		groupNames = append(groupNames, groupName)
	}

	// 批量描述消费者组
	groupDescriptions, err := adminClient.DescribeConsumerGroups(groupNames)
	if err != nil {
		// 如果描述失败，只返回基本信息
		return getBasicConsumerGroups(groups), nil
	}

	// 创建客户端用于获取高水位信息
	client, err := kafkaClientManager.NewClient(brokerList, req.KafkaVersion, req.TimeoutMS)
	if err != nil {
		// 如果创建客户端失败，返回基本信息
		return getConsumerGroupsWithDescriptions(groups, groupDescriptions), nil
	}
	defer client.Close()

	// 创建结果列表
	var groupList []ConsumerGroupListItem
	currentTime := time.Now().Format("2006-01-02 15:04:05")

	for _, desc := range groupDescriptions {
		if desc.Err != sarama.ErrNoError {
			continue
		}

		// 获取消费组类型
		groupTypeStr, exists := groups[desc.GroupId]
		if !exists {
			groupTypeStr = "unknown"
		}
		groupType := getConsumerGroupTypeDesc(groupTypeStr)

		// 获取消费组消费的主题
		var topics []string
		var totalLag int64

		// 获取消费组偏移量信息
		offsets, err := adminClient.ListConsumerGroupOffsets(desc.GroupId, nil)
		if err == nil {
			for topic := range offsets.Blocks {
				topics = append(topics, topic)

				// 计算该主题的总延迟
				for partition, block := range offsets.Blocks[topic] {
					if block != nil {
						// 获取高水位线
						highWater, err := getPartitionHighWater(brokerList, getKafkaVersion(req.KafkaVersion), req.TimeoutMS, topic, partition)
						if err == nil {
							lag := highWater - block.Offset
							if lag > 0 {
								totalLag += lag
							}
						}
					}
				}
			}
			// 排序主题列表
			sort.Strings(topics)
		}

		// 生成描述信息
		description := fmt.Sprintf("消费组类型: %s, 状态: %s, 成员数: %d",
			groupType, getConsumerGroupStateDesc(desc.State), len(desc.Members))
		if len(topics) > 0 {
			description += fmt.Sprintf(", 消费主题数: %d", len(topics))
		}
		if totalLag > 0 {
			description += fmt.Sprintf(", 总延迟: %d", totalLag)
		}

		groupList = append(groupList, ConsumerGroupListItem{
			Name:        desc.GroupId,
			Type:        groupType,
			State:       getConsumerGroupStateDesc(desc.State),
			Topics:      topics,
			MemberCount: len(desc.Members),
			Lag:         totalLag,
			QueryTime:   currentTime,
			Description: description,
		})
	}

	// 按名称排序
	sort.Slice(groupList, func(i, j int) bool {
		return groupList[i].Name < groupList[j].Name
	})

	return groupList, nil
}

// getBasicConsumerGroups 获取基本的消费者组信息（没有描述信息）
func getBasicConsumerGroups(groups map[string]string) []ConsumerGroupListItem {
	var groupList []ConsumerGroupListItem
	currentTime := time.Now().Format("2006-01-02 15:04:05")

	for groupName, groupTypeStr := range groups {
		groupType := getConsumerGroupTypeDesc(groupTypeStr)

		groupList = append(groupList, ConsumerGroupListItem{
			Name:        groupName,
			Type:        groupType,
			State:       "未知",
			Topics:      []string{},
			MemberCount: 0,
			Lag:         0,
			QueryTime:   currentTime,
			Description: fmt.Sprintf("消费组类型: %s", groupType),
		})
	}

	// 按名称排序
	sort.Slice(groupList, func(i, j int) bool {
		return groupList[i].Name < groupList[j].Name
	})

	return groupList
}

// getConsumerGroupsWithDescriptions 获取带描述的消费者组信息（没有高水位信息）
func getConsumerGroupsWithDescriptions(groups map[string]string, descriptions []*sarama.GroupDescription) []ConsumerGroupListItem {
	var groupList []ConsumerGroupListItem
	currentTime := time.Now().Format("2006-01-02 15:04:05")

	// 创建描述映射
	descMap := make(map[string]*sarama.GroupDescription)
	for _, desc := range descriptions {
		if desc.Err == sarama.ErrNoError {
			descMap[desc.GroupId] = desc
		}
	}

	for groupName, groupTypeStr := range groups {
		groupType := getConsumerGroupTypeDesc(groupTypeStr)
		state := "未知"
		memberCount := 0

		if desc, exists := descMap[groupName]; exists {
			state = getConsumerGroupStateDesc(desc.State)
			memberCount = len(desc.Members)
		}

		groupList = append(groupList, ConsumerGroupListItem{
			Name:        groupName,
			Type:        groupType,
			State:       state,
			Topics:      []string{}, // 没有偏移量信息，无法获取主题
			MemberCount: memberCount,
			Lag:         0,
			QueryTime:   currentTime,
			Description: fmt.Sprintf("消费组类型: %s, 状态: %s, 成员数: %d", groupType, state, memberCount),
		})
	}

	// 按名称排序
	sort.Slice(groupList, func(i, j int) bool {
		return groupList[i].Name < groupList[j].Name
	})

	return groupList
}

func produceMessage(c *gin.Context, req ProducerRequest) (interface{}, error) {
	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("broker list format error")
	}

	config := sarama.NewConfig()
	config.Version = getKafkaVersion(req.KafkaVersion)
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Return.Successes = true
	config.Net.DialTimeout = time.Duration(req.TimeoutMS) * time.Millisecond

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return nil, fmt.Errorf("create producer failed: %v", err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic:     req.Topic,
		Partition: req.Partition,
		Key:       sarama.StringEncoder(req.Key),
		Value:     sarama.StringEncoder(req.Value),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("send message failed: %v", err)
	}

	return ProducerMessageDetail{
		Topic:     req.Topic,
		Partition: partition,
		Offset:    offset,
		Key:       req.Key,
		Value:     req.Value,
		Timestamp: time.Now().Format("2006-01-02 15:04:05"),
	}, nil
}

func consumeMessage(c *gin.Context, req ConsumerRequest) (interface{}, error) {
	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("broker list format error")
	}

	config := sarama.NewConfig()
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = getKafkaVersion(req.KafkaVersion)
	config.Net.DialTimeout = time.Duration(req.TimeoutMS) * time.Millisecond

	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		return nil, fmt.Errorf("create consumer failed: %v", err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(req.Topic, req.Partition, req.StartOffset)
	if err != nil {
		return nil, fmt.Errorf("subscribe partition failed: %v", err)
	}
	defer pc.Close()

	var messages []ProducerMessageDetail
	count := 0
	timeout := time.After(time.Duration(req.TimeoutMS) * time.Millisecond)

	for {
		select {
		case msg := <-pc.Messages():
			messages = append(messages, ProducerMessageDetail{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Key:       string(msg.Key),
				Value:     string(msg.Value),
				Timestamp: msg.Timestamp.Format("2006-01-02 15:04:05"),
			})
			count++
			if count >= req.MaxNum {
				return messages, nil
			}
		case <-timeout:
			return messages, nil
		}
	}
}

// ConsumerGroupHandler 消费组处理器
type ConsumerGroupHandler struct {
	messages   []ConsumerGroupMessageDetail
	count      int
	maxNum     int
	groupName  string
	mu         sync.Mutex
	done       chan struct{}
	autoCommit bool
}

func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			if msg == nil {
				return nil
			}

			h.mu.Lock()
			if h.count >= h.maxNum {
				h.mu.Unlock()
				close(h.done)
				return nil
			}

			h.messages = append(h.messages, ConsumerGroupMessageDetail{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Key:       string(msg.Key),
				Value:     string(msg.Value),
				Timestamp: msg.Timestamp.Format("2006-01-02 15:04:05"),
				GroupName: h.groupName,
				Committed: h.autoCommit,
			})
			h.count++
			currentCount := h.count
			h.mu.Unlock()

			session.MarkMessage(msg, "")
			if h.autoCommit {
				session.Commit()
			}

			if currentCount >= h.maxNum {
				close(h.done)
				return nil
			}

		case <-session.Context().Done():
			return nil
		}
	}
}

func consumeMessageWithGroup(c *gin.Context, req ConsumerGroupRequest) (interface{}, error) {
	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("broker list format error")
	}

	config := sarama.NewConfig()
	config.Version = getKafkaVersion(req.KafkaVersion)
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Net.DialTimeout = time.Duration(req.TimeoutMS) * time.Millisecond

	group, err := sarama.NewConsumerGroup(brokerList, req.GroupName, config)
	if err != nil {
		return nil, fmt.Errorf("create consumer group failed: %v", err)
	}
	defer group.Close()

	handler := &ConsumerGroupHandler{
		messages:   make([]ConsumerGroupMessageDetail, 0),
		maxNum:     req.MaxNum,
		groupName:  req.GroupName,
		done:       make(chan struct{}),
		autoCommit: req.AutoCommit,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.TimeoutMS)*time.Millisecond)
	defer cancel()

	go func() {
		topics := []string{req.Topic}
		for {
			if err := group.Consume(ctx, topics, handler); err != nil {
				log.Printf("consumer group error: %v", err)
				return
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	select {
	case <-handler.done:
	case <-ctx.Done():
	}

	handler.mu.Lock()
	result := handler.messages
	handler.mu.Unlock()

	if len(result) == 0 {
		return result, fmt.Errorf("no messages consumed (topic may be empty or timeout)")
	}

	return result, nil
}

func queryProducerMessages(c *gin.Context, req ProducerMsgQueryRequest) (interface{}, error) {
	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("broker list format error")
	}

	var endOffset int64
	if req.EndOffset <= 0 {
		hw, err := getPartitionHighWater(brokerList, getKafkaVersion(req.KafkaVersion), req.TimeoutMS, req.Topic, req.Partition)
		if err != nil {
			return nil, fmt.Errorf("get high water for partition failed: %v", err)
		}
		endOffset = hw
	} else {
		endOffset = req.EndOffset
	}

	if req.StartOffset > endOffset {
		return nil, fmt.Errorf("start offset cannot be greater than end offset")
	}

	config := sarama.NewConfig()
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = getKafkaVersion(req.KafkaVersion)
	config.Net.DialTimeout = time.Duration(req.TimeoutMS) * time.Millisecond

	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		return nil, fmt.Errorf("create consumer failed: %v", err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(req.Topic, req.Partition, req.StartOffset)
	if err != nil {
		return nil, fmt.Errorf("subscribe partition failed: %v", err)
	}
	defer pc.Close()

	var messages []ProducerMessageDetail
	count := 0
	timeout := time.After(time.Duration(req.TimeoutMS) * time.Millisecond)

	for {
		select {
		case msg := <-pc.Messages():
			if msg.Offset > endOffset {
				return messages, nil
			}
			messages = append(messages, ProducerMessageDetail{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Key:       string(msg.Key),
				Value:     string(msg.Value),
				Timestamp: msg.Timestamp.Format("2006-01-02 15:04:05"),
			})
			count++
			if count >= req.MaxNum {
				return messages, nil
			}
		case <-timeout:
			return messages, nil
		}
	}
}

func getGroupTopics(c *gin.Context, req GroupTopicsRequest) (interface{}, error) {
	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("broker list format error")
	}

	adminClient, err := kafkaClientManager.NewAdminClient(brokerList, req.KafkaVersion, req.TimeoutMS)
	if err != nil {
		return nil, fmt.Errorf("create admin client failed: %v", err)
	}
	defer adminClient.Close()

	groupOffsets, err := adminClient.ListConsumerGroupOffsets(req.GroupName, nil)
	if err != nil {
		return nil, fmt.Errorf("list consumer group offsets failed: %v", err)
	}

	var topics []string
	for topic := range groupOffsets.Blocks {
		topics = append(topics, topic)
	}
	sort.Strings(topics)

	return topics, nil
}

func getGroupStatus(c *gin.Context, req GroupStatusRequest) (interface{}, error) {
	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("broker list format error")
	}

	client, err := kafkaClientManager.NewClient(brokerList, req.KafkaVersion, req.TimeoutMS)
	if err != nil {
		return nil, fmt.Errorf("create client failed: %v", err)
	}
	defer client.Close()

	adminClient, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("create admin client failed: %v", err)
	}
	defer adminClient.Close()

	topicMeta, err := adminClient.DescribeTopics([]string{req.Topic})
	if err != nil {
		return nil, fmt.Errorf("describe topics failed: %v", err)
	}
	if len(topicMeta) == 0 {
		return nil, fmt.Errorf("topic %s not found", req.Topic)
	}

	var partitionIDs []int32
	for _, p := range topicMeta[0].Partitions {
		partitionIDs = append(partitionIDs, p.ID)
	}
	if len(partitionIDs) == 0 {
		return nil, fmt.Errorf("topic %s has no partitions", req.Topic)
	}

	groupOffsets, err := adminClient.ListConsumerGroupOffsets(req.GroupName, map[string][]int32{req.Topic: partitionIDs})
	if err != nil {
		return nil, fmt.Errorf("list consumer group offsets failed: %v", err)
	}

	highWaterMap := make(map[int32]int64)
	for _, p := range partitionIDs {
		hw, err := getPartitionHighWater(brokerList, getKafkaVersion(req.KafkaVersion), req.TimeoutMS, req.Topic, p)
		if err != nil {
			log.Printf("get high water for partition %d failed: %v", p, err)
			highWaterMap[p] = 0
		} else {
			highWaterMap[p] = hw
		}
	}

	var statusList []ConsumerStatus
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	for _, partition := range partitionIDs {
		offset := int64(-1)
		if block, exists := groupOffsets.Blocks[req.Topic]; exists {
			if meta, exists := block[partition]; exists {
				offset = meta.Offset
			}
		}

		lag := int64(0)
		if offset == -1 {
			lag = highWaterMap[partition] + 1
		} else {
			lag = highWaterMap[partition] - offset
		}

		statusList = append(statusList, ConsumerStatus{
			GroupName:  req.GroupName,
			Topic:      req.Topic,
			Partition:  partition,
			Offset:     offset,
			HighWater:  highWaterMap[partition],
			Lag:        lag,
			LastUpdate: currentTime,
		})
	}

	return statusList, nil
}

func getAllTopics(c *gin.Context, req ListTopicsRequest) (interface{}, error) {
	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("broker list format error")
	}

	adminClient, err := kafkaClientManager.NewAdminClient(brokerList, req.KafkaVersion, req.TimeoutMS)
	if err != nil {
		return nil, fmt.Errorf("create admin client failed: %v", err)
	}
	defer adminClient.Close()

	topicsMap, err := adminClient.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("list topics failed: %v", err)
	}

	var topicNames []string
	for name := range topicsMap {
		if req.FilterInternal && isInternalTopic(name) {
			continue
		}
		topicNames = append(topicNames, name)
	}

	if len(topicNames) == 0 {
		if req.FilterInternal {
			return []TopicListItem{}, nil
		} else {
			return []TopicListItem{}, nil
		}
	}

	topicMetas, err := adminClient.DescribeTopics(topicNames)
	if err != nil {
		return nil, fmt.Errorf("describe topics failed: %v", err)
	}

	var topicList []TopicListItem
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	for _, meta := range topicMetas {
		if meta.Err != sarama.ErrNoError {
			continue
		}

		numPartitions := int32(len(meta.Partitions))
		replFactor := int16(1)
		if len(meta.Partitions) > 0 {
			replFactor = int16(len(meta.Partitions[0].Replicas))
		}

		topicType := "user"
		if isInternalTopic(meta.Name) {
			topicType = "internal"
		}

		topicList = append(topicList, TopicListItem{
			Name:              meta.Name,
			NumPartitions:     numPartitions,
			ReplicationFactor: replFactor,
			CreatedTime:       currentTime,
			TopicType:         topicType,
		})
	}

	// 按名称排序
	sort.Slice(topicList, func(i, j int) bool {
		return topicList[i].Name < topicList[j].Name
	})

	return topicList, nil
}

func deleteTopics(c *gin.Context, req DeleteTopicsRequest) (interface{}, error) {
	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("broker list format error")
	}

	adminClient, err := kafkaClientManager.NewAdminClient(brokerList, req.KafkaVersion, req.TimeoutMS)
	if err != nil {
		return nil, fmt.Errorf("create admin client failed: %v", err)
	}
	defer adminClient.Close()

	existingTopics, err := adminClient.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("list topics failed: %v", err)
	}

	var topicsToDelete []string
	var skippedTopics []string
	for _, topic := range req.Topics {
		if isInternalTopic(topic) {
			skippedTopics = append(skippedTopics, fmt.Sprintf("%s(internal topic)", topic))
			continue
		}
		if _, exists := existingTopics[topic]; !exists {
			skippedTopics = append(skippedTopics, fmt.Sprintf("%s(not found)", topic))
			continue
		}
		topicsToDelete = append(topicsToDelete, topic)
	}

	if len(topicsToDelete) == 0 {
		return map[string]interface{}{
			"deleted": []string{},
			"skipped": skippedTopics,
		}, fmt.Errorf("no topics to delete")
	}

	var deletedTopics []string
	var failedTopics []string
	for _, topic := range topicsToDelete {
		err := adminClient.DeleteTopic(topic)
		if err != nil {
			failedTopics = append(failedTopics, fmt.Sprintf("%s(%v)", topic, err))
		} else {
			deletedTopics = append(deletedTopics, topic)
		}
	}

	result := map[string]interface{}{
		"deleted": deletedTopics,
		"failed":  failedTopics,
		"skipped": skippedTopics,
	}

	if len(failedTopics) > 0 {
		return result, fmt.Errorf("some topics failed to delete")
	}

	return result, nil
}

// ------------------------------
// API处理器包装器
// ------------------------------

func apiHandler(fn func(*gin.Context) (interface{}, error)) gin.HandlerFunc {
	return func(c *gin.Context) {
		data, err := fn(c)
		if err != nil {
			c.JSON(http.StatusOK, Response{
				Code:    1,
				Message: err.Error(),
				Data:    data,
			})
			return
		}
		c.JSON(http.StatusOK, Response{
			Code:    0,
			Message: "success",
			Data:    data,
		})
	}
}

// ------------------------------
// HTML模板（保持不变）
// ------------------------------

var indexTemplate = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>Kafka Web工具</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: Arial, sans-serif;
        }
        body {
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .tabs {
            display: flex;
            margin-bottom: 20px;
            border-bottom: 1px solid #e0e0e0;
            flex-wrap: wrap;
        }
        .tab {
            padding: 10px 20px;
            cursor: pointer;
            border: none;
            background: none;
            font-size: 16px;
            color: #666;
            border-bottom: 2px solid transparent;
        }
        .tab.active {
            color: #2196F3;
            border-bottom: 2px solid #2196F3;
        }
        .tab-content {
            display: none;
        }
        .tab-content.active {
            display: block;
        }
        .form-group {
            margin-bottom: 15px;
        }
        label {
            display: block;
            margin-bottom: 5px;
            font-weight: bold;
            color: #333;
        }
        input, select, textarea {
            width: 100%;
            padding: 8px 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
        }
        textarea {
            height: 100px;
            resize: vertical;
        }
        button {
            padding: 10px 20px;
            background-color: #2196F3;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
            margin-right: 10px;
            margin-top: 5px;
        }
        button:hover {
            background-color: #1976D2;
        }
        button.secondary {
            background-color: #607D8B;
        }
        button.secondary:hover {
            background-color: #455A64;
        }
        button.copy-btn {
            padding: 5px 10px;
            font-size: 12px;
            background-color: #4CAF50;
            margin-left: 5px;
        }
        button.copy-btn:hover {
            background-color: #388E3C;
        }
        button.small-btn {
            padding: 3px 8px;
            font-size: 11px;
            margin-left: 5px;
        }
        .result {
            margin-top: 20px;
            padding: 15px;
            background-color: #f8f8f8;
            border-radius: 4px;
            white-space: pre-wrap;
            font-family: monospace;
            font-size: 14px;
            max-height: 500px;
            overflow-y: auto;
        }
        .error {
            color: #f44336;
        }
        .success {
            color: #4CAF50;
        }
        .hint {
            font-size: 12px;
            color: #999;
            margin-top: 5px;
        }
        .btn-group {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }
        .group-item {
            padding: 12px;
            margin: 5px 0;
            border: 1px solid #eee;
            border-radius: 4px;
            background-color: #fff;
        }
        .group-item:hover {
            background-color: #f9f9f9;
        }
        .group-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }
        .group-name {
            font-weight: bold;
            color: #333;
            font-size: 16px;
        }
        .group-meta {
            display: flex;
            gap: 15px;
            font-size: 12px;
            color: #666;
            margin-bottom: 8px;
        }
        .group-topics {
            margin-top: 8px;
            padding: 8px;
            background-color: #f5f5f5;
            border-radius: 3px;
        }
        .topics-header {
            font-size: 12px;
            color: #666;
            margin-bottom: 5px;
        }
        .topics-list {
            display: flex;
            flex-wrap: wrap;
            gap: 5px;
            max-height: 100px;
            overflow-y: auto;
            padding: 5px;
        }
        .topic-tag {
            display: inline-block;
            padding: 2px 8px;
            background-color: #e3f2fd;
            border-radius: 12px;
            font-size: 11px;
            color: #1976D2;
            border: 1px solid #bbdefb;
        }
        .status-badge {
            display: inline-block;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 11px;
            margin-left: 5px;
        }
        .status-stable {
            background-color: #e8f5e9;
            color: #2e7d32;
        }
        .status-rebalancing {
            background-color: #fff3e0;
            color: #ef6c00;
        }
        .status-dead {
            background-color: #ffebee;
            color: #c62828;
        }
        .lag-badge {
            display: inline-block;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 11px;
            margin-left: 5px;
            background-color: #f3e5f5;
            color: #7b1fa2;
        }
        .action-buttons {
            display: flex;
            gap: 5px;
            margin-top: 8px;
            flex-wrap: wrap;
        }
        .checkbox-group {
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .checkbox-group input[type="checkbox"] {
            width: auto;
        }
        .topic-list {
            margin-top: 15px;
            border: 1px solid #ddd;
            border-radius: 4px;
            max-height: 400px;
            overflow-y: auto;
        }
        .topic-list-header {
            padding: 10px;
            background-color: #f0f0f0;
            border-bottom: 1px solid #ddd;
            display: flex;
            align-items: center;
            gap: 15px;
        }
        .topic-list-header input[type="checkbox"] {
            width: auto;
        }
        .topic-item {
            padding: 10px;
            border-bottom: 1px solid #eee;
            display: flex;
            align-items: center;
            gap: 15px;
        }
        .topic-item:last-child {
            border-bottom: none;
        }
        .topic-item:hover {
            background-color: #f8f8f8;
        }
        .topic-item input[type="checkbox"] {
            width: auto;
        }
        .topic-item.internal {
            background-color: #fff3e0;
        }
        .topic-item.internal:hover {
            background-color: #ffe0b2;
        }
        .topic-info {
            flex: 1;
        }
        .topic-name {
            font-weight: bold;
            color: #333;
        }
        .topic-meta {
            font-size: 12px;
            color: #666;
            margin-top: 2px;
        }
        .topic-type-badge {
            font-size: 11px;
            padding: 2px 6px;
            border-radius: 3px;
            margin-left: 8px;
        }
        .topic-type-badge.internal {
            background-color: #ff9800;
            color: white;
        }
        .topic-type-badge.user {
            background-color: #4CAF50;
            color: white;
        }
        .batch-actions {
            margin-top: 15px;
            padding: 10px;
            background-color: #f5f5f5;
            border-radius: 4px;
            display: none;
        }
        .batch-actions.visible {
            display: flex;
            align-items: center;
            gap: 15px;
        }
        button.danger {
            background-color: #f44336;
        }
        button.danger:hover {
            background-color: #d32f2f;
        }
        .selected-count {
            font-weight: bold;
            color: #333;
        }
        .loading {
            text-align: center;
            padding: 20px;
            color: #666;
        }
        .no-data {
            text-align: center;
            padding: 30px;
            color: #999;
            font-style: italic;
        }
        .stats-bar {
            display: flex;
            gap: 15px;
            margin-bottom: 15px;
            padding: 10px;
            background-color: #f0f8ff;
            border-radius: 4px;
            border: 1px solid #d1eaff;
        }
        .stat-item {
            display: flex;
            flex-direction: column;
            align-items: center;
            padding: 5px 15px;
        }
        .stat-value {
            font-size: 18px;
            font-weight: bold;
            color: #2196F3;
        }
        .stat-label {
            font-size: 12px;
            color: #666;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Kafka Web工具</h1>
        <div class="tabs">
            <button class="tab" onclick="switchTab('create-topic')">创建主题</button>
            <button class="tab" onclick="switchTab('list-topics')">查看所有主题</button>
            <button class="tab" onclick="switchTab('list-consumer-groups')">查看所有消费者组</button>
            <button class="tab active" onclick="switchTab('produce')">生产消息</button>
            <button class="tab" onclick="switchTab('query-producer')">查询生产者消息</button>
            <button class="tab" onclick="switchTab('consume')">消费消息（独立）</button>
            <button class="tab" onclick="switchTab('consume-group')">消费消息（消费组）</button>
            <button class="tab" onclick="switchTab('group-status')">查询消费组状态</button>
        </div>

        <!-- 创建主题表单 -->
        <div id="create-topic" class="tab-content">
            <form id="create-topic-form">
                <div class="form-group">
                    <label>Kafka Broker列表（逗号分隔）</label>
                    <input type="text" name="broker_list" value="localhost:9092" required>
                </div>
                <div class="form-group">
                    <label>Topic名称</label>
                    <input type="text" name="topic" value="test_topic" required>
                    <div class="hint">只能包含字母、数字、下划线、连字符，不能以__开头</div>
                </div>
                <div class="form-group">
                    <label>Kafka版本</label>
                    <select name="kafka_version">
                        <option value="2.0.0">2.0.0</option>
                        <option value="2.5.0">2.5.0</option>
                        <option value="2.8.0" selected>2.8.0</option>
                        <option value="3.0.0">3.0.0</option>
                    </select>
                </div>
                <div class="form-group">
                    <label>超时时间(ms)</label>
                    <input type="number" name="timeout_ms" value="5000" min="1000">
                </div>
                <div class="form-group">
                    <label>分区数</label>
                    <input type="number" name="num_partitions" value="1" min="1">
                    <div class="hint">建议根据业务并发量设置，至少1个</div>
                </div>
                <div class="form-group">
                    <label>副本数</label>
                    <input type="number" name="replication_factor" value="1" min="1">
                    <div class="hint">不能超过Broker数量，生产环境建议3个</div>
                </div>
                <div class="form-group">
                    <label>消息保留小时数</label>
                    <input type="number" name="retention_hours" value="24" min="1">
                    <div class="hint">消息在Topic中保留的时间，默认24小时</div>
                </div>
                <div class="form-group">
                    <label>清理策略</label>
                    <select name="cleanup_policy">
                        <option value="delete" selected>delete（按时间删除）</option>
                        <option value="compact">compact（按Key压缩）</option>
                    </select>
                    <div class="hint">delete：删除旧消息；compact：保留每个Key的最新消息</div>
                </div>
                <button type="submit">创建Topic</button>
            </form>
            <div id="create-topic-result" class="result"></div>
        </div>

        <!-- 查看所有主题表单 -->
        <div id="list-topics" class="tab-content">
            <form id="list-topics-form">
                <div class="form-group">
                    <label>Kafka Broker列表（逗号分隔）</label>
                    <input type="text" name="broker_list" value="localhost:9092" required>
                </div>
                <div class="form-group">
                    <label>Kafka版本</label>
                    <select name="kafka_version">
                        <option value="2.0.0">2.0.0</option>
                        <option value="2.5.0">2.5.0</option>
                        <option value="2.8.0" selected>2.8.0</option>
                        <option value="3.0.0">3.0.0</option>
                    </select>
                </div>
                <div class="form-group">
                    <label>超时时间(ms)</label>
                    <input type="number" name="timeout_ms" value="5000" min="1000">
                    <div class="hint">连接Kafka的超时时间，默认5000ms</div>
                </div>
                <div class="btn-group">
                    <button type="button" onclick="queryTopics(false)">查询所有主题</button>
                    <button type="button" class="secondary" onclick="queryTopics(true)">查询用户创建主题</button>
                </div>
            </form>
            <div id="batch-actions" class="batch-actions">
                <span class="selected-count">已选择 <span id="selected-count-num">0</span> 个主题</span>
                <button type="button" class="danger" onclick="deleteSelectedTopics()">批量删除选中主题</button>
                <button type="button" class="secondary" onclick="clearTopicSelection()">取消选择</button>
            </div>
            <div id="list-topics-result" class="result"></div>
            <div id="topic-list-container"></div>
        </div>

        <!-- 查看所有消费者组表单（增强版） -->
        <div id="list-consumer-groups" class="tab-content">
            <form id="list-consumer-groups-form">
                <div class="form-group">
                    <label>Kafka Broker列表（逗号分隔）</label>
                    <input type="text" name="broker_list" value="localhost:9092" required>
                </div>
                <div class="form-group">
                    <label>Kafka版本</label>
                    <select name="kafka_version">
                        <option value="2.0.0">2.0.0</option>
                        <option value="2.5.0">2.5.0</option>
                        <option value="2.8.0" selected>2.8.0</option>
                        <option value="3.0.0">3.0.0</option>
                    </select>
                </div>
                <div class="form-group">
                    <label>超时时间(ms)</label>
                    <input type="number" name="timeout_ms" value="5000" min="1000">
                    <div class="hint">连接Kafka的超时时间，默认5000ms</div>
                </div>
                <button type="button" onclick="queryAllConsumerGroups()">查询所有消费者组</button>
            </form>
            <div id="list-consumer-groups-stats" class="stats-bar" style="display:none;">
                <div class="stat-item">
                    <div class="stat-value" id="stat-group-count">0</div>
                    <div class="stat-label">总组数</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value" id="stat-topic-count">0</div>
                    <div class="stat-label">总主题数</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value" id="stat-active-groups">0</div>
                    <div class="stat-label">活跃组</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value" id="stat-total-lag">0</div>
                    <div class="stat-label">总延迟</div>
                </div>
            </div>
            <div id="list-consumer-groups-result" class="result"></div>
        </div>

        <!-- 生产消息表单 -->
        <div id="produce" class="tab-content active">
            <form id="produce-form">
                <div class="form-group">
                    <label>Kafka Broker列表（逗号分隔）</label>
                    <input type="text" name="broker_list" value="localhost:9092" required>
                </div>
                <div class="form-group">
                    <label>Topic名称</label>
                    <input type="text" name="topic" value="test_topic" required>
                </div>
                <div class="form-group">
                    <label>Kafka版本</label>
                    <select name="kafka_version">
                        <option value="2.0.0">2.0.0</option>
                        <option value="2.5.0">2.5.0</option>
                        <option value="2.8.0" selected>2.8.0</option>
                        <option value="3.0.0">3.0.0</option>
                    </select>
                </div>
                <div class="form-group">
                    <label>超时时间(ms)</label>
                    <input type="number" name="timeout_ms" value="5000" min="1000">
                </div>
                <div class="form-group">
                    <label>分区号</label>
                    <input type="number" name="partition" value="0" min="0">
                </div>
                <div class="form-group">
                    <label>消息Key</label>
                    <input type="text" name="key" placeholder="可选">
                </div>
                <div class="form-group">
                    <label>消息Value</label>
                    <textarea name="value" placeholder="请输入消息内容" required>hello kafka from web</textarea>
                </div>
                <button type="submit">发送消息</button>
            </form>
            <div id="produce-result" class="result"></div>
        </div>

        <!-- 查询生产者消息表单 -->
        <div id="query-producer" class="tab-content">
            <form id="query-producer-form">
                <div class="form-group">
                    <label>Kafka Broker列表（逗号分隔）</label>
                    <input type="text" name="broker_list" value="localhost:9092" required>
                </div>
                <div class="form-group">
                    <label>Topic名称</label>
                    <input type="text" name="topic" value="test_topic" required>
                </div>
                <div class="form-group">
                    <label>Kafka版本</label>
                    <select name="kafka_version">
                        <option value="2.0.0">2.0.0</option>
                        <option value="2.5.0">2.5.0</option>
                        <option value="2.8.0" selected>2.8.0</option>
                        <option value="3.0.0">3.0.0</option>
                    </select>
                </div>
                <div class="form-group">
                    <label>超时时间(ms)</label>
                    <input type="number" name="timeout_ms" value="5000" min="1000">
                </div>
                <div class="form-group">
                    <label>分区号</label>
                    <input type="number" name="partition" value="0" min="0">
                </div>
                <div class="form-group">
                    <label>起始偏移量</label>
                    <input type="number" name="start_offset" value="0" min="0">
                    <div class="hint">从该偏移量开始查询，默认0（最旧消息）</div>
                </div>
                <div class="form-group">
                    <label>结束偏移量</label>
                    <input type="number" name="end_offset" value="-1" min="-1">
                    <div class="hint">-1表示最新消息，大于0则查询到该偏移量为止</div>
                </div>
                <div class="form-group">
                    <label>最大查询条数</label>
                    <input type="number" name="max_num" value="20" min="1" max="100">
                    <div class="hint">最多返回的消息条数，避免查询过多</div>
                </div>
                <button type="submit">查询消息</button>
            </form>
            <div id="query-producer-result" class="result"></div>
        </div>

        <!-- 消费消息表单（独立消费者） -->
        <div id="consume" class="tab-content">
            <form id="consume-form">
                <div class="form-group">
                    <label>Kafka Broker列表（逗号分隔）</label>
                    <input type="text" name="broker_list" value="localhost:9092" required>
                </div>
                <div class="form-group">
                    <label>Topic名称</label>
                    <input type="text" name="topic" value="test_topic" required>
                </div>
                <div class="form-group">
                    <label>Kafka版本</label>
                    <select name="kafka_version">
                        <option value="2.0.0">2.0.0</option>
                        <option value="2.5.0">2.5.0</option>
                        <option value="2.8.0" selected>2.8.0</option>
                        <option value="3.0.0">3.0.0</option>
                    </select>
                </div>
                <div class="form-group">
                    <label>超时时间(ms)</label>
                    <input type="number" name="timeout_ms" value="5000" min="1000">
                </div>
                <div class="form-group">
                    <label>分区号</label>
                    <input type="number" name="partition" value="0" min="0">
                </div>
                <div class="form-group">
                    <label>起始偏移量</label>
                    <input type="number" name="start_offset" value="0" min="0">
                    <div class="hint">从该偏移量开始消费，默认0（最旧消息）</div>
                </div>
                <div class="form-group">
                    <label>最大消费条数</label>
                    <input type="number" name="max_num" value="10" min="1">
                </div>
                <button type="submit">消费消息</button>
            </form>
            <div id="consume-result" class="result"></div>
        </div>

        <!-- 消费消息表单（消费组） -->
        <div id="consume-group" class="tab-content">
            <form id="consume-group-form">
                <div class="form-group">
                    <label>Kafka Broker列表（逗号分隔）</label>
                    <input type="text" name="broker_list" value="localhost:9092" required>
                </div>
                <div class="form-group">
                    <label>Topic名称</label>
                    <input type="text" name="topic" value="test_topic" required>
                </div>
                <div class="form-group">
                    <label>消费组名称</label>
                    <input type="text" name="group_name" id="consume_group_name" value="my_consumer_group" required>
                    <button type="button" class="copy-btn small-btn" onclick="pasteGroupNameToConsume()">粘贴</button>
                    <div class="hint">消费者将加入此消费组，组内消费者会均衡分配分区</div>
                </div>
                <div class="form-group">
                    <label>Kafka版本</label>
                    <select name="kafka_version">
                        <option value="2.0.0">2.0.0</option>
                        <option value="2.5.0">2.5.0</option>
                        <option value="2.8.0" selected>2.8.0</option>
                        <option value="3.0.0">3.0.0</option>
                    </select>
                </div>
                <div class="form-group">
                    <label>超时时间(ms)</label>
                    <input type="number" name="timeout_ms" value="5000" min="1000">
                    <div class="hint">消费超时时间，默认5000ms</div>
                </div>
                <div class="form-group">
                    <label>最大消费条数</label>
                    <input type="number" name="max_num" value="10" min="1">
                    <div class="hint">达到此数量后自动停止消费</div>
                </div>
                <div class="form-group">
                    <div class="checkbox-group">
                        <input type="checkbox" name="auto_commit" id="auto_commit" value="true">
                        <label for="auto_commit" style="margin-bottom: 0; font-weight: normal;">自动提交偏移量</label>
                    </div>
                    <div class="hint">勾选后消费的消息偏移量会自动提交，下次消费从此位置继续</div>
                </div>
                <button type="submit">开始消费</button>
            </form>
            <div id="consume-group-result" class="result"></div>
        </div>

        <!-- 查询消费组状态表单 -->
        <div id="group-status" class="tab-content">
            <form id="group-status-form">
                <div class="form-group">
                    <label>Kafka Broker列表（逗号分隔）</label>
                    <input type="text" name="broker_list" value="localhost:9092" required>
                </div>
                <div class="form-group">
                    <label>Topic名称</label>
                    <input type="text" name="topic" value="test_topic" required>
                </div>
                <div class="form-group">
                    <label>消费组名称</label>
                    <input type="text" name="group_name" id="group_name_input" value="my_consumer_group" required>
                    <button type="button" class="copy-btn small-btn" onclick="pasteGroupName()">粘贴</button>
                    <div class="hint">可从"查看所有消费者组"结果中复制名称粘贴到此处</div>
                </div>
                <div class="form-group">
                    <label>Kafka版本</label>
                    <select name="kafka_version">
                        <option value="2.0.0">2.0.0</option>
                        <option value="2.5.0">2.5.0</option>
                        <option value="2.8.0" selected>2.8.0</option>
                        <option value="3.0.0">3.0.0</option>
                    </select>
                </div>
                <div class="form-group">
                    <label>超时时间(ms)</label>
                    <input type="number" name="timeout_ms" value="5000" min="1000">
                </div>
                <button type="submit">查询状态</button>
            </form>
            <div id="group-status-result" class="result"></div>
        </div>
    </div>

    <script>
        let lastConsumerGroups = [];
        let lastTopics = [];
        let selectedTopics = new Set();
        
        function switchTab(tabId) {
            document.querySelectorAll('.tab-content').forEach(function(el) {
                el.classList.remove('active');
            });
            document.querySelectorAll('.tab').forEach(function(el) {
                el.classList.remove('active');
            });
            document.getElementById(tabId).classList.add('active');
            document.querySelector('.tab[onclick="switchTab(\'' + tabId + '\')"]').classList.add('active');
            
            // 如果切换到消费组列表，且已有数据，更新统计
            if (tabId === 'list-consumer-groups' && lastConsumerGroups.length > 0) {
                updateConsumerGroupStats(lastConsumerGroups);
            }
        }

        function updateConsumerGroupStats(groups) {
            if (groups.length === 0) {
                document.getElementById('list-consumer-groups-stats').style.display = 'none';
                return;
            }
            
            let totalGroups = groups.length;
            let totalTopics = new Set();
            let activeGroups = 0;
            let totalLag = 0;
            
            groups.forEach(function(group) {
                // 统计主题
                if (group.topics) {
                    group.topics.forEach(function(topic) {
                        totalTopics.add(topic);
                    });
                }
                
                // 统计活跃组
                if (group.state === '稳定' || group.state === 'Stable') {
                    activeGroups++;
                }
                
                // 统计延迟
                if (group.lag) {
                    totalLag += group.lag;
                }
            });
            
            document.getElementById('stat-group-count').textContent = totalGroups;
            document.getElementById('stat-topic-count').textContent = totalTopics.size;
            document.getElementById('stat-active-groups').textContent = activeGroups;
            document.getElementById('stat-total-lag').textContent = totalLag;
            document.getElementById('list-consumer-groups-stats').style.display = 'flex';
        }

        function getStateBadgeClass(state) {
            if (state.includes('稳定') || state === 'Stable') return 'status-stable';
            if (state.includes('重平衡') || state.includes('Rebalance')) return 'status-rebalancing';
            if (state.includes('死亡') || state === 'Dead') return 'status-dead';
            return 'status-stable';
        }

        document.getElementById('create-topic-form').addEventListener('submit', function(e) {
            e.preventDefault();
            const formData = new FormData(this);
            const resultEl = document.getElementById('create-topic-result');
            resultEl.innerHTML = '<div class="loading">处理中...</div>';
            
            fetch('/api/create-topic', {
                method: 'POST',
                body: new URLSearchParams(formData),
                headers: {'Content-Type': 'application/x-www-form-urlencoded'}
            })
            .then(function(res) {
                return res.json();
            })
            .then(function(data) {
                resultEl.className = 'result ' + (data.code === 0 ? 'success' : 'error');
                resultEl.innerHTML = JSON.stringify(data, null, 2);
            })
            .catch(function(err) {
                resultEl.className = 'result error';
                resultEl.innerHTML = '请求失败: ' + err.message;
            });
        });

        function queryTopics(filterInternal) {
            const form = document.getElementById('list-topics-form');
            const formData = new FormData(form);
            formData.append('filter_internal', filterInternal);
            
            const resultEl = document.getElementById('list-topics-result');
            const containerEl = document.getElementById('topic-list-container');
            resultEl.innerHTML = '<div class="loading">处理中...</div>';
            containerEl.innerHTML = '';
            selectedTopics.clear();
            updateBatchActions();
            
            fetch('/api/list-topics', {
                method: 'POST',
                body: new URLSearchParams(formData),
                headers: {'Content-Type': 'application/x-www-form-urlencoded'}
            })
            .then(function(res) {
                return res.json();
            })
            .then(function(data) {
                resultEl.className = 'result ' + (data.code === 0 ? 'success' : 'error');
                resultEl.innerHTML = JSON.stringify(data, null, 2);
                
                if (data.code === 0 && data.data && data.data.length > 0) {
                    lastTopics = data.data;
                    renderTopicList(data.data);
                }
            })
            .catch(function(err) {
                resultEl.className = 'result error';
                resultEl.innerHTML = '请求失败: ' + err.message;
            });
        }

        function renderTopicList(topics) {
            const containerEl = document.getElementById('topic-list-container');
            let html = '<div class="topic-list">';
            
            // 表头
            html += '<div class="topic-list-header">';
            html += '<input type="checkbox" id="select-all-topics" onchange="toggleSelectAllTopics(this)">';
            html += '<label for="select-all-topics" style="margin-bottom:0;font-weight:normal;">全选</label>';
            html += '<span style="color:#666;">共 ' + topics.length + ' 个主题</span>';
            html += '</div>';
            
            // 主题列表
            topics.forEach(function(topic, index) {
                const isInternal = topic.topic_type === 'internal';
                const itemClass = isInternal ? 'topic-item internal' : 'topic-item';
                const disabled = isInternal ? 'disabled' : '';
                const badgeClass = isInternal ? 'internal' : 'user';
                const badgeText = isInternal ? '内部' : '用户';
                
                html += '<div class="' + itemClass + '">';
                html += '<input type="checkbox" class="topic-checkbox" data-topic="' + topic.name + '" ' + disabled + ' onchange="toggleTopicSelection(this, \'' + topic.name + '\')">';
                html += '<div class="topic-info">';
                html += '<div class="topic-name">' + topic.name + '<span class="topic-type-badge ' + badgeClass + '">' + badgeText + '</span></div>';
                html += '<div class="topic-meta">分区数: ' + topic.num_partitions + ' | 副本数: ' + topic.replication_factor + '</div>';
                html += '</div>';
                html += '</div>';
            });
            
            html += '</div>';
            containerEl.innerHTML = html;
        }

        function toggleSelectAllTopics(checkbox) {
            const checkboxes = document.querySelectorAll('.topic-checkbox:not([disabled])');
            checkboxes.forEach(function(cb) {
                cb.checked = checkbox.checked;
                const topicName = cb.getAttribute('data-topic');
                if (checkbox.checked) {
                    selectedTopics.add(topicName);
                } else {
                    selectedTopics.delete(topicName);
                }
            });
            updateBatchActions();
        }

        function toggleTopicSelection(checkbox, topicName) {
            if (checkbox.checked) {
                selectedTopics.add(topicName);
            } else {
                selectedTopics.delete(topicName);
            }
            updateBatchActions();
            
            const allCheckboxes = document.querySelectorAll('.topic-checkbox:not([disabled])');
            const checkedBoxes = document.querySelectorAll('.topic-checkbox:not([disabled]):checked');
            document.getElementById('select-all-topics').checked = allCheckboxes.length === checkedBoxes.length;
        }

        function updateBatchActions() {
            const batchActions = document.getElementById('batch-actions');
            const countSpan = document.getElementById('selected-count-num');
            countSpan.textContent = selectedTopics.size;
            
            if (selectedTopics.size > 0) {
                batchActions.classList.add('visible');
            } else {
                batchActions.classList.remove('visible');
            }
        }

        function clearTopicSelection() {
            selectedTopics.clear();
            const checkboxes = document.querySelectorAll('.topic-checkbox');
            checkboxes.forEach(function(cb) {
                cb.checked = false;
            });
            document.getElementById('select-all-topics').checked = false;
            updateBatchActions();
        }

        function deleteSelectedTopics() {
            if (selectedTopics.size === 0) {
                alert('请先选择要删除的主题');
                return;
            }
            
            const topicNames = Array.from(selectedTopics);
            const confirmMsg = '确定要删除以下 ' + topicNames.length + ' 个主题吗？\\n\\n' + topicNames.join('\\n') + '\\n\\n此操作不可恢复！';
            if (!confirm(confirmMsg)) {
                return;
            }
            
            const form = document.getElementById('list-topics-form');
            const formData = new FormData(form);
            topicNames.forEach(function(name) {
                formData.append('topics[]', name);
            });
            
            const resultEl = document.getElementById('list-topics-result');
            resultEl.innerHTML = '<div class="loading">正在删除主题...</div>';
            
            fetch('/api/delete-topics', {
                method: 'POST',
                body: new URLSearchParams(formData),
                headers: {'Content-Type': 'application/x-www-form-urlencoded'}
            })
            .then(function(res) {
                return res.json();
            })
            .then(function(data) {
                resultEl.className = 'result ' + (data.code === 0 ? 'success' : 'error');
                resultEl.innerHTML = JSON.stringify(data, null, 2);
                
                if (data.code === 0) {
                    clearTopicSelection();
                    setTimeout(function() {
                        queryTopics(false);
                    }, 1000);
                }
            })
            .catch(function(err) {
                resultEl.className = 'result error';
                resultEl.innerHTML = '请求失败: ' + err.message;
            });
        }

        function queryAllConsumerGroups() {
            const form = document.getElementById('list-consumer-groups-form');
            const formData = new FormData(form);
            const resultEl = document.getElementById('list-consumer-groups-result');
            resultEl.innerHTML = '<div class="loading">处理中...</div>';
            
            fetch('/api/list-consumer-groups', {
                method: 'POST',
                body: new URLSearchParams(formData),
                headers: {'Content-Type': 'application/x-www-form-urlencoded'}
            })
            .then(function(res) {
                return res.json();
            })
            .then(function(data) {
                resultEl.className = 'result ' + (data.code === 0 ? 'success' : 'error');
                
                if (data.code === 0 && data.data && Array.isArray(data.data) && data.data.length > 0) {
                    lastConsumerGroups = data.data;
                    updateConsumerGroupStats(data.data);
                    renderConsumerGroups(data.data);
                } else if (data.code === 1) {
                    resultEl.innerHTML = '<div class="no-data">' + data.message + '</div>';
                } else {
                    resultEl.innerHTML = '<div class="no-data">未查询到任何消费者组</div>';
                }
            })
            .catch(function(err) {
                resultEl.className = 'result error';
                resultEl.innerHTML = '请求失败: ' + err.message;
            });
        }

        function renderConsumerGroups(groups) {
            const resultEl = document.getElementById('list-consumer-groups-result');
            let html = '';
            
            groups.forEach(function(group) {
                const stateClass = getStateBadgeClass(group.state);
                const lagDisplay = group.lag > 0 ? '<span class="lag-badge">延迟: ' + group.lag + '</span>' : '';
                
                html += '<div class="group-item">';
                html += '<div class="group-header">';
                html += '<div class="group-name">' + group.name + ' <span class="status-badge ' + stateClass + '">' + group.state + '</span>' + lagDisplay + '</div>';
                html += '<div class="action-buttons">';
                html += '<button class="copy-btn small-btn" onclick="copyGroupName(\'' + group.name + '\')">复制名称</button>';
                html += '<button class="copy-btn small-btn" onclick="viewGroupTopics(\'' + group.name + '\')">查看主题</button>';
                html += '<button class="copy-btn small-btn" onclick="useGroupForConsume(\'' + group.name + '\')">用于消费</button>';
                html += '</div>';
                html += '</div>';
                
                html += '<div class="group-meta">';
                html += '<span>类型: ' + group.type + '</span>';
                html += '<span>成员数: ' + group.member_count + '</span>';
                html += '<span>查询时间: ' + group.query_time + '</span>';
                html += '</div>';
                
                html += '<div class="group-description">' + group.description + '</div>';
                
                if (group.topics && group.topics.length > 0) {
                    html += '<div class="group-topics">';
                    html += '<div class="topics-header">消费主题 (' + group.topics.length + ' 个):</div>';
                    html += '<div class="topics-list">';
                    group.topics.forEach(function(topic) {
                        html += '<span class="topic-tag">' + topic + '</span>';
                    });
                    html += '</div>';
                    html += '</div>';
                }
                
                html += '</div>';
            });
            
            resultEl.innerHTML = html;
        }

        function viewGroupTopics(groupName) {
            const form = document.getElementById('list-consumer-groups-form');
            const formData = new FormData(form);
            formData.append('group_name', groupName);
            
            const resultEl = document.getElementById('list-consumer-groups-result');
            resultEl.innerHTML = '<div class="loading">查询中...</div>';
            
            fetch('/api/group-topics', {
                method: 'POST',
                body: new URLSearchParams(formData)
            })
            .then(function(res) {
                return res.json();
            })
            .then(function(data) {
                resultEl.className = 'result ' + (data.code === 0 ? 'success' : 'error');
                if (data.code === 0) {
                    let html = '<div>消费组 <strong>' + groupName + '</strong> 消费的主题:</div>';
                    if (data.data && data.data.length > 0) {
                        html += '<div style="margin-top:10px;">';
                        data.data.forEach(function(topic) {
                            html += '<div style="padding:5px;border-bottom:1px solid #eee;">' + topic + '</div>';
                        });
                        html += '</div>';
                    } else {
                        html += '<div style="margin-top:10px;color:#999;">该消费组未消费任何主题</div>';
                    }
                    html += '<div style="margin-top:15px;"><button onclick="queryAllConsumerGroups()">返回列表</button></div>';
                    resultEl.innerHTML = html;
                } else {
                    resultEl.innerHTML = data.message;
                }
            })
            .catch(function(err) {
                resultEl.className = 'result error';
                resultEl.innerHTML = '请求失败: ' + err.message;
            });
        }

        function useGroupForConsume(groupName) {
            document.getElementById('consume_group_name').value = groupName;
            document.getElementById('group_name_input').value = groupName;
            switchTab('consume-group');
            alert('已设置消费组名称: ' + groupName);
        }

        function copyGroupName(groupName) {
            navigator.clipboard.writeText(groupName).then(function() {
                alert('消费组名称已复制: ' + groupName);
            }).catch(function(err) {
                alert('复制失败: ' + err.message);
            });
        }

        function pasteGroupName() {
            if (lastConsumerGroups.length === 0) {
                alert('暂无消费组数据，请先查询所有消费者组');
                return;
            }
            const firstGroupName = lastConsumerGroups[0].name;
            document.getElementById('group_name_input').value = firstGroupName;
            alert('已粘贴第一个消费组名称: ' + firstGroupName);
        }

        function pasteGroupNameToConsume() {
            if (lastConsumerGroups.length === 0) {
                alert('暂无消费组数据，请先查询所有消费者组');
                return;
            }
            const firstGroupName = lastConsumerGroups[0].name;
            document.getElementById('consume_group_name').value = firstGroupName;
            alert('已粘贴第一个消费组名称: ' + firstGroupName);
        }

        document.getElementById('produce-form').addEventListener('submit', function(e) {
            e.preventDefault();
            const formData = new FormData(this);
            const resultEl = document.getElementById('produce-result');
            resultEl.innerHTML = '<div class="loading">处理中...</div>';
            
            fetch('/api/produce', {
                method: 'POST',
                body: new URLSearchParams(formData),
                headers: {'Content-Type': 'application/x-www-form-urlencoded'}
            })
            .then(function(res) {
                return res.json();
            })
            .then(function(data) {
                resultEl.className = 'result ' + (data.code === 0 ? 'success' : 'error');
                resultEl.innerHTML = JSON.stringify(data, null, 2);
            })
            .catch(function(err) {
                resultEl.className = 'result error';
                resultEl.innerHTML = '请求失败: ' + err.message;
            });
        });

        document.getElementById('query-producer-form').addEventListener('submit', function(e) {
            e.preventDefault();
            const formData = new FormData(this);
            const resultEl = document.getElementById('query-producer-result');
            resultEl.innerHTML = '<div class="loading">处理中...</div>';
            
            fetch('/api/query-producer', {
                method: 'POST',
                body: new URLSearchParams(formData),
                headers: {'Content-Type': 'application/x-www-form-urlencoded'}
            })
            .then(function(res) {
                return res.json();
            })
            .then(function(data) {
                resultEl.className = 'result ' + (data.code === 0 ? 'success' : 'error');
                resultEl.innerHTML = JSON.stringify(data, null, 2);
            })
            .catch(function(err) {
                resultEl.className = 'result error';
                resultEl.innerHTML = '请求失败: ' + err.message;
            });
        });

        document.getElementById('consume-form').addEventListener('submit', function(e) {
            e.preventDefault();
            const formData = new FormData(this);
            const resultEl = document.getElementById('consume-result');
            resultEl.innerHTML = '<div class="loading">处理中...</div>';
            
            fetch('/api/consume', {
                method: 'POST',
                body: new URLSearchParams(formData),
                headers: {'Content-Type': 'application/x-www-form-urlencoded'}
            })
            .then(function(res) {
                return res.json();
            })
            .then(function(data) {
                resultEl.className = 'result ' + (data.code === 0 ? 'success' : 'error');
                resultEl.innerHTML = JSON.stringify(data, null, 2);
            })
            .catch(function(err) {
                resultEl.className = 'result error';
                resultEl.innerHTML = '请求失败: ' + err.message;
            });
        });

        document.getElementById('consume-group-form').addEventListener('submit', function(e) {
            e.preventDefault();
            const formData = new FormData(this);
            const resultEl = document.getElementById('consume-group-result');
            resultEl.innerHTML = '<div class="loading">消费组消费中，请稍候...</div>';
            
            fetch('/api/consume-group', {
                method: 'POST',
                body: new URLSearchParams(formData),
                headers: {'Content-Type': 'application/x-www-form-urlencoded'}
            })
            .then(function(res) {
                return res.json();
            })
            .then(function(data) {
                resultEl.className = 'result ' + (data.code === 0 ? 'success' : 'error');
                resultEl.innerHTML = JSON.stringify(data, null, 2);
            })
            .catch(function(err) {
                resultEl.className = 'result error';
                resultEl.innerHTML = '请求失败: ' + err.message;
            });
        });

        document.getElementById('group-status-form').addEventListener('submit', function(e) {
            e.preventDefault();
            const formData = new FormData(this);
            const resultEl = document.getElementById('group-status-result');
            resultEl.innerHTML = '<div class="loading">处理中...</div>';
            
            fetch('/api/group-status', {
                method: 'POST',
                body: new URLSearchParams(formData),
                headers: {'Content-Type': 'application/x-www-form-urlencoded'}
            })
            .then(function(res) {
                return res.json();
            })
            .then(function(data) {
                resultEl.className = 'result ' + (data.code === 0 ? 'success' : 'error');
                resultEl.innerHTML = JSON.stringify(data, null, 2);
            })
            .catch(function(err) {
                resultEl.className = 'result error';
                resultEl.innerHTML = '请求失败: ' + err.message;
            });
        });
    </script>
</body>
</html>`

// ------------------------------
// 主函数
// ------------------------------
func main() {
	var port int
	flag.IntVar(&port, "port", 8010, "web service port")
	flag.Parse()

	if port < 1 || port > 65535 {
		log.Fatalf("invalid port: %d", port)
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusOK)
			return
		}
		c.Next()
	})

	r.GET("/", func(c *gin.Context) {
		tmpl, err := template.New("index").Parse(indexTemplate)
		if err != nil {
			c.String(http.StatusInternalServerError, "template parse error: %v", err)
			return
		}
		tmpl.Execute(c.Writer, nil)
	})

	api := r.Group("/api")
	{
		api.POST("/create-topic", apiHandler(func(c *gin.Context) (interface{}, error) {
			var req CreateTopicRequest
			if err := c.ShouldBind(&req); err != nil {
				return nil, fmt.Errorf("invalid request: %v", err)
			}
			return createTopic(c, req)
		}))

		api.POST("/list-topics", apiHandler(func(c *gin.Context) (interface{}, error) {
			var req ListTopicsRequest
			if err := c.ShouldBind(&req); err != nil {
				return nil, fmt.Errorf("invalid request: %v", err)
			}
			return getAllTopics(c, req)
		}))

		api.POST("/delete-topics", apiHandler(func(c *gin.Context) (interface{}, error) {
			var req DeleteTopicsRequest
			if err := c.ShouldBind(&req); err != nil {
				return nil, fmt.Errorf("invalid request: %v", err)
			}
			return deleteTopics(c, req)
		}))

		api.POST("/group-topics", apiHandler(func(c *gin.Context) (interface{}, error) {
			var req GroupTopicsRequest
			if err := c.ShouldBind(&req); err != nil {
				return nil, fmt.Errorf("invalid request: %v", err)
			}
			return getGroupTopics(c, req)
		}))

		api.POST("/list-consumer-groups", apiHandler(func(c *gin.Context) (interface{}, error) {
			var req ListConsumerGroupsRequest
			if err := c.ShouldBind(&req); err != nil {
				return nil, fmt.Errorf("invalid request: %v", err)
			}
			return getAllConsumerGroups(c, req)
		}))

		api.POST("/produce", apiHandler(func(c *gin.Context) (interface{}, error) {
			var req ProducerRequest
			if err := c.ShouldBind(&req); err != nil {
				return nil, fmt.Errorf("invalid request: %v", err)
			}
			return produceMessage(c, req)
		}))

		api.POST("/query-producer", apiHandler(func(c *gin.Context) (interface{}, error) {
			var req ProducerMsgQueryRequest
			if err := c.ShouldBind(&req); err != nil {
				return nil, fmt.Errorf("invalid request: %v", err)
			}
			return queryProducerMessages(c, req)
		}))

		api.POST("/consume", apiHandler(func(c *gin.Context) (interface{}, error) {
			var req ConsumerRequest
			if err := c.ShouldBind(&req); err != nil {
				return nil, fmt.Errorf("invalid request: %v", err)
			}
			return consumeMessage(c, req)
		}))

		api.POST("/consume-group", apiHandler(func(c *gin.Context) (interface{}, error) {
			var req ConsumerGroupRequest
			if err := c.ShouldBind(&req); err != nil {
				return nil, fmt.Errorf("invalid request: %v", err)
			}
			return consumeMessageWithGroup(c, req)
		}))

		api.POST("/group-status", apiHandler(func(c *gin.Context) (interface{}, error) {
			var req GroupStatusRequest
			if err := c.ShouldBind(&req); err != nil {
				return nil, fmt.Errorf("invalid request: %v", err)
			}
			return getGroupStatus(c, req)
		}))
	}

	listenAddr := fmt.Sprintf(":%d", port)
	log.Printf("Kafka Web工具启动成功！")
	log.Printf("监听端口: %d", port)
	log.Printf("访问地址: http://localhost:%d", port)

	if err := r.Run(listenAddr); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
