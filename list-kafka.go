package main

import (
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

// ------------------------------
// 数据结构定义
// ------------------------------
// KafkaConfig Web页面传入的Kafka配置
type KafkaConfig struct {
	BrokerList   string `form:"broker_list" json:"broker_list"` // 逗号分隔的Broker列表
	Topic        string `form:"topic" json:"topic"`
	KafkaVersion string `form:"kafka_version" json:"kafka_version"` // 如2.8.0
	TimeoutMS    int    `form:"timeout_ms" json:"timeout_ms"`       // 超时时间，默认5000
}

// CreateTopicRequest 创建Topic请求参数
type CreateTopicRequest struct {
	KafkaConfig
	NumPartitions     int32  `form:"num_partitions" json:"num_partitions"`         // 分区数，默认1
	ReplicationFactor int16  `form:"replication_factor" json:"replication_factor"` // 副本数，默认1
	RetentionHours    int    `form:"retention_hours" json:"retention_hours"`       // 消息保留小时数，默认24
	CleanupPolicy     string `form:"cleanup_policy" json:"cleanup_policy"`         // 清理策略：delete/compact，默认delete
}

// ProducerRequest 生产消息请求参数
type ProducerRequest struct {
	KafkaConfig
	Partition int32  `form:"partition" json:"partition"` // 分区号，默认0
	Key       string `form:"key" json:"key"`             // 消息Key
	Value     string `form:"value" json:"value"`         // 消息Value
}

// ConsumerRequest 消费消息请求参数
type ConsumerRequest struct {
	KafkaConfig
	Partition   int32 `form:"partition" json:"partition"`       // 分区号，默认0
	StartOffset int64 `form:"start_offset" json:"start_offset"` // 起始偏移量，默认0
	MaxNum      int   `form:"max_num" json:"max_num"`           // 最大消费条数，默认10
}

// GroupStatusRequest 消费组状态查询请求参数
type GroupStatusRequest struct {
	KafkaConfig
	GroupName string `form:"group_name" json:"group_name"` // 消费组名称
}

// ProducerMsgQueryRequest 查询生产者消息请求参数
type ProducerMsgQueryRequest struct {
	KafkaConfig
	Partition   int32 `form:"partition" json:"partition"`       // 分区号，默认0
	StartOffset int64 `form:"start_offset" json:"start_offset"` // 起始偏移量，默认0
	EndOffset   int64 `form:"end_offset" json:"end_offset"`     // 结束偏移量，默认-1（最新）
	MaxNum      int   `form:"max_num" json:"max_num"`           // 最大查询条数，默认20
}

// ListTopicsRequest 查看所有主题请求参数
type ListTopicsRequest struct {
	KafkaConfig
	FilterInternal bool `form:"filter_internal" json:"filter_internal"` // 是否过滤内部主题（__开头）
}

// TopicListItem 主题列表项
type TopicListItem struct {
	Name              string `json:"name"`               // 主题名称
	NumPartitions     int32  `json:"num_partitions"`     // 分区数
	ReplicationFactor int16  `json:"replication_factor"` // 副本数
	CreatedTime       string `json:"created_time"`       // 查询时间
	TopicType         string `json:"topic_type"`         // 主题类型：internal（内部）/user（用户）
}

// ListConsumerGroupsRequest 查看所有消费者组请求参数
type ListConsumerGroupsRequest struct {
	KafkaConfig
}

// ConsumerGroupListItem 消费者组列表项
type ConsumerGroupListItem struct {
	Name        string `json:"name"`        // 消费组名称
	Type        string `json:"type"`        // 消费组类型：consumer/connect/mirror-maker/unknown
	QueryTime   string `json:"query_time"`  // 查询时间
	Description string `json:"description"` // 消费组描述
}

// TopicDetail Topic详情
type TopicDetail struct {
	Name              string `json:"name"`
	NumPartitions     int32  `json:"num_partitions"`
	ReplicationFactor int16  `json:"replication_factor"`
	RetentionHours    int    `json:"retention_hours"`
	CleanupPolicy     string `json:"cleanup_policy"`
	CreatedTime       string `json:"created_time"`
}

// ProducerMessageDetail 生产者消息详情
type ProducerMessageDetail struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Timestamp string `json:"timestamp"`
}

// ConsumerStatus 消费者消费状态
type ConsumerStatus struct {
	GroupName  string `json:"group_name"`
	Topic      string `json:"topic"`
	Partition  int32  `json:"partition"`
	Offset     int64  `json:"offset"`
	HighWater  int64  `json:"high_water"`
	Lag        int64  `json:"lag"`
	LastUpdate string `json:"last_update"`
}

// 新增：查询主题关联消费组的请求参数
type TopicConsumerGroupsRequest struct {
	KafkaConfig
}

// 新增：主题关联消费组的详情项（消费组+该组在Topic的消费状态）
type TopicConsumerGroupDetail struct {
	GroupName     string           `json:"group_name"`     // 消费组名称
	GroupType     string           `json:"group_type"`     // 消费组类型
	ConsumerStats []ConsumerStatus `json:"consumer_stats"` // 该消费组在Topic各分区的消费状态
}

// Response 统一返回格式
type Response struct {
	Code    int         `json:"code"`    // 0成功，1失败
	Message string      `json:"message"` // 提示信息
	Data    interface{} `json:"data"`    // 数据
}

// ------------------------------
// 工具函数
// ------------------------------
func getKafkaVersion(versionStr string) sarama.KafkaVersion {
	switch versionStr {
	case "2.0.0":
		return sarama.V2_0_0_0
	case "2.5.0":
		return sarama.V2_5_0_0
	case "2.8.0":
		return sarama.V2_8_0_0
	case "3.0.0":
		return sarama.V3_0_0_0
	default:
		return sarama.V2_8_0_0
	}
}

func parseBrokerList(brokerStr string) []string {
	var brokerList []string
	if brokerStr != "" {
		brokerStr = strings.ReplaceAll(brokerStr, " ", "")
		brokerList = strings.Split(brokerStr, ",")
	}
	return brokerList
}

func getPartitionHighWater(brokerList []string, kafkaVersion sarama.KafkaVersion, timeoutMS int, topic string, partition int32) (int64, error) {
	config := sarama.NewConfig()
	config.Version = kafkaVersion
	config.Net.DialTimeout = time.Duration(timeoutMS) * time.Millisecond

	if len(brokerList) == 0 {
		return 0, fmt.Errorf("Broker列表为空")
	}
	broker := sarama.NewBroker(brokerList[0])
	if err := broker.Open(config); err != nil {
		return 0, fmt.Errorf("连接Broker[%s]失败: %v", brokerList[0], err)
	}
	defer broker.Close()

	connected, err := broker.Connected()
	if err != nil {
		return 0, fmt.Errorf("检查Broker连接状态失败: %v", err)
	}
	if !connected {
		return 0, fmt.Errorf("Broker[%s]未连接", brokerList[0])
	}

	req := &sarama.OffsetRequest{
		Version: 1,
	}
	req.AddBlock(topic, partition, sarama.OffsetNewest, 1)

	resp, err := broker.GetAvailableOffsets(req)
	if err != nil {
		return 0, fmt.Errorf("获取分区%d位移失败: %v", partition, err)
	}

	block := resp.GetBlock(topic, partition)
	if block == nil || block.Err != sarama.ErrNoError {
		return 0, fmt.Errorf("分区%d响应错误: %v", partition, block.Err)
	}

	if len(block.Offsets) == 0 {
		return 0, nil
	}
	return block.Offsets[0] - 1, nil
}

func isInternalTopic(topicName string) bool {
	return strings.HasPrefix(topicName, "__")
}

// 适配新版sarama：直接处理字符串类型的消费组类型
func getConsumerGroupTypeDesc(groupTypeStr string) string {
	switch strings.ToLower(groupTypeStr) {
	case "consumer":
		return "consumer" // 普通消费组
	case "connect":
		return "connect" // Connect消费组
	case "mirror-maker":
		return "mirror-maker" // MirrorMaker消费组
	default:
		return "unknown" // 未知类型
	}
}

// ------------------------------
// 核心业务逻辑
// ------------------------------
func createTopic(c *gin.Context, req CreateTopicRequest) (interface{}, error) {
	if req.BrokerList == "" {
		return nil, fmt.Errorf("Broker列表不能为空")
	}
	if req.Topic == "" {
		return nil, fmt.Errorf("Topic名称不能为空")
	}
	if req.NumPartitions <= 0 {
		req.NumPartitions = 1
	}
	if req.ReplicationFactor <= 0 {
		req.ReplicationFactor = 1
	}
	if req.ReplicationFactor > int16(len(parseBrokerList(req.BrokerList))) {
		return nil, fmt.Errorf("副本数(%d)不能大于Broker数量(%d)", req.ReplicationFactor, len(parseBrokerList(req.BrokerList)))
	}
	if req.TimeoutMS <= 0 {
		req.TimeoutMS = 5000
	}
	if req.RetentionHours <= 0 {
		req.RetentionHours = 24
	}
	if req.CleanupPolicy == "" {
		req.CleanupPolicy = "delete"
	}
	if req.CleanupPolicy != "delete" && req.CleanupPolicy != "compact" {
		return nil, fmt.Errorf("清理策略只能是delete或compact")
	}

	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("Broker列表格式错误，应为逗号分隔，如192.168.1.100:9092,192.168.1.101:9092")
	}

	config := sarama.NewConfig()
	config.Version = getKafkaVersion(req.KafkaVersion)
	config.Net.DialTimeout = time.Duration(req.TimeoutMS) * time.Millisecond
	config.Admin.Timeout = time.Duration(req.TimeoutMS) * time.Millisecond

	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		return nil, fmt.Errorf("创建Client失败: %v", err)
	}
	defer client.Close()

	adminClient, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("创建管理客户端失败: %v", err)
	}
	defer adminClient.Close()

	topics, err := adminClient.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("查询Topic列表失败: %v", err)
	}
	if _, exists := topics[req.Topic]; exists {
		return nil, fmt.Errorf("Topic %s 已存在", req.Topic)
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
		return nil, fmt.Errorf("创建Topic失败: %v", err)
	}

	result := TopicDetail{
		Name:              req.Topic,
		NumPartitions:     req.NumPartitions,
		ReplicationFactor: req.ReplicationFactor,
		RetentionHours:    req.RetentionHours,
		CleanupPolicy:     req.CleanupPolicy,
		CreatedTime:       time.Now().Format("2006-01-02 15:04:05"),
	}

	return result, nil
}

func produceMessage(c *gin.Context, req ProducerRequest) (interface{}, error) {
	if req.BrokerList == "" {
		return nil, fmt.Errorf("Broker列表不能为空")
	}
	if req.Topic == "" {
		return nil, fmt.Errorf("Topic不能为空")
	}
	if req.Value == "" {
		return nil, fmt.Errorf("消息内容不能为空")
	}
	if req.TimeoutMS <= 0 {
		req.TimeoutMS = 5000
	}

	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("Broker列表格式错误，应为逗号分隔，如192.168.1.100:9092,192.168.1.101:9092")
	}

	config := sarama.NewConfig()
	config.Version = getKafkaVersion(req.KafkaVersion)
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Return.Successes = true
	config.Net.DialTimeout = time.Duration(req.TimeoutMS) * time.Millisecond

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return nil, fmt.Errorf("创建生产者失败: %v", err)
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
		return nil, fmt.Errorf("发送消息失败: %v", err)
	}

	result := ProducerMessageDetail{
		Topic:     req.Topic,
		Partition: partition,
		Offset:    offset,
		Key:       req.Key,
		Value:     req.Value,
		Timestamp: time.Now().Format("2006-01-02 15:04:05"),
	}

	return result, nil
}

func consumeMessage(c *gin.Context, req ConsumerRequest) (interface{}, error) {
	if req.BrokerList == "" {
		return nil, fmt.Errorf("Broker列表不能为空")
	}
	if req.Topic == "" {
		return nil, fmt.Errorf("Topic不能为空")
	}
	if req.TimeoutMS <= 0 {
		req.TimeoutMS = 5000
	}
	if req.MaxNum <= 0 {
		req.MaxNum = 10
	}
	if req.StartOffset < 0 {
		req.StartOffset = 0
	}

	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("Broker列表格式错误，应为逗号分隔，如192.168.1.100:9092,192.168.1.101:9092")
	}

	config := sarama.NewConfig()
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = getKafkaVersion(req.KafkaVersion)
	config.Net.DialTimeout = time.Duration(req.TimeoutMS) * time.Millisecond

	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		return nil, fmt.Errorf("创建消费者失败: %v", err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(req.Topic, req.Partition, req.StartOffset)
	if err != nil {
		return nil, fmt.Errorf("订阅分区%d失败: %v", req.Partition, err)
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

func queryProducerMessages(c *gin.Context, req ProducerMsgQueryRequest) (interface{}, error) {
	if req.BrokerList == "" {
		return nil, fmt.Errorf("Broker列表不能为空")
	}
	if req.Topic == "" {
		return nil, fmt.Errorf("Topic不能为空")
	}
	if req.TimeoutMS <= 0 {
		req.TimeoutMS = 5000
	}
	if req.MaxNum <= 0 {
		req.MaxNum = 20
	}
	if req.StartOffset < 0 {
		req.StartOffset = 0
	}

	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("Broker列表格式错误，应为逗号分隔，如192.168.1.100:9092,192.168.1.101:9092")
	}

	var endOffset int64
	if req.EndOffset <= 0 {
		hw, err := getPartitionHighWater(brokerList, getKafkaVersion(req.KafkaVersion), req.TimeoutMS, req.Topic, req.Partition)
		if err != nil {
			return nil, fmt.Errorf("获取分区%d最新位移失败: %v", req.Partition, err)
		}
		endOffset = hw
	} else {
		endOffset = req.EndOffset
	}

	if req.StartOffset > endOffset {
		return nil, fmt.Errorf("起始偏移量(%d)不能大于结束偏移量(%d)", req.StartOffset, endOffset)
	}

	config := sarama.NewConfig()
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = getKafkaVersion(req.KafkaVersion)
	config.Net.DialTimeout = time.Duration(req.TimeoutMS) * time.Millisecond

	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		return nil, fmt.Errorf("创建消费者失败: %v", err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(req.Topic, req.Partition, req.StartOffset)
	if err != nil {
		return nil, fmt.Errorf("订阅分区%d失败: %v", req.Partition, err)
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

func getGroupStatus(c *gin.Context, req GroupStatusRequest) (interface{}, error) {
	if req.BrokerList == "" {
		return nil, fmt.Errorf("Broker列表不能为空")
	}
	if req.Topic == "" {
		return nil, fmt.Errorf("Topic不能为空")
	}
	if req.GroupName == "" {
		return nil, fmt.Errorf("消费组名称不能为空")
	}
	if req.TimeoutMS <= 0 {
		req.TimeoutMS = 5000
	}

	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("Broker列表格式错误，应为逗号分隔，如192.168.1.100:9092,192.168.1.101:9092")
	}

	config := sarama.NewConfig()
	config.Version = getKafkaVersion(req.KafkaVersion)
	config.Net.DialTimeout = time.Duration(req.TimeoutMS) * time.Millisecond

	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		return nil, fmt.Errorf("创建Client失败: %v", err)
	}
	defer client.Close()

	adminClient, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("创建管理客户端失败: %v", err)
	}
	defer adminClient.Close()

	topicMeta, err := adminClient.DescribeTopics([]string{req.Topic})
	if err != nil {
		return nil, fmt.Errorf("查询Topic元数据失败: %v", err)
	}
	if len(topicMeta) == 0 {
		return nil, fmt.Errorf("Topic %s 不存在", req.Topic)
	}

	var partitionIDs []int32
	for _, p := range topicMeta[0].Partitions {
		partitionIDs = append(partitionIDs, p.ID)
	}
	if len(partitionIDs) == 0 {
		return nil, fmt.Errorf("Topic %s 无分区", req.Topic)
	}

	groupOffsets, err := adminClient.ListConsumerGroupOffsets(req.GroupName, map[string][]int32{req.Topic: partitionIDs})
	if err != nil {
		return nil, fmt.Errorf("查询消费组位移失败: %v", err)
	}

	highWaterMap := make(map[int32]int64)
	for _, p := range partitionIDs {
		hw, err := getPartitionHighWater(brokerList, config.Version, req.TimeoutMS, req.Topic, p)
		if err != nil {
			log.Printf("分区%d获取最新位移失败: %v", p, err)
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
	if req.BrokerList == "" {
		return nil, fmt.Errorf("Broker列表不能为空")
	}
	if req.TimeoutMS <= 0 {
		req.TimeoutMS = 5000
	}

	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("Broker列表格式错误，应为逗号分隔，如192.168.1.100:9092,192.168.1.101:9092")
	}

	config := sarama.NewConfig()
	config.Version = getKafkaVersion(req.KafkaVersion)
	config.Net.DialTimeout = time.Duration(req.TimeoutMS) * time.Millisecond
	config.Admin.Timeout = time.Duration(req.TimeoutMS) * time.Millisecond

	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		return nil, fmt.Errorf("创建Client失败: %v", err)
	}
	defer client.Close()

	adminClient, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("创建管理客户端失败: %v", err)
	}
	defer adminClient.Close()

	topicsMap, err := adminClient.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("查询所有主题失败: %v", err)
	}

	var topicNames []string
	for name := range topicsMap {
		if req.FilterInternal {
			if !isInternalTopic(name) {
				topicNames = append(topicNames, name)
			}
		} else {
			topicNames = append(topicNames, name)
		}
	}

	if len(topicNames) == 0 {
		if req.FilterInternal {
			return []TopicListItem{}, fmt.Errorf("未查询到用户创建的主题")
		} else {
			return []TopicListItem{}, fmt.Errorf("未查询到任何主题")
		}
	}

	topicMetas, err := adminClient.DescribeTopics(topicNames)
	if err != nil {
		return nil, fmt.Errorf("查询主题元数据失败: %v", err)
	}

	var topicList []TopicListItem
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	for _, meta := range topicMetas {
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

	return topicList, nil
}

// 适配新版sarama：ListConsumerGroups返回map[string]string（名称→类型）
func getAllConsumerGroups(c *gin.Context, req ListConsumerGroupsRequest) (interface{}, error) {
	if req.BrokerList == "" {
		return nil, fmt.Errorf("Broker列表不能为空")
	}
	if req.TimeoutMS <= 0 {
		req.TimeoutMS = 5000
	}

	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("Broker列表格式错误，应为逗号分隔，如192.168.1.100:9092,192.168.1.101:9092")
	}

	config := sarama.NewConfig()
	config.Version = getKafkaVersion(req.KafkaVersion)
	config.Net.DialTimeout = time.Duration(req.TimeoutMS) * time.Millisecond
	config.Admin.Timeout = time.Duration(req.TimeoutMS) * time.Millisecond

	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		return nil, fmt.Errorf("创建Client失败: %v", err)
	}
	defer client.Close()

	adminClient, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("创建管理客户端失败: %v", err)
	}
	defer adminClient.Close()

	// 新版sarama：ListConsumerGroups()返回map[string]string
	groups, err := adminClient.ListConsumerGroups()
	if err != nil {
		return nil, fmt.Errorf("查询消费者组列表失败: %v", err)
	}

	var groupList []ConsumerGroupListItem
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	for groupName, groupTypeStr := range groups {
		// 解析消费组类型
		groupType := getConsumerGroupTypeDesc(groupTypeStr)

		// 生成描述信息
		description := fmt.Sprintf("消费组类型: %s", groupType)
		if groupType == "unknown" {
			description = fmt.Sprintf("未知类型消费组 (原始类型值: %s)", groupTypeStr)
		}

		groupList = append(groupList, ConsumerGroupListItem{
			Name:        groupName,
			Type:        groupType,
			QueryTime:   currentTime,
			Description: description,
		})
	}

	if len(groupList) == 0 {
		return groupList, fmt.Errorf("未查询到任何消费者组")
	}

	return groupList, nil
}

// 新增：查询指定Topic关联的所有消费组及各分区消费状态
func getTopicConsumerGroups(c *gin.Context, req TopicConsumerGroupsRequest) (interface{}, error) {
	// 1. 参数校验
	if req.BrokerList == "" {
		return nil, fmt.Errorf("Broker列表不能为空")
	}
	if req.Topic == "" {
		return nil, fmt.Errorf("Topic名称不能为空")
	}
	if req.TimeoutMS <= 0 {
		req.TimeoutMS = 5000
	}

	// 2. 解析Broker列表
	brokerList := parseBrokerList(req.BrokerList)
	if len(brokerList) == 0 {
		return nil, fmt.Errorf("Broker列表格式错误，应为逗号分隔，如192.168.1.100:9092,192.168.1.101:9092")
	}

	// 3. 创建Kafka客户端和Admin客户端
	config := sarama.NewConfig()
	config.Version = getKafkaVersion(req.KafkaVersion)
	config.Net.DialTimeout = time.Duration(req.TimeoutMS) * time.Millisecond
	config.Admin.Timeout = time.Duration(req.TimeoutMS) * time.Millisecond

	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		return nil, fmt.Errorf("创建Client失败: %v", err)
	}
	defer client.Close()

	adminClient, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("创建管理客户端失败: %v", err)
	}
	defer adminClient.Close()

	// 4. 校验Topic是否存在
	topicMeta, err := adminClient.DescribeTopics([]string{req.Topic})
	if err != nil {
		return nil, fmt.Errorf("查询Topic元数据失败: %v", err)
	}
	if len(topicMeta) == 0 {
		return nil, fmt.Errorf("Topic %s 不存在", req.Topic)
	}

	// 5. 获取Topic的所有分区
	var partitionIDs []int32
	for _, p := range topicMeta[0].Partitions {
		partitionIDs = append(partitionIDs, p.ID)
	}
	if len(partitionIDs) == 0 {
		return nil, fmt.Errorf("Topic %s 无分区", req.Topic)
	}

	// 6. 查询所有消费组
	groups, err := adminClient.ListConsumerGroups()
	if err != nil {
		return nil, fmt.Errorf("查询所有消费组失败: %v", err)
	}
	if len(groups) == 0 {
		return []TopicConsumerGroupDetail{}, fmt.Errorf("未查询到任何消费组")
	}

	// 7. 遍历每个消费组，筛选出消费该Topic的组并查询消费状态
	var topicConsumerGroupList []TopicConsumerGroupDetail
	currentTime := time.Now().Format("2006-01-02 15:04:05")

	for groupName, groupTypeStr := range groups {
		// 查询该消费组在当前Topic的偏移量
		groupOffsets, err := adminClient.ListConsumerGroupOffsets(groupName, map[string][]int32{req.Topic: partitionIDs})
		if err != nil {
			log.Printf("查询消费组%s的偏移量失败: %v，跳过该消费组", groupName, err)
			continue
		}

		// 过滤：仅保留消费了当前Topic的消费组
		if _, hasTopic := groupOffsets.Blocks[req.Topic]; !hasTopic {
			continue
		}

		// 计算每个分区的高水位和Lag
		highWaterMap := make(map[int32]int64)
		for _, p := range partitionIDs {
			hw, err := getPartitionHighWater(brokerList, config.Version, req.TimeoutMS, req.Topic, p)
			if err != nil {
				log.Printf("分区%d获取高水位失败: %v", p, err)
				highWaterMap[p] = 0
			} else {
				highWaterMap[p] = hw
			}
		}

		// 整理该消费组在Topic各分区的消费状态
		var consumerStats []ConsumerStatus
		for _, partition := range partitionIDs {
			offset := int64(-1)
			if block, exists := groupOffsets.Blocks[req.Topic]; exists {
				if meta, exists := block[partition]; exists {
					offset = meta.Offset
				}
			}

			// 计算Lag（消费滞后量）
			lag := int64(0)
			if offset == -1 {
				lag = highWaterMap[partition] + 1 // 未消费过，Lag为总消息数
			} else {
				lag = highWaterMap[partition] - offset
			}

			consumerStats = append(consumerStats, ConsumerStatus{
				GroupName:  groupName,
				Topic:      req.Topic,
				Partition:  partition,
				Offset:     offset,
				HighWater:  highWaterMap[partition],
				Lag:        lag,
				LastUpdate: currentTime,
			})
		}

		// 添加到结果列表
		topicConsumerGroupList = append(topicConsumerGroupList, TopicConsumerGroupDetail{
			GroupName:     groupName,
			GroupType:     getConsumerGroupTypeDesc(groupTypeStr),
			ConsumerStats: consumerStats,
		})
	}

	// 8. 结果处理
	if len(topicConsumerGroupList) == 0 {
		return []TopicConsumerGroupDetail{}, fmt.Errorf("未查询到消费Topic %s的消费组", req.Topic)
	}

	return topicConsumerGroupList, nil
}

// ------------------------------
// HTML模板
// ------------------------------
var indexTemplate = `
<!DOCTYPE html>
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
            max-width: 1200px;
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
        .result {
            margin-top: 20px;
            padding: 15px;
            background-color: #f8f8f8;
            border-radius: 4px;
            white-space: pre-wrap;
            font-family: monospace;
            font-size: 14px;
            max-height: 400px;
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
            padding: 8px;
            margin: 5px 0;
            border: 1px solid #eee;
            border-radius: 4px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .group-item:hover {
            background-color: #f5f5f5;
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
            <!-- 修改：新增"查看主题关联消费组"标签页 -->
            <button class="tab" onclick="switchTab('topic-consumer-groups')">查看主题关联消费组</button>
            <button class="tab active" onclick="switchTab('produce')">生产消息</button>
            <button class="tab" onclick="switchTab('query-producer')">查询生产者消息</button>
            <button class="tab" onclick="switchTab('consume')">消费消息</button>
            <button class="tab" onclick="switchTab('group-status')">查询消费组状态</button>
        </div>

        <!-- 创建主题表单 -->
        <div id="create-topic" class="tab-content">
            <form id="create-topic-form">
                <div class="form-group">
                    <label>Kafka Broker列表（逗号分隔）</label>
                    <input type="text" name="broker_list" value="192.168.1.100:9092" required>
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
                    <input type="text" name="broker_list" value="192.168.1.100:9092" required>
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
            <div id="list-topics-result" class="result"></div>
        </div>

        <!-- 查看所有消费者组表单 -->
        <div id="list-consumer-groups" class="tab-content">
            <form id="list-consumer-groups-form">
                <div class="form-group">
                    <label>Kafka Broker列表（逗号分隔）</label>
                    <input type="text" name="broker_list" value="192.168.1.100:9092" required>
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
            <div id="list-consumer-groups-result" class="result"></div>
        </div>

        <!-- 新增：查看主题关联消费组表单 -->
        <div id="topic-consumer-groups" class="tab-content">
            <form id="topic-consumer-groups-form">
                <div class="form-group">
                    <label>Kafka Broker列表（逗号分隔）</label>
                    <input type="text" name="broker_list" value="192.168.1.100:9092" required>
                </div>
                <div class="form-group">
                    <label>Topic名称</label>
                    <input type="text" name="topic" value="test_topic" required>
                    <div class="hint">输入要查询的Topic名称，支持用户创建的普通主题</div>
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
                <button type="submit">查询主题关联消费组</button>
            </form>
            <div id="topic-consumer-groups-result" class="result"></div>
        </div>

        <!-- 生产消息表单 -->
        <div id="produce" class="tab-content active">
            <form id="produce-form">
                <div class="form-group">
                    <label>Kafka Broker列表（逗号分隔）</label>
                    <input type="text" name="broker_list" value="192.168.1.100:9092" required>
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
                    <input type="text" name="broker_list" value="192.168.1.100:9092" required>
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

        <!-- 消费消息表单 -->
        <div id="consume" class="tab-content">
            <form id="consume-form">
                <div class="form-group">
                    <label>Kafka Broker列表（逗号分隔）</label>
                    <input type="text" name="broker_list" value="192.168.1.100:9092" required>
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
                </div>
                <div class="form-group">
                    <label>最大消费条数</label>
                    <input type="number" name="max_num" value="10" min="1">
                </div>
                <button type="submit">消费消息</button>
            </form>
            <div id="consume-result" class="result"></div>
        </div>

        <!-- 查询消费组状态表单 -->
        <div id="group-status" class="tab-content">
            <form id="group-status-form">
                <div class="form-group">
                    <label>Kafka Broker列表（逗号分隔）</label>
                    <input type="text" name="broker_list" value="192.168.1.100:9092" required>
                </div>
                <div class="form-group">
                    <label>Topic名称</label>
                    <input type="text" name="topic" value="test_topic" required>
                </div>
                <div class="form-group">
                    <label>消费组名称</label>
                    <input type="text" name="group_name" id="group_name_input" value="my_consumer_group" required>
                    <button type="button" class="copy-btn" onclick="pasteGroupName()">粘贴消费组名称</button>
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
        
        function switchTab(tabId) {
            document.querySelectorAll('.tab-content').forEach(function(el) {
                el.classList.remove('active');
            });
            document.querySelectorAll('.tab').forEach(function(el) {
                el.classList.remove('active');
            });
            document.getElementById(tabId).classList.add('active');
            document.querySelector('.tab[onclick="switchTab(\'' + tabId + '\')"]').classList.add('active');
        }

        document.getElementById('create-topic-form').addEventListener('submit', function(e) {
            e.preventDefault();
            const formData = new FormData(this);
            const resultEl = document.getElementById('create-topic-result');
            resultEl.innerHTML = '处理中...';
            
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
            resultEl.innerHTML = '处理中...';
            
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
            resultEl.innerHTML = '处理中...';
            
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
                if (data.code === 0 && data.data && data.data.length > 0) {
                    lastConsumerGroups = data.data;
                    let html = '<div>';
                    data.data.forEach(function(group) {
                        html += '<div class="group-item">' +
                            '<span><strong>' + group.name + '</strong> (类型: ' + group.type + ') - ' + group.description + '</span>' +
                            '<button class="copy-btn" onclick="copyGroupName(\'' + group.name + '\')">复制名称</button>' +
                            '</div>';
                    });
                    html += '</div><pre>' + JSON.stringify(data, null, 2) + '</pre>';
                    resultEl.innerHTML = html;
                } else {
                    resultEl.innerHTML = JSON.stringify(data, null, 2);
                }
            })
            .catch(function(err) {
                resultEl.className = 'result error';
                resultEl.innerHTML = '请求失败: ' + err.message;
            });
        }

        // 新增：处理主题关联消费组查询的表单提交
        document.getElementById('topic-consumer-groups-form').addEventListener('submit', function(e) {
            e.preventDefault();
            const formData = new FormData(this);
            const resultEl = document.getElementById('topic-consumer-groups-result');
            resultEl.innerHTML = '处理中...';
            
            fetch('/api/topic-consumer-groups', {
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

        document.getElementById('produce-form').addEventListener('submit', function(e) {
            e.preventDefault();
            const formData = new FormData(this);
            const resultEl = document.getElementById('produce-result');
            resultEl.innerHTML = '处理中...';
            
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
            resultEl.innerHTML = '处理中...';
            
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
            resultEl.innerHTML = '处理中...';
            
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

        document.getElementById('group-status-form').addEventListener('submit', function(e) {
            e.preventDefault();
            const formData = new FormData(this);
            const resultEl = document.getElementById('group-status-result');
            resultEl.innerHTML = '处理中...';
            
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
`

// ------------------------------
// HTTP处理器（路由绑定）
// ------------------------------
// 统一响应处理函数
func response(c *gin.Context, code int, message string, data interface{}) {
	c.JSON(http.StatusOK, Response{
		Code:    code,
		Message: message,
		Data:    data,
	})
}

// 创建Topic处理器
func createTopicHandler(c *gin.Context) {
	var req CreateTopicRequest
	if err := c.ShouldBind(&req); err != nil {
		response(c, 1, fmt.Sprintf("参数解析失败: %v", err), nil)
		return
	}

	data, err := createTopic(c, req)
	if err != nil {
		response(c, 1, err.Error(), nil)
		return
	}
	response(c, 0, "创建Topic成功", data)
}

// 生产消息处理器
func produceMessageHandler(c *gin.Context) {
	var req ProducerRequest
	if err := c.ShouldBind(&req); err != nil {
		response(c, 1, fmt.Sprintf("参数解析失败: %v", err), nil)
		return
	}

	data, err := produceMessage(c, req)
	if err != nil {
		response(c, 1, err.Error(), nil)
		return
	}
	response(c, 0, "发送消息成功", data)
}

// 消费消息处理器
func consumeMessageHandler(c *gin.Context) {
	var req ConsumerRequest
	if err := c.ShouldBind(&req); err != nil {
		response(c, 1, fmt.Sprintf("参数解析失败: %v", err), nil)
		return
	}

	data, err := consumeMessage(c, req)
	if err != nil {
		response(c, 1, err.Error(), nil)
		return
	}
	response(c, 0, "消费消息成功", data)
}

// 查询生产者消息处理器
func queryProducerMessagesHandler(c *gin.Context) {
	var req ProducerMsgQueryRequest
	if err := c.ShouldBind(&req); err != nil {
		response(c, 1, fmt.Sprintf("参数解析失败: %v", err), nil)
		return
	}

	data, err := queryProducerMessages(c, req)
	if err != nil {
		response(c, 1, err.Error(), nil)
		return
	}
	response(c, 0, "查询消息成功", data)
}

// 查询消费组状态处理器
func getGroupStatusHandler(c *gin.Context) {
	var req GroupStatusRequest
	if err := c.ShouldBind(&req); err != nil {
		response(c, 1, fmt.Sprintf("参数解析失败: %v", err), nil)
		return
	}

	data, err := getGroupStatus(c, req)
	if err != nil {
		response(c, 1, err.Error(), nil)
		return
	}
	response(c, 0, "查询消费组状态成功", data)
}

// 查看所有主题处理器
func getAllTopicsHandler(c *gin.Context) {
	var req ListTopicsRequest
	if err := c.ShouldBind(&req); err != nil {
		response(c, 1, fmt.Sprintf("参数解析失败: %v", err), nil)
		return
	}

	data, err := getAllTopics(c, req)
	if err != nil {
		response(c, 1, err.Error(), nil)
		return
	}
	response(c, 0, "查询主题列表成功", data)
}

// 查看所有消费者组处理器
func getAllConsumerGroupsHandler(c *gin.Context) {
	var req ListConsumerGroupsRequest
	if err := c.ShouldBind(&req); err != nil {
		response(c, 1, fmt.Sprintf("参数解析失败: %v", err), nil)
		return
	}

	data, err := getAllConsumerGroups(c, req)
	if err != nil {
		response(c, 1, err.Error(), nil)
		return
	}
	response(c, 0, "查询消费组列表成功", data)
}

// 查看主题关联消费组处理器
func getTopicConsumerGroupsHandler(c *gin.Context) {
	var req TopicConsumerGroupsRequest
	if err := c.ShouldBind(&req); err != nil {
		response(c, 1, fmt.Sprintf("参数解析失败: %v", err), nil)
		return
	}

	data, err := getTopicConsumerGroups(c, req)
	if err != nil {
		response(c, 1, err.Error(), nil)
		return
	}
	response(c, 0, "查询主题关联消费组成功", data)
}

// 首页处理器（渲染HTML模板）
func indexHandler(c *gin.Context) {
	tmpl, err := template.New("index").Parse(indexTemplate)
	if err != nil {
		c.String(http.StatusInternalServerError, fmt.Sprintf("模板解析失败: %v", err))
		return
	}
	tmpl.Execute(c.Writer, nil)
}

// ------------------------------
// 程序入口
// ------------------------------
func main() {
	// 解析命令行参数（端口）
	port := flag.Int("port", 8080, "服务监听端口")
	flag.Parse()

	// 设置Gin模式（生产环境可改为ReleaseMode）
	gin.SetMode(gin.DebugMode)

	// 初始化Gin引擎
	r := gin.Default()

	// 首页路由
	r.GET("/", indexHandler)

	// API路由组
	api := r.Group("/api")
	{
		api.POST("/create-topic", createTopicHandler)                     // 创建Topic
		api.POST("/produce", produceMessageHandler)                       // 生产消息
		api.POST("/consume", consumeMessageHandler)                       // 消费消息
		api.POST("/query-producer", queryProducerMessagesHandler)         // 查询生产者消息
		api.POST("/group-status", getGroupStatusHandler)                  // 查询消费组状态
		api.POST("/list-topics", getAllTopicsHandler)                     // 查看所有主题
		api.POST("/list-consumer-groups", getAllConsumerGroupsHandler)    // 查看所有消费组
		api.POST("/topic-consumer-groups", getTopicConsumerGroupsHandler) // 查看主题关联消费组
	}

	// 启动HTTP服务
	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Kafka Web工具已启动，监听地址: http://localhost%s", addr)
	if err := r.Run(addr); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
