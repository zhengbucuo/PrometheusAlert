package controllers

import (
	"strings"
	"github.com/IBM/sarama"
	"github.com/astaxie/beego"
	"time"
)

func GetKafkaProducer() (sarama.SyncProducer, error) {
	// 获取 Kafka 服务器地址并正确解析
	kafka_server_str := beego.AppConfig.String("kafka_server")
	var kafka_server []string
	
	// 处理多种分隔符情况
	if strings.Contains(kafka_server_str, ",") {
		kafka_server = strings.Split(kafka_server_str, ",")
	} else {
		kafka_server = []string{kafka_server_str}
	}
	
	// 清理空格
	for i, addr := range kafka_server {
		kafka_server[i] = strings.TrimSpace(addr)
	}
	
	config := sarama.NewConfig()
	
	// 基本生产者配置
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewHashPartitioner
	
	// 检查是否启用认证
	kafka_auth := beego.AppConfig.String("kafka_auth")
	if kafka_auth == "1" {
		// Kafka 认证配置
		config.Net.SASL.Enable = true
		
		// 从配置文件读取 SASL 机制
		sasl_mechanism := beego.AppConfig.String("kafka_sasl_mechanism")
		config.Net.SASL.Mechanism = sarama.SASLMechanism(sasl_mechanism)
		
		config.Net.SASL.User = beego.AppConfig.String("kafka_username")
		config.Net.SASL.Password = beego.AppConfig.String("kafka_password")
		
		// TLS 配置（如果需要）
		kafka_tls := beego.AppConfig.String("kafka_tls")
		if kafka_tls == "1" {
			config.Net.TLS.Enable = true
		}
	}

	// 创建生产者
	producer, err := sarama.NewSyncProducer(kafka_server, config)
	return producer, err
}

func SendKafka(message, logsign string) string {
	// 发送kafka
	open := beego.AppConfig.String("open-kafka")
	if open != "1" {
		beego.Info(logsign, "[kafka]", "kafka未配置未开启状态,请先配置open-kafka为1")
		return "kafka未配置未开启状态,请先配置open-kafka为1"
	}
	
	t1 := time.Now().UnixMilli()
	
	// 获取带认证的生产者
	producer, err := GetKafkaProducer()
	if err != nil {
		beego.Error(logsign, "[kafka] 创建生产者失败:", err.Error())
		return "创建kafka生产者失败: " + err.Error()
	}
	
	defer func() {
		if producer != nil {
			producer.Close()
		}
	}()
	
	kafka_topic := beego.AppConfig.String("kafka_topic")
	Key_string := beego.AppConfig.String("kafka_key") + "-" + logsign
	
	msg := &sarama.ProducerMessage{
		Topic: kafka_topic,
		Value: sarama.StringEncoder(message),
		Key:   sarama.StringEncoder(Key_string),
	}

	partition, offset, err := producer.SendMessage(msg)
	t2 := time.Now().UnixMilli()
	
	if err == nil {
		beego.Debug("发送kafka消息:", Key_string, "成功, partition:", partition, ",offset:", offset, ",cost:", t2-t1, " ms")
		return "发送kafka消息:" + Key_string + "成功"
	} else {
		beego.Error(logsign, "[kafka] 发送消息失败:", err)
		return "发送kafka消息失败: " + err.Error()
	}
}
