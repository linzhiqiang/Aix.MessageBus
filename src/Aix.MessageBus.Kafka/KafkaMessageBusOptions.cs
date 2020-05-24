using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Kafka
{
    /// <summary>
    /// kafka配置
    /// </summary>
    public class KafkaMessageBusOptions
    {
        public KafkaMessageBusOptions()
        {
            //this.TopicPrefix = "kafka-messagebus-";
            this.Serializer = new MessagePackSerializer();
            this.DefaultConsumerThreadCount = 4;
            this.ManualCommitBatch = 100;
            this.ManualCommitIntervalSecond = 0;

        }

        /// <summary>
        /// 生产者配置
        /// </summary>
        public ProducerConfig ProducerConfig { get; set; }

        /// <summary>
        /// 消费者配置
        /// </summary>
        public ConsumerConfig ConsumerConfig { get; set; }

        /// <summary>
        /// kafka集群地址 多个时逗号分隔
        /// </summary>
        public string BootstrapServers { get; set; }

        /// <summary>
        /// topic前缀，为了防止重复，建议用项目名称
        /// </summary>
        public string TopicPrefix { get; set; }

        /// <summary>
        /// 自定义序列化，默认为MessagePack
        /// </summary>
        public ISerializer Serializer { get; set; }

        /// <summary>
        /// 默认每个Topic的消费线程数 默认4个,请注意与分区数的关系
        /// </summary>
        public int DefaultConsumerThreadCount { get; set; }

        /// <summary>
        /// EnableAutoCommit=false时每多少个消息提交一次 默认100条消息提交一次，重要业务建议手工提交
        /// </summary>
        public int ManualCommitBatch { get; set; }

        /// <summary>
        /// EnableAutoCommit=false时 每多少秒提交一次 默认0秒不开启  ManualCommitBatch和ManualCommitIntervalSecond是或的关系
        /// </summary>
        public int ManualCommitIntervalSecond { get; set; }

    }
}
