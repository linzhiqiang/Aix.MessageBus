using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Kafka
{
    [Flags]
    public enum ClientMode
    {
        Producer = 1,
        Consumer = 2,
        Both = 3
    }

    public class KafkaMessageBusOptions
    {
        public KafkaMessageBusOptions()
        {
           // this.TopicPrefix = "kafka-";
            this.ClientMode = ClientMode.Producer;
            this.Serializer = new MessagePackSerializer();
            this.ConsumerThreadCount = 4;
            this.ManualCommitBatch = 10;
        }

        public ProducerConfig ProducerConfig { get; set; }

        public ConsumerConfig ConsumerConfig { get; set; }

        public string BootstrapServers { get; set; }

        /// <summary>
        /// topic前缀，为了防止重复，建议用项目名称
        /// </summary>
        public string TopicPrefix { get; set; }

        /// <summary>
        /// 客户端模式，是生产者还是消费者
        /// </summary>
        public ClientMode ClientMode { get; set; }

        /// <summary>
        /// 自定义序列化，默认为MessagePack
        /// </summary>
        public ISerializer Serializer { get; set; }

        /// <summary>
        /// 默认每个类型的消费线程数 默认4个
        /// </summary>
        public int ConsumerThreadCount { get; set; }

        /// <summary>
        /// EnableAutoCommit=false时每多少个消息提交一次 默认10条消息提交一次
        /// </summary>
        public int ManualCommitBatch { get; set; }
    }
}
