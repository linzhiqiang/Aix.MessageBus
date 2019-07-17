using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Kafka
{
    public class KafkaMessageBusOptions
    {
        /// <summary>
        /// 默认延迟队列 延迟时间配置  （秒，延迟队列名称后缀）
        /// </summary>
        private static Dictionary<int, string> DefaultDelayQueueConfig = new Dictionary<int, string>
        {
            { (int)TimeSpan.FromSeconds(5).TotalSeconds,"5s"},
            { (int)TimeSpan.FromSeconds(30).TotalSeconds,"30s"},
            { (int)TimeSpan.FromMinutes(1).TotalSeconds,"1m"},
            { (int)TimeSpan.FromMinutes(30).TotalSeconds,"30m"},
            { (int)TimeSpan.FromHours(1).TotalSeconds,"1h"},
            { (int)TimeSpan.FromDays(1).TotalSeconds,"1d"},
        };
        public KafkaMessageBusOptions()
        {
            this.TopicPrefix = "kafka-messagebus-";
            this.Serializer = new MessagePackSerializer();
            this.DefaultConsumerThreadCount = 4;
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
        /// 默认的groupid
        /// </summary>
        public string DefaultConsumerGroupId { get; set; }

        /// <summary>
        /// 自定义序列化，默认为MessagePack
        /// </summary>
        public ISerializer Serializer { get; set; }

        /// <summary>
        /// 默认每个类型的消费线程数 默认4个
        /// </summary>
        public int DefaultConsumerThreadCount { get; set; }

        /// <summary>
        /// EnableAutoCommit=false时每多少个消息提交一次 默认10条消息提交一次
        /// </summary>
        public int ManualCommitBatch { get; set; }

        private Dictionary<int, string> _delayQueueConfig;
        public Dictionary<int, string> DelayQueueConfig
        {
            get
            {
                if (_delayQueueConfig == null || _delayQueueConfig.Count == 0) _delayQueueConfig = DefaultDelayQueueConfig;
                return _delayQueueConfig;
            }
            set { _delayQueueConfig = value; }
        }
    }
}
