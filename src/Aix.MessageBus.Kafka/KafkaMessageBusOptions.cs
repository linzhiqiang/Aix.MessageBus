using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Kafka
{
    public class KafkaMessageBusOptions
    {
        private int[] DefaultRetryStrategy = new int[] { 1, 5, 10, 30, 60, 60, 2 * 60, 2 * 60, 5 * 60, 5 * 60 };
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

            //this.MaxErrorReTryCount = 10;
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

        /*
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

        /// <summary>
        /// 最大错误重试次数 默认10次
        /// </summary>
        public int MaxErrorReTryCount { get; set; }

        private int[] _retryStrategy;
        /// <summary>
        /// 失败重试延迟策略 单位：秒  默认失败次数对应值延迟时间[ 1,5, 10, 30,  60,  60, 2 * 60, 2 * 60, 5 * 60, 5 * 60  ];
        /// </summary>
        public int[] RetryStrategy
        {
            get
            {
                if (_retryStrategy == null || _retryStrategy.Length == 0) _retryStrategy = DefaultRetryStrategy;
                return _retryStrategy;
            }
            set { _retryStrategy = value; }
        }
        */
    }
}
