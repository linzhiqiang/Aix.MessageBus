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
        //private int[] DefaultRetryStrategy = new int[] { 1, 5, 10, 30, 60, 2*60, 2 * 60, 2 * 60, 5 * 60, 5 * 60 };
        private int[] DefaultRetryStrategy = new int[] { 0, 5, 30, 60, 2 * 60 };

        /// <summary>
        /// 默认延迟队列 延迟时间配置  （秒，延迟队列名称后缀）
        /// </summary>
        private static Dictionary<int, string> DefaultDelayQueueConfig = new Dictionary<int, string>
        {
            { (int)TimeSpan.FromSeconds(5).TotalSeconds,"5s"},
            { (int)TimeSpan.FromSeconds(30).TotalSeconds,"30s"},
            { (int)TimeSpan.FromMinutes(1).TotalSeconds,"1m"},
            { (int)TimeSpan.FromMinutes(2).TotalSeconds,"2m"},
            //{ (int)TimeSpan.FromMinutes(30).TotalSeconds,"30m"},
            //{ (int)TimeSpan.FromHours(1).TotalSeconds,"1h"},
            //{ (int)TimeSpan.FromDays(1).TotalSeconds,"1d"},
        };
        public KafkaMessageBusOptions()
        {
            //this.TopicPrefix = "kafka-messagebus-";
            this.Serializer = new MessagePackSerializer();
            this.DefaultConsumerThreadCount = 4;
            this.ManualCommitBatch = 100;
            this.ManualCommitIntervalSecond = 0;

            this.DelayConsumerThreadCount = 1;
            this.MaxErrorReTryCount = 10;
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

        public int DelayConsumerThreadCount { get; set; }

        public Dictionary<int, string> DelayQueueConfig { get; set; }

        public Dictionary<int, string> GetDelayQueueConfig()
        {
            if (DelayQueueConfig == null || DelayQueueConfig.Count == 0) return DefaultDelayQueueConfig;
            return DelayQueueConfig;
        }

        /// <summary>
        /// 最大错误重试次数 默认5次
        /// </summary>
        public int MaxErrorReTryCount { get; set; }

        /// <summary>
        /// 失败重试延迟策略 单位：秒  默认失败次数对应值延迟时间[ 1,5, 10, 30,  60,  60, 2 * 60, 2 * 60, 5 * 60, 5 * 60  ];
        /// </summary>
        public int[] RetryStrategy { get; set; }

        public int[] GetRetryStrategy()
        {
            if (RetryStrategy == null || RetryStrategy.Length == 0) return DefaultRetryStrategy;
            return RetryStrategy;
        }

    }
}
