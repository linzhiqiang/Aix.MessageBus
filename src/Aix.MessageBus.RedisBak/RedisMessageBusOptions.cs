using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Redis
{
    public class RedisMessageBusOptions
    {
        private int[] DefaultRetryStrategy = new int[] { 10, 60, 2 * 60, 5 * 60, 10 * 60, 20 * 60, 30 * 60 };

        public RedisMessageBusOptions()
        {
            this.TopicPrefix = "redis:";
            this.Serializer = new MessagePackSerializer();
            this.DefaultConsumerThreadCount = 4;
            this.ConsumerMode = ConsumerMode.AtLeastOnce;
            this.NoAckReEnqueueDelay = 30;
            this.DataExpireDay = 7;
            this.RetryMaxCount = 5;
            this.RetryStrategy = DefaultRetryStrategy;
        }

        /// <summary>
        /// RedisConnectionString和ConnectionMultiplexer传一个即可
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        ///  RedisConnectionString和ConnectionMultiplexer传一个即可
        /// </summary>
        public ConnectionMultiplexer ConnectionMultiplexer { get; set; }

        /// <summary>
        /// topic前缀，为了防止重复，建议用项目名称
        /// </summary>
        public string TopicPrefix { get; set; }

        /// <summary>
        /// 自定义序列化，默认为MessagePack
        /// </summary>
        public ISerializer Serializer { get; set; }

        /// <summary>
        /// 默认每个类型的消费线程数 默认4个
        /// </summary>
        public int DefaultConsumerThreadCount { get; set; }

        /// <summary>
        /// 消费模式 语义 默认至少一次
        /// </summary>
        public ConsumerMode ConsumerMode { get; set; }

        /// <summary>
        /// 没有确认重新入队的超时时间 单位：秒，默认30秒
        /// </summary>
        public int NoAckReEnqueueDelay { get; set; }

        /// <summary>
        /// 任务数据有效期 默认7天 单位  天
        /// </summary>
        public int DataExpireDay { get; set; }

        /// <summary>
        /// 失败重试最大次数 0不重试，默认：5,需要重试请抛出RetryException异常
        /// </summary>
        public int RetryMaxCount { get; set; }

        private int[] _retryStrategy;
        /// <summary>
        /// 失败重试延迟策略 单位：秒 （第二次以后要大于等于60秒）  默认失败次数对应值延迟时间[ 10, 60, 2 * 60, 5 * 60, 10 * 60, 20 * 60, 30 * 60 ];
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
    }
}
