using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Redis
{
    public class RedisMessageBusOptions
    {
        public RedisMessageBusOptions()
        {
            this.TopicPrefix = "redis:";
            this.Serializer = new MessagePackSerializer();
            this.DefaultConsumerThreadCount = 4;
            this.ConsumerMode = ConsumerMode.AtLeastOnce;
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
    }
}
