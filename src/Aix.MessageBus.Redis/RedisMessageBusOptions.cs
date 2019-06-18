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
            this.TopicPrefix = "kafka-";
            this.Serializer = new MessagePackSerializer();
        }

        /// <summary>
        /// RedisConnectionString和ConnectionMultiplexer传一个即可
        /// </summary>
        public string RedisConnectionString { get; set; }

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
    }
}
