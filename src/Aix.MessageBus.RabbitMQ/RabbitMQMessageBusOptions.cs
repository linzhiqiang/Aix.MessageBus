using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.RabbitMQ
{
    public class RabbitMQMessageBusOptions
    {
        public RabbitMQMessageBusOptions()
        {
            Port = 5672;
            VirtualHost = "/";

            this.TopicPrefix = "rabbitmq-";
            this.Serializer = new MessagePackSerializer();
            this.DefaultConsumerThreadCount = 4;
            this.ConsumerMode = ConsumerMode.AtLeastOnce;
        }
        public string HostName { get; set; }

        public int Port { get; set; }

        public string VirtualHost { get; set; }

        public string UserName { get; set; }

        public string Password { get; set; }

        /***********************************************************/

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
