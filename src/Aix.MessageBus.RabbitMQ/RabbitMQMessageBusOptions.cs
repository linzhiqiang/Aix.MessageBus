using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.RabbitMQ
{
    public class RabbitMQMessageBusOptions
    {
        /// <summary>
        /// 默认延迟队列 延迟时间配置  （秒，延迟队列名称后缀）
        /// </summary>
        private static Dictionary<int, string> DefaultDelayQueueConfig = new Dictionary<int, string>
        {
            { (int)TimeSpan.FromSeconds(5).TotalSeconds,"5s"},
            { (int)TimeSpan.FromSeconds(30).TotalSeconds,"30s"},
            { (int)TimeSpan.FromMinutes(1).TotalSeconds,"1m"},
            { (int)TimeSpan.FromHours(1).TotalSeconds,"1h"},
            { (int)TimeSpan.FromDays(1).TotalSeconds,"1d"},
        };
        public RabbitMQMessageBusOptions()
        {
            Port = 5672;
            VirtualHost = "/";

            this.TopicPrefix = "rabbitmq-";
            this.Serializer = new MessagePackSerializer();
            this.ConfirmSelect = false;
            this.DefaultConsumerThreadCount = 4;
            this.AutoAck = true;
            this.ManualCommitBatch = 10;


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
        /// 发布消息时是否确认消息收到确认 false=不确认 true=确认，默认false
        /// </summary>
        public bool ConfirmSelect { get; set; }

        /// <summary>
        /// 默认每个类型的消费线程数 默认4个
        /// </summary>
        public int DefaultConsumerThreadCount { get; set; }

        /// <summary>
        /// 消费时是否自动确认
        /// </summary>
        public bool AutoAck { get; set; }

        /// <summary>
        /// AutoAck=false时每多少个消息提交一次 默认10条消息提交一次
        /// </summary>
        public ushort ManualCommitBatch { get; set; }

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
