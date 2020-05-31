using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Kafka.Model
{
   public class KafkaMessageBusData
    {
        public string Topic { get; set; }
        public byte[] Data { get; set; }

        /// <summary>
        /// 执行时间  （延迟队列有用），即时任务就是当前时间戳 
        /// </summary>
        public long ExecuteTimeStamp { get; set; }

        public int ErrorCount { get; set; }
    }
}
