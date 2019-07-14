using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.RabbitMQ.Model
{
  public  class DelayMessage
    {
        /// <summary>
        /// 类型对应的topic (交换器的名字)
        /// </summary>
        public string topic { get; set; }

        /// <summary>
        /// 原始消息
        /// </summary>
        public byte[] Data { get; set; }

        public long ExecuteTimeStamp { get; set; }
    }
}
