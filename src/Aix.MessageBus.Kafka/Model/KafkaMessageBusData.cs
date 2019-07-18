using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Kafka.Model
{
   public class KafkaMessageBusData
    {
        public string Topic { get; set; }
        public byte[] Data { get; set; }
    }
}
