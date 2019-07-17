using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Redis.Model
{
    public class RedisMessageBusData
    {
        public string Type { get; set; }
        public byte[] Data { get; set; }
    }
}
