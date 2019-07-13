using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Redis.Model
{
  public  class MessageResult
    {
        public string JobId { get; set; }
        public string Topic { get; set; }

        public byte[] Data { get; set; }
    }
}
