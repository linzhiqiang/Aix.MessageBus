using System;
using System.Collections.Generic;
using System.Text;

namespace Sample.kafka
{
    public class KafkaMessage
    {
        public string MessageId { get; set; }
        public string Content { get; set; }

        public DateTime CreateTime { get; set; }
    }

    public class KafkaMessage2
    {
        public string MessageId { get; set; }
        public string Content { get; set; }

        public DateTime CreateTime { get; set; }
    }
}
