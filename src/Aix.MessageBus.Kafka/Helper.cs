using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Aix.MessageBus.Kafka
{
    internal static class Helper
    {
        public static string GetTopic(KafkaMessageBusOptions options, Type type)
        {
            return $"{options.TopicPrefix ?? ""}{type.Name}";
        }
    }
}
