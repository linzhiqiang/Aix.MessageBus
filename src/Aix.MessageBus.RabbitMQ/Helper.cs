using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.RabbitMQ
{
   internal static class Helper
    {
        public static string GeteExchangeName(string topic)
        {
            return $"{topic}-exchange"; 
        }
        public static string GeteQueueName(string topic,string groupId)
        {
            if (string.IsNullOrEmpty(groupId))
            {
                return $"{topic}-queue";
            }
            else
            {
                return $"{topic}-{groupId}-queue";
            }
        }

        public static string GeteRoutingKey(string topic)
        {
            return $"{topic}-routingkey";
        }
    }
}
