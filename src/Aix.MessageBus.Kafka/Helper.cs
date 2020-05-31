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
            string topicName = type.Name;

            var topicAttr = TopicAttribute.GetTopicAttribute(type);
            if (topicAttr != null && !string.IsNullOrEmpty(topicAttr.Name))
            {
                topicName = topicAttr.Name;
            }
            
            return $"{options.TopicPrefix ?? ""}{topicName}";
        }

        public static string GetDelayTopic(KafkaMessageBusOptions options, TimeSpan delay)
        {
            var dealySecond = (int)delay.TotalSeconds;

            var keys = options.GetDelayQueueConfig().Keys.ToList();

            //for (int i = 0; i < keys.Count; i++)
            for (int i = keys.Count - 1; i >= 0; i--)
            {
                if (dealySecond > keys[i])
                {
                    return GetDelayTopic(options, options.GetDelayQueueConfig()[keys[i]]);
                }
            }

            return GetDelayTopic(options, options.GetDelayQueueConfig()[keys[0]]);

        }

        public static string GetDelayTopic(KafkaMessageBusOptions options, string postfix)
        {
            return $"{options.TopicPrefix }delay-topic{postfix}";
        }

    }
}
