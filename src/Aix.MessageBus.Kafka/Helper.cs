using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace Aix.MessageBus.Kafka
{
  public  class Helper
    {
        public static string GetDelayTopic(KafkaMessageBusOptions options, TimeSpan delay)
        {
            var dealySecond = (int)delay.TotalSeconds;

            var keys = options.DelayQueueConfig.Keys.ToList();

            //for (int i = 0; i < keys.Count; i++)
            for (int i = keys.Count - 1; i >= 0; i--)
            {
                if (dealySecond > keys[i])
                {
                    return GetDelayTopic(options, options.DelayQueueConfig[keys[i]]);
                }
            }

            return GetDelayTopic(options, options.DelayQueueConfig[keys[0]]);

        }

        public static string GetDelayTopic(KafkaMessageBusOptions options, string postfix)
        {
            return $"{options.TopicPrefix }delay-topic{postfix}";
        }
    }
}
