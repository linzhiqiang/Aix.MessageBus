using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Redis
{
    internal static class Helper
    {
        public static string GetProcessingQueueName(string queue)
        {
            return $"{queue}:processing";
        }
        public static string GetJobHashId(RedisMessageBusOptions options, string jobId)
        {
            return $"{options.TopicPrefix ?? ""}jobdata:{jobId}";
        }

        public static string GetDelaySortedSetName(RedisMessageBusOptions options)
        {
            return $"{options.TopicPrefix ?? ""}delay:jobid";
        }
    }
}
