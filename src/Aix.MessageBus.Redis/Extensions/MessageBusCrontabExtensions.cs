using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.Redis
{
    public static class MessageBusCrontabExtensions
    {
        public static Task PublishDelayAsync<T>(this IMessageBus messageBus, Type messageType, object message, TimeSpan delay)
        {
            return (messageBus as RedisMessageBus).PublishDelayAsync(typeof(T), message, delay);
        }

        public static Task PublishCrontabAsync<T>(this IMessageBus messageBus, T message, CrontabJobInfo crontabJobInfo)
        {
            return (messageBus as RedisMessageBus).PublishCrontabAsync(typeof(T), message, crontabJobInfo);
        }
    }

    public class CrontabJobInfo
    {
        /// <summary>
        /// 定时任务标识  不能重复
        /// </summary>
        public string JobId { get; set; }

        /// <summary>
        /// 定时任务名称
        /// </summary>
        public string JobName { get; set; }

        /// <summary>
        /// 定时表达式
        /// </summary>
        public string CrontabExpression { get; set; }
    }
}
