using Aix.MessageBus.Redis.Model;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.Redis
{
    public class RedisMessageBus : IMessageBus
    {
        private ILogger<RedisMessageBus> _logger;
        private RedisMessageBusOptions _options;
        ConnectionMultiplexer _connectionMultiplexer;

        ISubscriber _subscriber;
        IDatabase _database;
        public RedisMessageBus(ILogger<RedisMessageBus> logger, RedisMessageBusOptions options, ConnectionMultiplexer connectionMultiplexer)
        {
            _logger = logger;
            _options = options;
            _connectionMultiplexer = connectionMultiplexer;
            _subscriber = _connectionMultiplexer.GetSubscriber();
            _database = _connectionMultiplexer.GetDatabase();
        }

        public async Task PublishAsync(Type messageType, object message)
        {
            //1加入hashset
            //插入list
            //var data = new MessageBusData { Type = GetHandlerKey(messageType), Data = _options.Serializer.Serialize(message) };
            var jobData = JobData.CreateJobData(_options.Serializer.Serialize(message), GetTopic(messageType));

            var values = jobData.ToDictionary();
            await _database.HashSetAsync(jobData.JobId, values.ToArray());
            await _database.ListLeftPushAsync(jobData.Queue, jobData.JobId);
        }

        public Task SubscribeAsync<T>(Func<T, Task> handler, MessageBusContext context, CancellationToken cancellationToken = default)
        {
            //把队列加入临时列表
            //如果循环启动，返回
            //启动循环
            //循环每次判断临时队列列表是否有新订阅的队列，如果有复制过来，并清空 ,这里要加锁。

            throw new NotImplementedException();
        }

        public void Dispose()
        {

        }

        #region private

        private string GetHandlerKey(Type type)
        {
            return String.Concat(type.FullName, ", ", type.Assembly.GetName().Name);
        }

        private string GetTopic(Type type)
        {
            return $"{_options.TopicPrefix ?? ""}{type.Name}";
        }

        #endregion
    }
}
