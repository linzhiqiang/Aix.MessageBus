using Aix.MessageBus.Redis.Impl;
using Aix.MessageBus.Redis.Model;
using Aix.MessageBus.Utils;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.Redis
{
    /// <summary>
    /// 仅实现队列功能，语义：至少一次
    /// </summary>
    public class RedisMessageBus : IMessageBus
    {
        private IServiceProvider _serviceProvider;
        private ILogger<RedisMessageBus> _logger;
        private RedisMessageBusOptions _options;
        ConnectionMultiplexer _connectionMultiplexer;

        ISubscriber _subscriber;
        IDatabase _database;
        IRedisProducer _producer;
        List<IDisposable> _consumers = new List<IDisposable>();
        private HashSet<string> Subscribers = new HashSet<string>();

        public RedisMessageBus(IServiceProvider serviceProvider, ILogger<RedisMessageBus> logger, RedisMessageBusOptions options, ConnectionMultiplexer connectionMultiplexer)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _options = options;
            _connectionMultiplexer = connectionMultiplexer;
            _subscriber = _connectionMultiplexer.GetSubscriber();
            _database = _connectionMultiplexer.GetDatabase();

            this._producer = new RedisProducer(this._serviceProvider);
        }

        public async Task PublishAsync(Type messageType, object message)
        {
            //1加入hashset
            //插入list
            var topic = GetTopic(messageType);
            var jobData = JobData.CreateJobData(_options.Serializer.Serialize(message));
            var result = await this._producer.ProduceAsync(topic, jobData);
            AssertUtils.IsTrue(result, $"redis生产者失败,topic:{topic}");
        }

        public async Task SubscribeAsync<T>(Func<T, Task> handler, MessageBusContext context, CancellationToken cancellationToken = default)
        {
            var topic = GetTopic(typeof(T));
            AssertUtils.IsTrue(!Subscribers.Contains(topic), "该类型重复订阅");
            Subscribers.Add(topic);
           
            context = context ?? new MessageBusContext();
            var threadCountStr = context.Config.GetValue("consumer.thread.count", "ConsumerThreadCount");
            var threadCount = !string.IsNullOrEmpty(threadCountStr) ? int.Parse(threadCountStr) : _options.DefaultConsumerThreadCount;
            AssertUtils.IsTrue(threadCount > 0, "消费者线程数必须大于0");

            _logger.LogInformation($"订阅[{topic}],threadcount={threadCount}");
            for (int i = 0; i < threadCount; i++)
            {
                var consumer = new RedisConsumer<T>(this._serviceProvider);
                _consumers.Add(consumer);
                consumer.OnMessage += async (obj) =>
                {
                    await handler(obj);
                };
                await consumer.Subscribe(topic, cancellationToken);
            }
           
        }


        public void Dispose()
        {
            foreach (var item in _consumers)
            {
                item.Dispose();
            }
        }

        #region private

        private string GetTopic(Type type)
        {
            return $"{_options.TopicPrefix ?? ""}{type.Name}";
        }


        #endregion
    }
}
