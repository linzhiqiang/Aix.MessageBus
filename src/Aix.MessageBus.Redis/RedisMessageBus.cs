using Aix.MessageBus.Redis.Impl;
using Aix.MessageBus.Redis.Model;
using Aix.MessageBus.Utils;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.Redis
{
    /// <summary>
    /// 仅实现队列功能
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
        ConcurrentDictionary<string, List<SubscriberInfo>> _subscriberDict = new ConcurrentDictionary<string, List<SubscriberInfo>>(); //订阅事件

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

        public async Task SubscribeAsyncSimple<T>(Func<T, Task> handler, MessageBusContext context = null, CancellationToken cancellationToken = default)
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
                consumer.OnMessage += async (message) =>
                {
                    var realObj = _options.Serializer.Deserialize<T>(message);
                    await handler(realObj);
                };
                await consumer.Subscribe(topic, cancellationToken);
            }

        }

        public async Task SubscribeAsync<T>(Func<T, Task> handler, MessageBusContext context = null, CancellationToken cancellationToken = default)
        {
            var topic = GetTopic(typeof(T));
            var subscriber = new SubscriberInfo
            {
                Type = typeof(T),
                Action = (message) =>
                {
                    var realObj = _options.Serializer.Deserialize<T>(message);
                    return handler(realObj);
                }
            };

            lock (typeof(T))
            {
                if (_subscriberDict.ContainsKey(topic))
                {
                    _subscriberDict[topic].Add(subscriber);
                }
                else
                {
                    _subscriberDict.TryAdd(topic, new List<SubscriberInfo> { subscriber });
                }
            }

            await SubscribeRedis<T>(topic, context, cancellationToken);
        }

        private async Task SubscribeRedis<T>(string topic, MessageBusContext context, CancellationToken cancellationToken)
        {
            if (Subscribers.Contains(topic)) return; //同一主题订阅一次即可
            lock (Subscribers)
            {
                if (Subscribers.Contains(topic)) return;
                Subscribers.Add(topic);
            }

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
                    var hasHandler = _subscriberDict.TryGetValue(topic, out List<SubscriberInfo> list);
                    if (!hasHandler || list == null) return;
                    foreach (var item in list)
                    {
                        await item.Action(obj);
                    }

                };
                await consumer.Subscribe(topic, cancellationToken);
            }
        }

        public void Dispose()
        {
            _producer.Dispose();

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
