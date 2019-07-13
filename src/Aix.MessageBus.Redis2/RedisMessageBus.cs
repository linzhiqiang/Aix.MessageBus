using Aix.MessageBus.Exceptions;
using Aix.MessageBus.Redis2.BackgroundProcess;
using Aix.MessageBus.Redis2.Model;
using Aix.MessageBus.Redis2.RedisImpl;
using Aix.MessageBus.Utils;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.Redis2
{
    public class RedisMessageBus : IMessageBus
    {
        private IServiceProvider _serviceProvider;
        private ILogger<RedisMessageBus> _logger;
        private RedisMessageBusOptions _options;
        private RedisStorage _redisStorage;

        private HashSet<string> Subscribers = new HashSet<string>();
        ConcurrentDictionary<string, List<SubscriberInfo>> _subscriberDict = new ConcurrentDictionary<string, List<SubscriberInfo>>(); //订阅事件
        ProcessExecuter _processExecuter;
        BackgroundProcessContext backgroundProcessContext;
        public RedisMessageBus(IServiceProvider serviceProvider, ILogger<RedisMessageBus> logger
            , RedisMessageBusOptions options
            , ConnectionMultiplexer connectionMultiplexer
            , RedisStorage redisStorage)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _options = options;
            _redisStorage = redisStorage;

            backgroundProcessContext = new BackgroundProcessContext(default);
            _processExecuter = new ProcessExecuter(_serviceProvider, backgroundProcessContext);

            InitProcess();
        }
        public async Task PublishAsync(Type messageType, object message)
        {
            var topic = GetTopic(messageType);
            var jobData = JobData.CreateJobData(topic, _options.Serializer.Serialize(message));
            var result = await _redisStorage.Enqueue(jobData);
            AssertUtils.IsTrue(result, $"redis生产者失败,topic:{topic}");
        }

        public async Task PublishAsync(Type messageType, object message, TimeSpan delay)
        {
            var topic = GetTopic(messageType);
            var jobData = JobData.CreateJobData(topic, _options.Serializer.Serialize(message));
            var result = await _redisStorage.EnqueueDealy(jobData, delay);
            AssertUtils.IsTrue(result, $"redis生产者失败,topic:{topic}");
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

            lock (_subscriberDict)
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

            await SubscribeRedis(topic, context, cancellationToken);
        }

        public void Dispose()
        {
            _processExecuter.Close();
        }

        #region private

        private void InitProcess()
        {
            Task.Run(async () =>
            {
                await _processExecuter.AddProcess(new DelayedWorkProcess(_serviceProvider));
                await _processExecuter.AddProcess(new ErrorWorkerProcess(_serviceProvider));
            });
        }

        private async Task SubscribeRedis(string topic, MessageBusContext context, CancellationToken cancellationToken)
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
                var process = new WorkerProcess(_serviceProvider, topic, HandlerMessage);
                await _processExecuter.AddProcess(process);
            }
            backgroundProcessContext.SubscriberTopics.Add(topic);//便于ErrorProcess处理
        }

        private async Task<bool> HandlerMessage(MessageResult result)
        {
            var isSuccess = true; //需要重试返回false
            var hasHandler = _subscriberDict.TryGetValue(result.Topic, out List<SubscriberInfo> list);
            if (!hasHandler || list == null) return isSuccess;

            foreach (var item in list)
            {
                try
                {
                    await item.Action(result.Data);
                }
                catch (RetryException ex)
                {
                    _logger.LogError($"redis消费失败重试,topic:{result.Topic}, {ex.Message}, {ex.StackTrace}");
                    isSuccess = false;
                }
                catch (Exception ex)
                {
                    _logger.LogError($"redis消费失败,topic:{result.Topic}, {ex.Message}, {ex.StackTrace}");
                }
            }

            return isSuccess;
        }

        private string GetTopic(Type type)
        {
            return $"{_options.TopicPrefix ?? ""}{type.Name}";
        }


        #endregion
    }
}
