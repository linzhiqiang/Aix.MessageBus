using Aix.MessageBus.Kafka.Impl;
using Aix.MessageBus.Utils;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.Kafka
{
    public class KafkaMessageBus : IMessageBus
    {
        #region 属性 构造
        private IServiceProvider _serviceProvider;
        private ILogger<KafkaMessageBus> _logger;
        private KafkaMessageBusOptions _kafkaOptions;
        IKafkaProducer<Null, MessageBusData> _producer = null;
        List<IKafkaConsumer<Null, MessageBusData>> _consumerList = new List<IKafkaConsumer<Null, MessageBusData>>();
        private CancellationToken _cancellationToken = default;

        private HashSet<string> Subscribers = new HashSet<string>();

        #endregion

        public KafkaMessageBus(IServiceProvider serviceProvider, ILogger<KafkaMessageBus> logger, KafkaMessageBusOptions kafkaOptions)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _kafkaOptions = kafkaOptions;

            this._producer = new KafkaProducer<Null, MessageBusData>(this._serviceProvider);
        }

        public async Task PublishAsync(Type messageType, object message)
        {
            var data = new MessageBusData { Type = GetHandlerKey(messageType), Data = _kafkaOptions.Serializer.Serialize(message) };
            await _producer.ProduceAsync(GetTopic(messageType), new Message<Null, MessageBusData> { Value = data });
        }

        public async Task SubscribeAsync<T>(Func<T, Task> handler, MessageBusContext context=null, CancellationToken cancellationToken=default)
        {
            string topic = GetTopic(typeof(T));

            context = context ?? new MessageBusContext();
            var groupId = context.Config.GetValue("group.id", "groupid");
            groupId = !string.IsNullOrEmpty(groupId) ? groupId : _kafkaOptions.DefaultConsumerGroupId;
            var threadCountStr = context.Config.GetValue("consumer.thread.count", "ConsumerThreadCount");
            var threadCount = !string.IsNullOrEmpty(threadCountStr) ? int.Parse(threadCountStr) : _kafkaOptions.DefaultConsumerThreadCount;
            AssertUtils.IsTrue(threadCount > 0, "消费者线程数必须大于0");

            var key = $"{topic}_{groupId}";
            AssertUtils.IsTrue(!Subscribers.Contains(key), "重复订阅");
            Subscribers.Add(key);

            _logger.LogInformation($"订阅[{topic}]：groupid:{groupId},threadcount={threadCount}");
            for (int i = 0; i < threadCount; i++)
            {
                var consumer = new KafkaConsumer<Null, MessageBusData>(_serviceProvider);
                consumer.OnMessage += consumeResult =>
                {
                    if (this._cancellationToken.IsCancellationRequested) return Task.CompletedTask;
                    return With.NoException(_logger, async () =>
                   {
                       var obj = _kafkaOptions.Serializer.Deserialize<T>(consumeResult.Value.Data);
                       await handler(obj);
                   }, $"消费数据{consumeResult.Value.Type}");
                };

                _consumerList.Add(consumer);
                await consumer.Subscribe(topic, groupId, cancellationToken);
            }
        }

        public void Dispose()
        {
            _logger.LogInformation("KafkaMessageBus 释放...");
            With.NoException(_logger, () => { _producer?.Dispose(); }, "关闭生产者");

            foreach (var item in _consumerList)
            {
                item.Close();
            }
        }

        #region private

        private string GetHandlerKey(Type type)
        {
            return String.Concat(type.FullName, ", ", type.Assembly.GetName().Name);
        }

        private string GetTopic(Type type)
        {
            return $"{_kafkaOptions.TopicPrefix ?? ""}{type.Name}";
        }

        #endregion
    }
}
