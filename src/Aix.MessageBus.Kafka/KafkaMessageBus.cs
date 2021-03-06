﻿using Aix.MessageBus.Kafka.Impl;
using Aix.MessageBus.Kafka.Model;
using Aix.MessageBus.Utils;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.Kafka
{
    /// <summary>
    /// kafka实现messagebus
    /// </summary>
    public class KafkaMessageBus : IMessageBus
    {
        #region 属性 构造
        private IServiceProvider _serviceProvider;
        private ILogger<KafkaMessageBus> _logger;
        private KafkaMessageBusOptions _kafkaOptions;
        IKafkaProducer<Null, KafkaMessageBusData> _producer = null;
        List<IKafkaConsumer<Null, KafkaMessageBusData>> _consumerList = new List<IKafkaConsumer<Null, KafkaMessageBusData>>();

        private HashSet<string> Subscribers = new HashSet<string>();

        #endregion

        public KafkaMessageBus(IServiceProvider serviceProvider, ILogger<KafkaMessageBus> logger, KafkaMessageBusOptions kafkaOptions)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _kafkaOptions = kafkaOptions;

            this._producer = new KafkaProducer<Null, KafkaMessageBusData>(this._serviceProvider);
        }

        #region IMessageBus
        public async Task PublishAsync(Type messageType, object message)
        {
            AssertUtils.IsNotNull(message, "消息不能null");
            var topic = GetTopic(messageType);
            var data = new KafkaMessageBusData { Topic = topic, Data = _kafkaOptions.Serializer.Serialize(message) };
            await _producer.ProduceAsync(topic, new Message<Null, KafkaMessageBusData> { Value = data });
        }

        public Task PublishDelayAsync(Type messageType, object message, TimeSpan delay)
        {
            throw new NotImplementedException("kafka未实现延迟任务"); //建议使用数据库实现或 数据库加redis实现
        }

        
        public async Task SubscribeAsync<T>(Func<T, Task> handler, MessageBusContext context = null, CancellationToken cancellationToken = default(CancellationToken)) where T : class
        {
            string topic = GetTopic(typeof(T));

            context = context ?? new MessageBusContext();
            var groupId = context.Config.GetValue(MessageBusContextConstant.GroupId); //这里可以传不同的groupid，订阅相同对象
            groupId = !string.IsNullOrEmpty(groupId) ? groupId : _kafkaOptions.ConsumerConfig.GroupId;

            int.TryParse(context.Config.GetValue(MessageBusContextConstant.ConsumerThreadCount), out int threadCount);
            threadCount = threadCount > 0 ? threadCount : _kafkaOptions.DefaultConsumerThreadCount;
            AssertUtils.IsTrue(threadCount > 0, "消费者线程数必须大于0");

            ValidateSubscribe(topic, groupId);

            _logger.LogInformation($"-------------订阅[topic:{topic}]：groupid:{groupId},threadcount:{threadCount}-------------");
            for (int i = 0; i < threadCount; i++)
            {
                var consumer = new KafkaConsumer<Null, KafkaMessageBusData>(_serviceProvider);
                consumer.OnMessage += consumeResult =>
                {
                    return With.NoException(_logger, async () =>
                    {
                        var obj = _kafkaOptions.Serializer.Deserialize<T>(consumeResult.Message.Value.Data);
                        await handler(obj);
                    }, $"消费数据{consumeResult.Message.Value.Topic}");
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

        #endregion

        #region private
        private void ValidateSubscribe(string topic, string groupId)
        {
            lock (Subscribers)
            {
                var key = $"{topic}_{groupId}";
                AssertUtils.IsTrue(!Subscribers.Contains(key), "重复订阅");
                Subscribers.Add(key);
            }
        }
        private string GetTopic(Type type)
        {
            return Helper.GetTopic(_kafkaOptions, type);
        }

        #endregion
    }
}
