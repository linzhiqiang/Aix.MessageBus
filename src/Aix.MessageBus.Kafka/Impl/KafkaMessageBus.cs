using Aix.MessageBus.Exceptions;
using Aix.MessageBus.Kafka.Impl;
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
        IKafkaProducer<string, KafkaMessageBusData> _producer = null;
        List<IKafkaConsumer<string, KafkaMessageBusData>> _consumerList = new List<IKafkaConsumer<string, KafkaMessageBusData>>();
        List<IKafkaDelayConsumer> _delayConsumers = new List<IKafkaDelayConsumer>();
        private HashSet<string> Subscribers = new HashSet<string>();

        private CancellationTokenSource CloseCancellationTokenSource = new CancellationTokenSource();
        private CancellationToken Token;
        #endregion

        public KafkaMessageBus(IServiceProvider serviceProvider, ILogger<KafkaMessageBus> logger, KafkaMessageBusOptions kafkaOptions)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _kafkaOptions = kafkaOptions;

            this._producer = new KafkaProducer<string, KafkaMessageBusData>(this._serviceProvider);

            Token = CloseCancellationTokenSource.Token;
        }

        #region IMessageBus
        public async Task PublishAsync(Type messageType, object message)
        {
            AssertUtils.IsNotNull(message, "消息不能null");
            var topic = GetTopic(messageType);
            var data = new KafkaMessageBusData { Topic = topic, Data = _kafkaOptions.Serializer.Serialize(message), ExecuteTimeStamp = DateUtils.GetTimeStamp(DateTime.Now) };
            var keyValue = AttributeUtils.GetPropertyValue<RouteKeyAttribute>(message);
            await _producer.ProduceAsync(topic, new Message<string, KafkaMessageBusData> { Key = keyValue?.ToString(), Value = data });
        }

        public async Task PublishDelayAsync(Type messageType, object message, TimeSpan delay)
        {
            throw new NotImplementedException("kafka未实现延迟任务"); //建议使用数据库实现或 数据库加redis实现
            if (delay <= TimeSpan.Zero)
            {
                await PublishAsync(messageType, message);
                return;
            }
            AssertUtils.IsNotNull(message, "消息不能null");
            var delayTopic = Helper.GetDelayTopic(_kafkaOptions, delay);
            var topic = GetTopic(messageType);
            var data = new KafkaMessageBusData { Topic = topic, Data = _kafkaOptions.Serializer.Serialize(message), ExecuteTimeStamp = DateUtils.GetTimeStamp(DateTime.Now.Add(delay)) };

            await _producer.ProduceAsync(delayTopic, new Message<string, KafkaMessageBusData> { Key = null, Value = data });
        }

        public async Task SubscribeAsync<T>(Func<T, Task> handler, SubscribeOptions subscribeOptions = null, CancellationToken cancellationToken = default(CancellationToken)) where T : class
        {
            cancellationToken = Token;
            StartDelay(cancellationToken);
            string topic = GetTopic(typeof(T));

            var groupId = subscribeOptions?.GroupId;
            groupId = !string.IsNullOrEmpty(groupId) ? groupId : _kafkaOptions.ConsumerConfig.GroupId;

            var threadCount = subscribeOptions?.ConsumerThreadCount ?? 0;
            threadCount = threadCount > 0 ? threadCount : _kafkaOptions.DefaultConsumerThreadCount;
            AssertUtils.IsTrue(threadCount > 0, "消费者线程数必须大于0");

            ValidateSubscribe(topic, groupId);

            _logger.LogInformation($"-------------订阅[topic:{topic}]：groupid:{groupId},threadcount:{threadCount}-------------");
            for (int i = 0; i < threadCount; i++)
            {
                var consumer = new KafkaConsumer<string, KafkaMessageBusData>(_serviceProvider);
                consumer.OnMessage += async (consumeResult) =>
                 {
                     // 重试实现
                     var isSuccess = true;
                     var messageBusData = consumeResult.Message.Value;
                     try
                     {
                         var obj = _kafkaOptions.Serializer.Deserialize<T>(messageBusData.Data);
                         await handler(obj);
                     }
                     catch (RetryException ex)
                     {
                         isSuccess = false;
                         _logger.LogError(ex, $"kafka消费失败重试, topic={messageBusData.Topic}，ErrorCount={messageBusData.ErrorCount}");
                     }
                     catch (Exception ex)
                     {
                         _logger.LogError(ex, $"kafka消费数据{messageBusData.Topic}");
                     }
                     if (isSuccess == false)
                     {
                         await ExecuteErrorToDelayTask(consumeResult);
                     }
                     //await With.NoException(_logger, async () =>
                     //{
                     //    var obj = _kafkaOptions.Serializer.Deserialize<T>(consumeResult.Message.Value.Data);
                     //    await handler(obj);
                     //}, $"消费数据{consumeResult.Message.Value.Topic}");
                 };

                _consumerList.Add(consumer);
                await consumer.Subscribe(topic, groupId, cancellationToken);
            }
        }


        public void Dispose()
        {
            With.NoException(_logger, CloseCancellationTokenSource.Cancel, "取消任务");
            _logger.LogInformation("KafkaMessageBus 释放...");
            With.NoException(_logger, () => { _producer?.Dispose(); }, "关闭消费者");

            foreach (var item in _consumerList)
            {

                With.NoException(_logger, () => { item.Close(); }, "关闭消费者");
            }
            foreach (var item in _delayConsumers)
            {
                With.NoException(_logger, () => { item.Close(); }, "关闭延迟消费者");
            }
        }

        #endregion

        private volatile bool _isStartDelay = false;
        private void StartDelay(CancellationToken cancellationToken)
        {
            return;
            cancellationToken = Token;
            if (_isStartDelay) return;
            lock (this)
            {
                if (_isStartDelay) return;
                _isStartDelay = true;
            }

            foreach (var item in _kafkaOptions.GetDelayQueueConfig())
            {
                var delay = TimeSpan.FromSeconds(item.Key);
                var topic = Helper.GetDelayTopic(_kafkaOptions, delay);
                for (int i = 0; i < _kafkaOptions.DelayConsumerThreadCount; i++)
                {
                    var delayConsumer = new KafkaDelayConsumer(_serviceProvider, topic, delay, _producer);
                    delayConsumer.Subscribe(cancellationToken);
                    _delayConsumers.Add(delayConsumer);
                }
            }
        }

        /// <summary>
        /// 加入延迟队列重试
        /// </summary>
        /// <param name="data"></param>
        private async Task ExecuteErrorToDelayTask(ConsumeResult<string, KafkaMessageBusData> consumeResult)
        {
            return;
            var messageBusData = consumeResult.Message.Value;
            if (messageBusData.ErrorCount >= _kafkaOptions.MaxErrorReTryCount) return;
            var delay = TimeSpan.FromSeconds(GetDelaySecond(messageBusData.ErrorCount));
            messageBusData.ErrorCount++;
            messageBusData.ExecuteTimeStamp = DateUtils.GetTimeStamp(DateTime.Now.Add(delay));
            consumeResult.Message.Value = messageBusData;
            //加入延迟topic 即可   延迟消费者会把任务重新插入原来对应topic中
            if (delay > TimeSpan.Zero)
            {
                var delayTopic = Helper.GetDelayTopic(_kafkaOptions, delay);
                await _producer.ProduceAsync(delayTopic, consumeResult.Message);
            }
            else //立即重试
            {
                await _producer.ProduceAsync(messageBusData.Topic, consumeResult.Message);
            }
        }

        private int GetDelaySecond(int errorCount)
        {
            var retryStrategy = _kafkaOptions.GetRetryStrategy();
            if (errorCount < retryStrategy.Length)
            {
                return retryStrategy[errorCount];
            }
            return retryStrategy[retryStrategy.Length - 1];
        }

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
