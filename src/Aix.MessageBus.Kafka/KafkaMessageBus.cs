using Aix.MessageBus.Kafka.Impl;
using Aix.MessageBus.Utils;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.Kafka
{
    /// <summary>
    /// kafka版MessageBus，每个类型创建一个topic, 支持手工提交offset
    /// </summary>
    public class KafkaMessageBus : IMessageBus
    {
        #region 属性 构造
        private IServiceProvider _serviceProvider;
        private ILogger<KafkaMessageBus> _logger;
        private KafkaMessageBusOptions _kafkaOptions;
        IKafkaProducer<Null, MessageBusData> _producer = null;
        List<IKafkaConsumer<Null, MessageBusData>> _consumerList = new List<IKafkaConsumer<Null, MessageBusData>>();

        ConcurrentDictionary<string, List<SubscriberInfo>> _subscriberDict = new ConcurrentDictionary<string, List<SubscriberInfo>>();
        HashSet<Type> _subscriberTypeSet = new HashSet<Type>();

        private CancellationToken _cancellationToken = default(CancellationToken);
        private volatile bool _isStart = false;

        public KafkaMessageBus(IServiceProvider serviceProvider, ILogger<KafkaMessageBus> logger, KafkaMessageBusOptions kafkaOptions)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _kafkaOptions = kafkaOptions;
        }

        #endregion

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (_isStart) return;

            lock (this)
            {
                if (_isStart) return;
                _isStart = true;
            }
            _cancellationToken = cancellationToken;

            if ((_kafkaOptions.ClientMode & ClientMode.Producer) == ClientMode.Producer)
            {
                this._producer = new KafkaProducer<Null, MessageBusData>(this._serviceProvider);
            }
            if ((_kafkaOptions.ClientMode & ClientMode.Consumer) == ClientMode.Consumer)
            {
                //消费者连接订阅时再创建
                foreach (var type in _subscriberTypeSet)
                {
                    await SubscribeKafka(type, cancellationToken);
                    if (this._kafkaOptions.TopicMode == TopicMode.Single) //如果是单topic只注册一个就行了，所有类型用一个topic存储
                    {
                        break;
                    }
                }
            }

        }

        public async Task PublishAsync(Type messageType, object message)
        {
            var data = new MessageBusData { Type = GetHandlerKey(messageType), Data = _kafkaOptions.Serializer.Serialize(message) };
            await _producer.ProduceAsync(GetTopic(messageType), new Message<Null, MessageBusData> { Value = data });
        }

        public Task SubscribeAsync<T>(Func<T, Task> handler)
        {
            string handlerKey = GetHandlerKey(typeof(T)); //handler缓存key
            var subscriber = new SubscriberInfo
            {
                Type = typeof(T),
                Action = (message) =>
                {
                    var realObj = _kafkaOptions.Serializer.Deserialize<T>(message);
                    return handler(realObj);
                }
            };

            bool hasSubscribeType = false; //该类型是否已订阅过kafka
            lock (typeof(T))
            {
                if (_subscriberDict.ContainsKey(handlerKey))
                {
                    hasSubscribeType = true;
                    _subscriberDict[handlerKey].Add(subscriber);
                }
                else
                {
                    _subscriberDict.TryAdd(handlerKey, new List<SubscriberInfo> { subscriber });

                }
            }
            if (!hasSubscribeType) //没有订阅过kafka  kafka主题订阅一次就够了
            {
                _subscriberTypeSet.Add(typeof(T));
                //await SubscribeKafka(typeof(T));
            }

            return Task.CompletedTask;
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

        private Task SubscribeKafka(Type type, CancellationToken cancellationToken)
        {
            Task.Run(async () =>
            {
                for (int i = 0; i < _kafkaOptions.ConsumerThreadCount; i++)
                {
                    var consumer = new KafkaConsumer<Null, MessageBusData>(_serviceProvider);
                    consumer.OnMessage += Handler;
                    _consumerList.Add(consumer);
                    await consumer.Subscribe(GetTopic(type), cancellationToken);
                }
            }, cancellationToken);

            return Task.CompletedTask;
        }


        private async Task Handler(ConsumeResult<Null, MessageBusData> consumeResult)
        {
            string handlerKey = consumeResult.Value.Type;
            var hasHandler = _subscriberDict.TryGetValue(handlerKey, out List<SubscriberInfo> list);
            if (!hasHandler || list == null) return;
            foreach (var item in list)
            {
                if (this._cancellationToken.IsCancellationRequested) return;
                await With.NoException(_logger, async () =>
                {
                    await item.Action(consumeResult.Value.Data);
                }, $"消费数据{consumeResult.Value.Type}");
            }
        }

        private string GetHandlerKey(Type type)
        {
            //return type.FullName;
            return String.Concat(type.FullName, ", ", type.Assembly.GetName().Name);
        }

        private string GetTopic(Type type)
        {
            if (this._kafkaOptions.TopicMode == TopicMode.Single)
            {
                return GetTopic(this._kafkaOptions.Topic);
            }
            return GetTopic(type.Name);
        }
        private string GetTopic(string name)
        {
            return $"{_kafkaOptions.TopicPrefix ?? ""}{name}";
        }

        #endregion
    }
}
