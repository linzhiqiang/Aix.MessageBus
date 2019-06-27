using Aix.MessageBus.RabbitMQ.Impl;
using Aix.MessageBus.Utils;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.RabbitMQ
{
    public class RabbitMQMessageBus : IMessageBus
    {
        private IServiceProvider _serviceProvider;
        private ILogger<RabbitMQMessageBus> _logger;
        private RabbitMQMessageBusOptions _options;

        IConnection _connection;
        IRabbitMQProducer _producer;
        List<IDisposable> _consumers = new List<IDisposable>();
        private HashSet<string> Subscribers = new HashSet<string>();

        public RabbitMQMessageBus(IServiceProvider serviceProvider, ILogger<RabbitMQMessageBus> logger, RabbitMQMessageBusOptions options)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _options = options;

            _connection = _serviceProvider.GetService<IConnection>();
            this._producer = new RabbitMQProducer(this._serviceProvider);
        }

        public async Task PublishAsync(Type messageType, object message)
        {
            var topic = GetTopic(messageType);
            var data = _options.Serializer.Serialize(message);
            await this._producer.ProduceAsync(topic, data);
        }

        public async Task SubscribeAsync<T>(Func<T, Task> handler, MessageBusContext context = null, CancellationToken cancellationToken = default)
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
                var consumer = new RabbitMQConsumer<T>(this._serviceProvider);
                _consumers.Add(consumer);
                consumer.OnMessage += async(obj) =>
               {
                  await handler(obj);
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

            With.NoException(_logger,()=> {
                _connection.Close();
            },"关闭rabbitMQ连接");
         
        }

        #region private

        private string GetTopic(Type type)
        {
            return $"{_options.TopicPrefix ?? ""}{type.Name}";
        }


        #endregion
    }
}
