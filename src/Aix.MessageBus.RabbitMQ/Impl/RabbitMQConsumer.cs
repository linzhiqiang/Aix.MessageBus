using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Aix.MessageBus.Utils;
using RabbitMQ.Client.Events;

namespace Aix.MessageBus.RabbitMQ.Impl
{
    public class RabbitMQConsumer<T> : IRabbitMQConsumer<T>
    {
        private IServiceProvider _serviceProvider;
        private ILogger<RabbitMQConsumer<T>> _logger;
        private RabbitMQMessageBusOptions _options;

        IConnection _connection;
        IModel _channel;

        bool _autoAck = true;

        public RabbitMQConsumer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _logger = serviceProvider.GetService<ILogger<RabbitMQConsumer<T>>>();
            _options = serviceProvider.GetService<RabbitMQMessageBusOptions>();

            _connection = _serviceProvider.GetService<IConnection>();
            _channel = _connection.CreateModel();

            _autoAck = _options.ConsumerMode == ConsumerMode.AtMostOnce;
        }


        public event Func<T, Task> OnMessage;

        public Task Subscribe(string topic, CancellationToken cancellationToken)
        {
            var exchange = $"{topic}-exchange";
            var routingKey = $"{topic}-routingkey";
            var queue = $"{topic}-queue";

            //定义交换器
            _channel.ExchangeDeclare(
               exchange: exchange,
               type: ExchangeType.Direct,
               durable: true,
                autoDelete: false,
                arguments: null
               );

            //定义队列
            _channel.QueueDeclare(queue: queue,
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

            _channel.BasicQos(0, 1, false); //客户端最多保留10调未确认的消息 只有autoack=false 有用

            //绑定交换器到队列
            _channel.QueueBind(queue, exchange, routingKey);

            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += Received;
            String consumerTag = _channel.BasicConsume(queue, _autoAck, topic, consumer);
            return Task.CompletedTask;
        }

        private void Received(object sender, BasicDeliverEventArgs ea)
        {
            try
            {
                var obj = _options.Serializer.Deserialize<T>(ea.Body);
                Handler(obj);
            }
            catch (Exception ex)
            {
                _logger.LogError($"rabbitMQ消费接收消息失败, {ex.Message}, {ex.StackTrace}");
            }
            finally
            {
                if (!_autoAck)
                {
                    _channel.BasicAck(ea.DeliveryTag, false);
                }
            }
        }

        /// <summary>
        /// 执行消费事件
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        private void Handler(T obj)
        {
            if (OnMessage != null)
            {
                With.NoException(_logger, () =>
              {
                  OnMessage(obj).GetAwaiter().GetResult();
              }, "rabbitMQ消费失败");
            }
        }

        public void Close()
        {
            _logger.LogInformation("RabbitMQ关闭消费者");

            With.NoException(_logger, () => { this._channel?.Close(); }, "关闭消费者");
        }

        public void Dispose()
        {
            Close();
        }
    }
}
