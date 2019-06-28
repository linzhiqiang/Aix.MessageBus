using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using System.Text;
using System.Threading.Tasks;
using Aix.MessageBus.Utils;

namespace Aix.MessageBus.RabbitMQ.Impl
{
    public class RabbitMQProducer : IRabbitMQProducer
    {
        private IServiceProvider _serviceProvider;
        private ILogger<RabbitMQProducer> _logger;
        private RabbitMQMessageBusOptions _options;

        IConnection _connection;
        IModel _channel;
        IBasicProperties _basicProperties;
        public RabbitMQProducer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _logger = _serviceProvider.GetService<ILogger<RabbitMQProducer>>();
            _options = _serviceProvider.GetService<RabbitMQMessageBusOptions>();

            _connection = _serviceProvider.GetService<IConnection>();
            _channel = _connection.CreateModel();

            _basicProperties = _channel.CreateBasicProperties();
            _basicProperties.ContentType = "text/plain";
            _basicProperties.DeliveryMode = 2;
        }
        public Task<bool> ProduceAsync(string topic, byte[] data)
        {
            var exchange = Helper.GeteExchangeName(topic);
            var routingKey = Helper.GeteRoutingKey(topic);
            
            _channel.BasicPublish(exchange: exchange,
                                              routingKey: routingKey,
                                              basicProperties: _basicProperties,
                                              body: data);

            return Task.FromResult(true) ;
        }

        public void Dispose()
        {
            _logger.LogInformation("RabbitMQ关闭生产者");
            if (this._channel != null)
            {
                With.NoException(_logger, () => { this._channel.Dispose(); }, "RabbitMQ关闭生产者");
            }
        }
    }
}
