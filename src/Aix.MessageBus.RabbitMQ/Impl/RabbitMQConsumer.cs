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
        ulong _currentDeliveryTag = 0;
        private int Count = 0;

        public RabbitMQConsumer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _logger = serviceProvider.GetService<ILogger<RabbitMQConsumer<T>>>();
            _options = serviceProvider.GetService<RabbitMQMessageBusOptions>();

            _connection = _serviceProvider.GetService<IConnection>();
            _channel = _connection.CreateModel();

            _autoAck = _options.AutoAck;
        }


        public event Func<T, Task> OnMessage;

        public Task Subscribe(string topic, string groupId, CancellationToken cancellationToken)
        {
            var exchange = Helper.GeteExchangeName(topic);
            var routingKey = Helper.GeteRoutingKey(topic);
            var queue = Helper.GeteQueueName(topic, groupId);

            //定义交换器
            _channel.ExchangeDeclare(
               exchange: exchange,
               type: ExchangeType.Fanout,//ExchangeType.Direct,
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



            //绑定交换器到队列
            _channel.QueueBind(queue, exchange, routingKey);

            var prefetchCount = _options.ManualCommitBatch;  //> ushort.MaxValue ? ushort.MaxValue : _options.ManualCommitBatch;
            _channel.BasicQos(0, prefetchCount, false); //客户端最多保留这么多条未确认的消息 只有autoack=false 有用
            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += Received;
            consumer.Shutdown += Consumer_Shutdown;

            String consumerTag = _channel.BasicConsume(queue, _autoAck, topic, consumer);
            return Task.CompletedTask;
        }

        private void Consumer_Shutdown(object sender, ShutdownEventArgs e)
        {
            // _logger.LogInformation($"RabbitMQ关闭消费者，reason:{e.ReplyText}");
        }

        private void Received(object sender, BasicDeliverEventArgs deliverEventArgs)
        {
            try
            {

                var obj = _options.Serializer.Deserialize<T>(deliverEventArgs.Body);
                Handler(obj);
            }
            catch (Exception ex)
            {
                _logger.LogError($"rabbitMQ消费接收消息失败, {ex.Message}, {ex.StackTrace}");
            }
            finally
            {
                _currentDeliveryTag = deliverEventArgs.DeliveryTag;// _currentDeliveryTag = deliverEventArgs.DeliveryTag;//放在消费后，防止未处理完成但是关闭时也确认了该消息
                Count++;
                ManualAck(false);
            }
        }

        private void ManualAck(bool isForce)
        {
            if (_autoAck) return;
            //单条确认
            // _channel.BasicAck(deliverEventArgs.DeliveryTag, false); //可以优化成批量提交 如没10条提交一次 true，最后关闭时记得也要提交最后一次的消费

            //批量确认
            if (isForce) //关闭时强制确认剩余的
            {
                if (Count > 0)
                {
                    // _logger.LogInformation("关闭时确认剩余的未确认消息"+ _currentDeliveryTag);
                    With.NoException(_logger, () => { _channel.BasicAck(_currentDeliveryTag, true); }, "关闭时确认剩余的未确认消息");
                }
            }
            else //按照批量确认
            {
                if (Count % _options.ManualCommitBatch == 0)
                {
                    With.NoException(_logger, () => { _channel.BasicAck(_currentDeliveryTag, true); }, "批量手工确认消息");
                    Count = 0;
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
            With.NoException(_logger, () => { ManualAck(true); }, "关闭消费者前手工确认最后未确认的消息");
            With.NoException(_logger, () => { this._channel?.Close(); }, "关闭消费者");
        }

        public void Dispose()
        {
            Close();
        }
    }
}
