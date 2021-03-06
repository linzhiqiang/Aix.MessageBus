﻿using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;
using Aix.MessageBus.Utils;
using Aix.MessageBus.RabbitMQ.Model;

namespace Aix.MessageBus.RabbitMQ.Impl
{
    internal class RabbitMQDelayConsumer : IRabbitMQDelayConsumer
    {
        private IServiceProvider _serviceProvider;
        private ILogger<RabbitMQDelayConsumer> _logger;
        private RabbitMQMessageBusOptions _options;
        private IRabbitMQProducer _producer;

        IConnection _connection;
        IModel _channel;

        private volatile bool _isStart = false;
        bool _autoAck = true;
        ulong _currentDeliveryTag = 0; //记录最新的消费tag，便于手工确认
        private int Count = 0;//记录消费记录数，便于手工批量确认

        private int ManualCommitBatch = 1;

        public RabbitMQDelayConsumer(IServiceProvider serviceProvider, IRabbitMQProducer producer)
        {
            _serviceProvider = serviceProvider;
            _producer = producer;

            _logger = serviceProvider.GetService<ILogger<RabbitMQDelayConsumer>>();
            _options = serviceProvider.GetService<RabbitMQMessageBusOptions>();

            _connection = _serviceProvider.GetService<IConnection>();
            _channel = _connection.CreateModel();

            _autoAck = false;// _options.AutoAck;
            ManualCommitBatch = 1;// _options.ManualCommitBatch;
        }

        private void CreateDelayExchangeAndQueue()
        {
            var exchange = Helper.GeteDelayExchangeName(_options);
            //定义交换器
            _channel.ExchangeDeclare(
               exchange: exchange,
               type: ExchangeType.Direct,
               durable: true,
                autoDelete: false,
                arguments: null
               );

            foreach (var item in _options.DelayQueueConfig)
            {
                var delayMillTime = item.Key * 1000;

                var delayTopic = Helper.GetDelayTopic(_options, item.Value);
                var routingKey = Helper.GeteRoutingKey(delayTopic,"");
                var queue = delayTopic;

                var arguments = new Dictionary<string, object>();
                arguments.Add("x-message-ttl", delayMillTime);//毫秒   每个元素的有效期
                arguments.Add("x-dead-letter-exchange", Helper.GetDelayConsumerExchange(_options));
                //定义队列
                _channel.QueueDeclare(queue: queue,
                                         durable: true,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: arguments);

                //绑定交换器到队列
                _channel.QueueBind(queue, exchange, routingKey);
            }
        }
        public Task Subscribe()
        {
            _isStart = true;
            CreateDelayExchangeAndQueue();

            var exchange = Helper.GetDelayConsumerExchange(_options);
            var topic = Helper.GetDelayConsumerQueue(_options);
            var routingKey = Helper.GeteRoutingKey(topic,"");
            var queue = topic;

            //定义交换器
            _channel.ExchangeDeclare(
               exchange: exchange,
               type: ExchangeType.Fanout,
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

            var prefetchCount = (ushort)ManualCommitBatch;// _options.ManualCommitBatch;  //最大值：ushort.MaxValue
            _channel.BasicQos(0, prefetchCount, false); //客户端最多保留这么多条未确认的消息 只有autoack=false 有用
            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += Received;
            consumer.Shutdown += Consumer_Shutdown;

            String consumerTag = _channel.BasicConsume(queue, _autoAck, queue, consumer);
            return Task.CompletedTask;
        }

        private void Received(object sender, BasicDeliverEventArgs deliverEventArgs)
        {
            if (!_isStart) return; //这里有必要的，关闭时已经手工提交了，由于客户端还有累计消息会继续执行，但是不能确认（连接已关闭）
            try
            {
                Handler(deliverEventArgs.Body.ToArray());
            }
            catch (Exception ex)
            {
                _logger.LogError($"rabbitMQ消费延迟消息失败, {ex.Message}, {ex.StackTrace}");
            }
            finally
            {
                _currentDeliveryTag = deliverEventArgs.DeliveryTag;// _currentDeliveryTag = deliverEventArgs.DeliveryTag;//放在消费后，防止未处理完成但是关闭时也确认了该消息
                Count++;
                ManualAck(false);
            }
        }

        /// <summary>
        /// 延迟任务消费处理   三种情况： 1=任务到期进去即时任务，2=任务没有到期 继续进入延迟队列。3=任务到期，是失败重试的任务需要进入单个队列
        /// </summary>
        /// <param name="data"></param>
        private void Handler(byte[] data)
        {
            var delayMessage = _options.Serializer.Deserialize<RabbitMessageBusData>(data);

            var delayTime = TimeSpan.FromMilliseconds(delayMessage.ExecuteTimeStamp - DateUtils.GetTimeStamp(DateTime.Now));
            if (delayTime > TimeSpan.Zero)
            {//继续延迟
                var delayTopic = Helper.GetDelayTopic(_options, delayTime);
                _producer.ProduceDelayAsync(delayMessage.Type, data, delayTime);
            }
            else
            {//即时任务
                if (delayMessage.ErrorCount <= 0)
                {
                    _producer.ProduceAsync(delayMessage.Type, data);
                }
                else
                {
                    _producer.ErrorReProduceAsync(delayMessage.Type, delayMessage.GroupId, data);
                }
            }
        }

        private void Consumer_Shutdown(object sender, ShutdownEventArgs e)
        {
            _logger.LogInformation($"RabbitMQ关闭延迟消费者，reason:{e.ReplyText}");
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
                //if (Count % _options.ManualCommitBatch == 0)
                if (Count % ManualCommitBatch == 0)
                {
                    With.NoException(_logger, () => { _channel.BasicAck(_currentDeliveryTag, true); }, "批量手工确认消息");
                    Count = 0;
                }
            }
        }

        public void Close()
        {
            _isStart = false;
            _logger.LogInformation($"RabbitMQ开始关闭延迟消费者......");
            With.NoException(_logger, () =>
            {
                ManualAck(true);
            }, "关闭延迟消费者前手工确认最后未确认的消息");
            With.NoException(_logger, () => { this._channel?.Close(); }, "关闭延迟消费者");
        }

        public void Dispose()
        {
            Close();
        }
    }
}
