﻿using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Aix.MessageBus.Utils;
using RabbitMQ.Client.Events;
using Aix.MessageBus.Exceptions;
using Aix.MessageBus.RabbitMQ.Model;

namespace Aix.MessageBus.RabbitMQ.Impl
{
    internal class RabbitMQConsumer : IRabbitMQConsumer
    {
        private IServiceProvider _serviceProvider;
        private ILogger<RabbitMQConsumer> _logger;
        private RabbitMQMessageBusOptions _options;
        IRabbitMQProducer _producer;

        IConnection _connection;
        IModel _channel;
        private volatile bool _isStart = false;
        bool _autoAck = true;
        ulong _currentDeliveryTag = 0; //记录最新的消费tag，便于手工确认
        private int Count = 0;//记录消费记录数，便于手工批量确认

        private string _groupId = "";

        public RabbitMQConsumer(IServiceProvider serviceProvider, IRabbitMQProducer producer)
        {
            _serviceProvider = serviceProvider;
            _producer = producer;

            _logger = serviceProvider.GetService<ILogger<RabbitMQConsumer>>();
            _options = serviceProvider.GetService<RabbitMQMessageBusOptions>();

            _connection = _serviceProvider.GetService<IConnection>();
            _channel = _connection.CreateModel();

            _autoAck = _options.AutoAck;
        }

        public event Func<RabbitMessageBusData, Task> OnMessage;

        public Task Subscribe(string topic, string groupId, CancellationToken cancellationToken)
        {
            _isStart = true;
            _groupId = groupId ?? "";
            var exchange = Helper.GeteExchangeName(topic);
            var routingKey = Helper.GeteRoutingKey(topic, groupId);
            var queue = Helper.GeteQueueName(topic, groupId);

            //定义交换器
            _channel.ExchangeDeclare(
               exchange: exchange,
               type: ExchangeType.Fanout,//所有绑定的队列都发送
               durable: true,
                autoDelete: false,
                arguments: null
               );

            //定义一个失败重试入口的交换器
            var errorReEnqueneExchangeName = Helper.GetErrorReEnqueneExchangeName(topic);
            _channel.ExchangeDeclare(
               exchange: errorReEnqueneExchangeName,
               type: ExchangeType.Direct,//根据routeingkey分发到不同队列
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
            _channel.QueueBind(queue, errorReEnqueneExchangeName, routingKey);

            var prefetchCount = _options.ManualCommitBatch;  //最大值：ushort.MaxValue
            _channel.BasicQos(0, prefetchCount, false); //客户端最多保留这么多条未确认的消息 只有autoack=false 有用
            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += Received;
            consumer.Shutdown += Consumer_Shutdown;

            String consumerTag = _channel.BasicConsume(queue, _autoAck, topic, consumer);
            return Task.CompletedTask;
        }

        private void Consumer_Shutdown(object sender, ShutdownEventArgs e)
        {
            _logger.LogInformation($"RabbitMQ关闭消费者，reason:{e.ReplyText}");
        }

        private void Received(object sender, BasicDeliverEventArgs deliverEventArgs)
        {
            if (!_isStart) return; //这里有必要的，关闭时已经手工提交了，由于客户端还有累计消息会继续执行，但是不能确认（连接已关闭）
            try
            {
                var data = _options.Serializer.Deserialize<RabbitMessageBusData>(deliverEventArgs.Body.ToArray());
                var isSuccess = Handler(data);
                if (isSuccess == false)
                {
                    ExecuteErrorToDelayTask(data);
                }
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

        /// <summary>
        /// 加入延迟队列重试
        /// </summary>
        /// <param name="data"></param>
        private void ExecuteErrorToDelayTask(RabbitMessageBusData data)
        {
            if (data.ErrorCount < _options.MaxErrorReTryCount)
            {
                var delay = TimeSpan.FromSeconds(GetDelaySecond(data.ErrorCount));
                data.ErrorCount++;
                data.GroupId = _groupId;
                data.ExecuteTimeStamp = DateUtils.GetTimeStamp(DateTime.Now.Add(delay));

                var delayData = _options.Serializer.Serialize(data);

                if (delay > TimeSpan.Zero)
                {
                    _producer.ProduceDelayAsync(data.Type, delayData, delay);
                }
                else
                {
                    _producer.ErrorReProduceAsync(data.Type, data.GroupId, delayData);
                }
            }
        }

        private int GetDelaySecond(int errorCount)
        {

            if (errorCount < _options.RetryStrategy.Length)
            {
                return _options.RetryStrategy[errorCount];
            }
            return _options.RetryStrategy[_options.RetryStrategy.Length - 1];
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
        private bool Handler(RabbitMessageBusData obj)
        {
            var isSuccess = true; //失败标识需要重试
            if (OnMessage == null) return isSuccess;

            try
            {
                OnMessage(obj).GetAwaiter().GetResult();
            }
            catch (RetryException ex)
            {
                isSuccess = false;
                _logger.LogError($"rabbitMQ消费失败重试, topic={obj.Type}，group={obj.GroupId}，ErrorCount={obj.ErrorCount}，{ex.Message}, {ex.StackTrace}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"rabbitMQ消费失败, topic={obj.Type}，group={obj.GroupId}，{ex.Message}, {ex.StackTrace}");
            }
            return isSuccess;
        }

        public void Close()
        {
            _isStart = false;
            _logger.LogInformation($"RabbitMQ开始关闭消费者......");
            With.NoException(_logger, () =>
            {
                ManualAck(true);
            }, "关闭消费者前手工确认最后未确认的消息");
            With.NoException(_logger, () => { this._channel?.Close(); }, "关闭消费者");
        }

        public void Dispose()
        {
            Close();
        }
    }
}
