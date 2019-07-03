using Aix.MessageBus;
using Aix.MessageBus.Redis;
using Aix.MessageBus.Utils;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Sample
{
    public class MessageBusProduerService : IHostedService
    {
        private ILogger<MessageBusProduerService> _logger;
        public IMessageBus _messageBus;
        CmdOptions _cmdOptions;
        public MessageBusProduerService(ILogger<MessageBusProduerService> logger, IMessageBus messageBus, CmdOptions cmdOptions)
        {
            _logger = logger;
            _messageBus = messageBus;
            _cmdOptions = cmdOptions;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Task.Run(() =>
           {
               return ProducerDelay(cancellationToken);
               //return Producer(cancellationToken);
           });

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("StopAsync");
            return Task.CompletedTask;
        }

        private async Task Producer(CancellationToken cancellationToken)
        {
            int producerCount = _cmdOptions.Count > 0 ? _cmdOptions.Count : 1;
            try
            {
                for (int i = 0; i < producerCount; i++)
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    await With.NoException(_logger, async () =>
                    {
                        var messageData = new BusinessMessage { MessageId = i.ToString(), Content = $"我是内容_{i}", CreateTime = DateTime.Now };
                        await _messageBus.PublishAsync(messageData);
                        _logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}生产数据：MessageId={messageData.MessageId}");
                        //await Task.Delay(TimeSpan.FromSeconds(1));
                    }, "生产消息");

                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "");
            }
        }

        private async Task ProducerDelay(CancellationToken cancellationToken)
        {
            RedisMessageBus messagebus = _messageBus as RedisMessageBus;
            int producerCount = _cmdOptions.Count > 0 ? _cmdOptions.Count : 1;
            try
            {
                for (int i = 0; i < producerCount; i++)
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    await With.NoException(_logger, async () =>
                    {
                        var delay = TimeSpan.FromSeconds(13);
                        var delayDatetime = DateTime.Now.Add(delay);
                        var messageData = new BusinessMessage { MessageId = i.ToString(), Content = $"我是内容_{i}", CreateTime = delayDatetime };
                        await messagebus.PublishAsync(typeof(BusinessMessage),messageData,TimeSpan.FromSeconds(6));
                        _logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}——{delayDatetime.ToString("yyyy-MM-dd HH:mm:ss")}生产数据：MessageId={messageData.MessageId}");
                        //await Task.Delay(TimeSpan.FromSeconds(1));
                    }, "生产消息");

                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "");
            }
        }
    }
}
