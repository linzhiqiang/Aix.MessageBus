using Aix.MessageBus;
using Aix.MessageBus.Redis;
using Aix.MessageBus.Utils;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
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
               // return ProducerDelay(cancellationToken);
               return Producer(cancellationToken);
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

        private int[] DelaySeconds = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 30, 60, 300 };
        private async Task ProducerDelay(CancellationToken cancellationToken)
        {
            int producerCount = _cmdOptions.Count > 0 ? _cmdOptions.Count : 1;
            try
            {
                for (int i = 0; i < producerCount; i++)
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    await With.NoException(_logger, async () =>
                    {
                        var delay = TimeSpan.FromSeconds(DelaySeconds[i % DelaySeconds.Length]);
                        var delayDatetime = DateTime.Now.Add(delay);
                        var messageId = (i + 1).ToString();
                        var messageData = new BusinessMessage { MessageId = messageId, Content = $"我是内容_{messageId}", CreateTime = delayDatetime };
                        //  await _messageBus.PublishDelayAsync<BusinessMessage>(messageData, delay);
                        await _messageBus.PublishAsync(typeof(BusinessMessage), messageData);
                        //await _messageBus.PublishCrontabAsync<BusinessMessage>(messageData,new CrontabJobInfo {
                        //     JobId="test",
                        //      JobName="test",
                        //       CrontabExpression= "*/5 * * * * *"
                        //});
                        _logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}——{delayDatetime.ToString("yyyy-MM-dd HH:mm:ss")}生产数据：MessageId={messageData.MessageId}");
                        //await Task.Delay(TimeSpan.FromSeconds(1));
                    }, "生产消息");

                    //await With.NoException(_logger, async () =>
                    //{
                    //    var delay = TimeSpan.FromSeconds(DelaySeconds[i % DelaySeconds.Length]);
                    //    var delayDatetime = DateTime.Now.Add(delay);
                    //    var messageData = new BusinessMessage2 { MessageId = i.ToString(), Content = $"我是内容_{i}", CreateTime = delayDatetime };
                    //    await _messageBus.PublishDelayAsync<BusinessMessage2>(messageData, delay);
                    //    await _messageBus.PublishAsync<BusinessMessage2>(messageData);
                    //    _logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}——{delayDatetime.ToString("yyyy-MM-dd HH:mm:ss")}生产数据：MessageId={messageData.MessageId}");
                    //    //await Task.Delay(TimeSpan.FromSeconds(1));
                    //}, "生产消息");



                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "");
            }
        }


    }
}
