using Aix.MessageBus;
using Aix.MessageBus.Foundation.EventLoop;
using Aix.MessageBus.Utils;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Sample
{
    public class MessageBusProduerService : IHostedService
    {
        private ILogger<MessageBusProduerService> _logger;
        public IMessageBus _messageBus;
        CmdOptions _cmdOptions;
        ITaskExecutor  _taskExecutor;
        public MessageBusProduerService(ILogger<MessageBusProduerService> logger, IMessageBus messageBus, CmdOptions cmdOptions)
        {
            _logger = logger;
            _messageBus = messageBus;
            _cmdOptions = cmdOptions;
            _taskExecutor = new MultithreadTaskExecutor(8);
            _taskExecutor.OnException += _taskExecutor_OnException;
            _taskExecutor.Start();
        }

        private Task _taskExecutor_OnException(Exception arg)
        {
            _logger.LogError(arg,"任务执行器出错");
            return Task.CompletedTask;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Task.Run(async () =>
           {
               List<Task> taskList = new List<Task>(); //多个订阅者
               taskList.Add(Producer(cancellationToken));
               // taskList.Add(ProducerDelay(cancellationToken));
               await Task.WhenAll(taskList.ToArray());
           });

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("StopAsync");
            return Task.CompletedTask;
        }

        private async Task Producer(CancellationToken cancellationToken)
        {
            int producerCount = _cmdOptions.Count > 0 ? _cmdOptions.Count : 1;
            try
            {
                for (int i = 0; i < producerCount; i++)
                {
                    _taskExecutor.Execute(async(state)=> {
                        if (cancellationToken.IsCancellationRequested) return;
                        var index = (int)state;
                        var messageData = new BusinessMessage { MessageId = index.ToString(), Content = $"我是内容_{index}", CreateTime = DateTime.Now };
                        await _messageBus.PublishAsync(messageData);
                        _logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}生产数据：MessageId={messageData.MessageId}");

                    },i);

                    //if (cancellationToken.IsCancellationRequested) break;
                    //var messageData = new BusinessMessage { MessageId = i.ToString(), Content = $"我是内容_{i}", CreateTime = DateTime.Now };
                    //await _messageBus.PublishAsync(messageData);
                    //_logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}生产数据：MessageId={messageData.MessageId}");
              
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "生产消息失败");
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
                        var delay = TimeSpan.FromSeconds(i + 1);// TimeSpan.FromSeconds(DelaySeconds[i % DelaySeconds.Length]);
                        var delayDatetime = DateTime.Now.Add(delay);
                        var messageId = (i + 1).ToString();
                        var messageData = new BusinessMessage { MessageId = messageId, Content = $"我是内容_{messageId}", CreateTime = delayDatetime };
                        await _messageBus.PublishDelayAsync<BusinessMessage>(messageData, delay);
                        //await _messageBus.PublishAsync(typeof(BusinessMessage), messageData);
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
