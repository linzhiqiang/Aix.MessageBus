using Aix.MessageBus;
using Aix.MessageBus.Exceptions;
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
    public class MessageBusConsumeService : IHostedService
    {
        private ILogger<MessageBusConsumeService> _logger;
        public IMessageBus _messageBus;

        private int Count = 0;
        public MessageBusConsumeService(ILogger<MessageBusConsumeService> logger, IMessageBus messageBus)
        {
            _logger = logger;
            _messageBus = messageBus;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Task.Run(async () =>
            {
                List<Task> taskList = new List<Task>(); //多个订阅者
                taskList.Add(Subscribe(cancellationToken));
                // taskList.Add(Subscribe2(cancellationToken));

                await Task.WhenAll(taskList.ToArray());
            });

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("StopAsync");
            return Task.CompletedTask;
        }

        private async Task Subscribe(CancellationToken cancellationToken)
        {
            try
            {
                {
                    //订阅
                    SubscribeOptions subscribeOptions = new SubscribeOptions();
                    //subscribeOptions.GroupId = "group1";
                    //subscribeOptions.ConsumerThreadCount = 4;
                    await _messageBus.SubscribeAsync<BusinessMessage>(async (message) =>
                    {
                        var current = Interlocked.Increment(ref Count);
                        _logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}消费--1--数据：MessageId={message.MessageId},Content={message.Content},count={current}");
                        // throw new Exception();
                        // await Task.Delay(100);
                       // throw new RetryException();
                        await Task.CompletedTask;
                    }, subscribeOptions, cancellationToken);
                }

                //{
                //    //订阅
                //    SubscribeOptions subscribeOptions2 = new SubscribeOptions();
                //    subscribeOptions2.GroupId = "group2";
                //    subscribeOptions2.ConsumerThreadCount = 2;

                //    //使用默认分组
                //    await _messageBus.SubscribeAsync<BusinessMessage>(async (message) =>
                //    {
                //        var current = Interlocked.Increment(ref Count);
                //        _logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}消费--2--数据：MessageId={message.MessageId},Content={message.Content},count={current}");
                //        await Task.CompletedTask;
                //    }, subscribeOptions2, cancellationToken);
                //}

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "订阅失败");
            }
        }

    }
}
