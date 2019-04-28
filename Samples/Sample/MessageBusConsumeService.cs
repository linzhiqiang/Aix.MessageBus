﻿using Aix.MessageBus;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
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
                //taskList.Add(Test(cancellationToken));

                await Task.WhenAll(taskList.ToArray());

                await this._messageBus.StartAsync(cancellationToken);
            });

            //Task.Run(async()=> {
            //    await Task.Delay(20*1000);
            //    _messageBus.Dispose();
            //});
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        private async Task Subscribe(CancellationToken cancellationToken)
        {
            await _messageBus.SubscribeAsync<KafkaMessage>(async (message) =>
            {
                var current = Interlocked.Increment(ref Count);
                _logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}消费数据：MessageId={message.MessageId},Content={message.Content},count={current}");
               // await Task.Delay(100);
                await Task.CompletedTask;
            });
        }
    }
}
