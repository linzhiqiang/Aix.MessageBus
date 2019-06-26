using Aix.MessageBus;
using Aix.MessageBus.Kafka;
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
                //taskList.Add(Test(cancellationToken));

                await Task.WhenAll(taskList.ToArray());
            });

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        private async Task Subscribe(CancellationToken cancellationToken)
        {
            try
            {
                //订阅
                MessageBusContext context = new MessageBusContext();
                context.Config.Add("GroupId", "kafkaMessageGroup1"); //kafka消费者组(只有kafka使用)
                context.Config.Add("ConsumerThreadCount", "4");//该订阅的消费线程数，若是kafka注意和分区数匹配
                await _messageBus.SubscribeAsync<KafkaMessage>(async (message) =>
                {
                    var current = Interlocked.Increment(ref Count);
                    _logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}消费1数据：MessageId={message.MessageId},Content={message.Content},count={current}");

                   //await Task.Delay(50);

                    await Task.CompletedTask;

                }, context, cancellationToken);


                //订阅
                //MessageBusContext context2 = new MessageBusContext();
                //context2.Config.Add("GroupId", "kafkaMessageGroup2");//消费者组
                //context2.Config.Add("ConsumerThreadCount", "2");//该订阅的消费线程数，注意和分区数匹配

                //使用默认分组
                await _messageBus.SubscribeAsync<KafkaMessage2>(async (message) =>
                {
                    var current = Interlocked.Increment(ref Count);
                    _logger.LogInformation($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}消费2数据：MessageId={message.MessageId},Content={message.Content},count={current}");
                    await Task.CompletedTask;
                }, null, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "");
            }
        }
    }
}
