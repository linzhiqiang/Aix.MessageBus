using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;
using Aix.MessageBus.Utils;
using Aix.MessageBus.Redis.Foundation;

namespace Aix.MessageBus.Redis.Impl
{
    public class RedisConsumer<T> : IRedisConsumer<T>
    {
        private IServiceProvider _serviceProvider;
        private ILogger<RedisConsumer<T>> _logger;
        private RedisMessageBusOptions _options;

        private volatile bool _isStart = false;
        ConnectionMultiplexer _connectionMultiplexer;
        IDatabase _database;
        DistributedLock _distributedLock;


        public RedisConsumer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _logger = serviceProvider.GetService<ILogger<RedisConsumer<T>>>();
            _options = serviceProvider.GetService<RedisMessageBusOptions>();

            _connectionMultiplexer = serviceProvider.GetService<ConnectionMultiplexer>();
            _database = _connectionMultiplexer.GetDatabase();
            _distributedLock = new DistributedLock(_connectionMultiplexer);
        }

        public event Func<T, Task> OnMessage;

        public Task Subscribe(string topic, CancellationToken cancellationToken)
        {
            return Task.Run(async () =>
            {
                lock (this)
                {
                    if (_isStart == true)
                    {
                        return;
                    }
                    _isStart = true;
                }
                //初始化没有执行或者执行没有确认的任务，重新入队
                await ProcessNoAck(topic, cancellationToken);

                await StartPoll(topic, cancellationToken);

            });
        }

        private Task ProcessNoAck(string topic, CancellationToken cancellationToken)
        {
            Task.Run(async () =>
            {
                //分布式锁
                var lockKey = $"{_options.TopicPrefix}ProcessNoAck:lock";
                await _distributedLock.Lock(lockKey, TimeSpan.FromMinutes(2),async() =>
                {
                    TimeSpan delayTime = TimeSpan.FromSeconds(30);
                    await Task.Delay(delayTime);
                    var processingQueue = GetProcessingQueueName(topic);

                    //循环拉取 重新入队
                    int deleteCount = 0;
                    var length = 0;
                    var PerBatchSize = 50;
                    var start = PerBatchSize * -1;
                    var end = -1;
                    do
                    {
                        var list = _database.ListRange(processingQueue, start, end);
                        length = list.Length;
                        deleteCount = await ProcessNoAckJob(topic, list);

                        end = 0 - ((length - deleteCount) + 1);
                        start = end - PerBatchSize + 1;

                    }
                    while (length > 0);

                },()=> { return Task.CompletedTask; });
            });

            return Task.CompletedTask;
        }

        private async Task<int> ProcessNoAckJob(string topic, RedisValue[] list)
        {
            int deleteCount = 0;
            for (var i = list.Length - 1; i >= 0; i--)
            {
                var jobId = list[i];

                var value = await _database.HashGetAsync(GetJobHashId(jobId), "ExecuteTime");
                var executeTime = DateUtils.ToDateTimeNullable(value);
                if (executeTime != null)
                {
                    if (DateTime.Now - executeTime.Value > TimeSpan.FromSeconds(30))
                    {
                        await _database.ListRemoveAsync(GetProcessingQueueName(topic), jobId);
                        await _database.ListLeftPushAsync(topic, jobId);
                        deleteCount++;
                    }
                }
                else
                {
                    var createTime = DateUtils.ToDateTimeNullable(await _database.HashGetAsync(GetJobHashId(jobId), "CreateTime"));
                    if (createTime.HasValue && DateTime.Now - createTime.Value > TimeSpan.FromSeconds(30))
                    {
                        await _database.ListRemoveAsync(GetProcessingQueueName(topic), jobId);
                        await _database.ListLeftPushAsync(topic, jobId);
                        deleteCount++;
                    }
                }

            }

            return deleteCount;
        }
        private Task StartPoll(string topic, CancellationToken cancellationToken)
        {
            Task.Run(async () =>
            {
                try
                {
                    while (_isStart && !cancellationToken.IsCancellationRequested)
                    {
                        await With.NoException(_logger, async () =>
                        {
                            await Consumer(topic);
                        }, "消费拉取消息系统异常");
                    }
                }
                finally
                {
                    _logger.LogInformation("退出消费循环 关闭消费者...");
                    this.Close();
                }

            });

            return Task.CompletedTask;
        }
        private async Task Consumer(string topic)
        {
            var processingQueue = GetProcessingQueueName(topic);
            string jobId = await _database.ListRightPopLeftPushAsync(topic, processingQueue);
            await _database.HashSetAsync(GetJobHashId(jobId), new HashEntry[] {
                     new HashEntry("ExecuteTime",DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"))
             });

            if (string.IsNullOrEmpty(jobId))
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                return;
            }
            byte[] data = await _database.HashGetAsync(GetJobHashId(jobId), "Data");//取出数据字段
            var obj = _options.Serializer.Deserialize<T>(data);
            await Handler(obj);

            await _database.ListRemoveAsync(processingQueue, jobId);
            await _database.KeyDeleteAsync(GetJobHashId(jobId));
        }

        private async Task Handler(T obj)
        {
            if (OnMessage != null)
            {
                await With.NoException(_logger, async () =>
                {
                    await OnMessage(obj);
                }, "redis消费失败");
            }
        }

        public void Close()
        {
            this._isStart = false;
        }

        public void Dispose()
        {
            Close();
        }

        #region private

        private string GetProcessingQueueName(string queue)
        {
            return $"{queue}:processing";
        }
        private string GetJobHashId(string jobId)
        {
            //return GetRedisKey("jobdata" + $":job:{jobId}");

            return $"{_options.TopicPrefix ?? ""}jobdata:{jobId}";
        }
        #endregion
    }
}
