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

        TimeSpan _noAckReEnqueueDelay = TimeSpan.FromSeconds(30);

        public RedisConsumer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _logger = serviceProvider.GetService<ILogger<RedisConsumer<T>>>();
            _options = serviceProvider.GetService<RedisMessageBusOptions>();

            _connectionMultiplexer = serviceProvider.GetService<ConnectionMultiplexer>();
            _database = _connectionMultiplexer.GetDatabase();
            _distributedLock = new DistributedLock(_connectionMultiplexer);
            _noAckReEnqueueDelay = TimeSpan.FromSeconds(_options.NoAckReEnqueueDelay);
        }

        public event Func<byte[], Task> OnMessage;

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

        /// <summary>
        /// 抓取到没有消费的或者消费完没有确认的 要重新入队，实现至少一次的语义
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private Task ProcessNoAck(string topic, CancellationToken cancellationToken)
        {
            Task.Run(async () =>
            {
                await Task.Delay(_noAckReEnqueueDelay);
                //分布式锁
                var lockKey = $"{_options.TopicPrefix}ProcessNoAck:lock";
                await _distributedLock.Lock(lockKey, TimeSpan.FromMinutes(2), async () =>
                 {
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

                 }, () => { return Task.CompletedTask; });
            });

            return Task.CompletedTask;
        }

        private async Task<int> ProcessNoAckJob(string topic, RedisValue[] list)
        {
            int deleteCount = 0;
            for (var i = list.Length - 1; i >= 0; i--)
            {
                var jobId = list[i];
                //var values = await _database.HashGetAsync(GetJobHashId(jobId), new RedisValue[] { "CreateTime", "ExecuteTime" });
                //var createTime = DateUtils.ToDateTimeNullable(values[0]);
                //var executeTime = DateUtils.ToDateTimeNullable(values[1]);
                var value = await _database.HashGetAsync(GetJobHashId(jobId), "ExecuteTime");
                var executeTime = DateUtils.ToDateTimeNullable(value);
                if (executeTime != null)
                {//准备执行了，但是没有收到确认（执行失败或者没执行）
                    if (DateTime.Now - executeTime.Value > _noAckReEnqueueDelay)
                    {
                        await ReEnquene(topic, jobId);
                        deleteCount++;
                    }
                }
                else
                {//抓取到了，修改ExecuteTime时间出错了，没成功。
                    var createTime = DateUtils.ToDateTimeNullable(await _database.HashGetAsync(GetJobHashId(jobId), "CreateTime"));
                    if (createTime.HasValue && DateTime.Now - createTime.Value > _noAckReEnqueueDelay)
                    {
                        await Task.Delay(100);
                        executeTime = DateUtils.ToDateTimeNullable(await _database.HashGetAsync(GetJobHashId(jobId), "ExecuteTime"));
                        if (executeTime == null)
                        {
                            await ReEnquene(topic, jobId);
                            deleteCount++;
                        }
                    }
                }

            }

            return deleteCount;
        }

        /// <summary>
        /// 重新入队
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="jobId"></param>
        /// <returns></returns>
        private Task ReEnquene(string topic, string jobId)
        {
            var trans = _database.CreateTransaction();

            trans.ListRemoveAsync(GetProcessingQueueName(topic), jobId);
            trans.ListLeftPushAsync(topic, jobId);

            return trans.ExecuteAsync();
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
                            if (_options.ConsumerMode == ConsumerMode.AtMostOnce)
                            {
                                await ConsumerAtMostOnce(topic);
                            }
                            else
                            {
                                await ConsumerAtLeastOnce(topic);
                            }
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

        /// <summary>
        /// 至少一次
        /// </summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        private async Task ConsumerAtLeastOnce(string topic)
        {
            var processingQueue = GetProcessingQueueName(topic);
            string jobId = await _database.ListRightPopLeftPushAsync(topic, processingQueue);//加入备份队列，执行完进行移除
            if (string.IsNullOrEmpty(jobId))
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                return;
            }
            await _database.HashSetAsync(GetJobHashId(jobId), new HashEntry[] {
                     new HashEntry("ExecuteTime",DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"))
             });

            byte[] data = await _database.HashGetAsync(GetJobHashId(jobId), "Data");//取出数据字段
            if (data == null || data.Length == 0) return;
            //var obj = _options.Serializer.Deserialize<T>(data);
            await Handler(data);

            await CommitACK(topic, jobId);
        }

        /// <summary>
        /// 至多一次
        /// </summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        private async Task ConsumerAtMostOnce(string topic)
        {
            var processingQueue = GetProcessingQueueName(topic);
            string jobId = await _database.ListRightPopAsync(topic);
            if (string.IsNullOrEmpty(jobId))
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                return;
            }
            byte[] data = await _database.HashGetAsync(GetJobHashId(jobId), "Data");//取出数据字段
            if (data == null || data.Length == 0) return;
            //var obj = _options.Serializer.Deserialize<T>(data);
            await Handler(data);

            await _database.KeyDeleteAsync(GetJobHashId(jobId));
        }

        /// <summary>
        /// 手动移除备份队列数据
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="jobId"></param>
        /// <returns></returns>
        private Task CommitACK(string topic, string jobId)
        {
            var trans = _database.CreateTransaction();
            trans.ListRemoveAsync(GetProcessingQueueName(topic), jobId);
            trans.KeyDeleteAsync(GetJobHashId(jobId));

            return With.ReTry(_logger, () =>
            {
                return trans.ExecuteAsync();
            }, "redismessagebus CommitACK");
        }

        /// <summary>
        /// 执行消费事件
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        private async Task Handler(byte[] data)
        {
            if (OnMessage != null)
            {
                await With.NoException(_logger, async () =>
                {
                    await OnMessage(data);
                }, "redis消费失败");
            }
        }

        public void Close()
        {
            this._isStart = false;
            _logger.LogInformation("redis关闭消费者");
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
