using Aix.MessageBus.Redis.Foundation;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Aix.MessageBus.Utils;
using Aix.MessageBus.Redis.Model;

namespace Aix.MessageBus.Redis.Impl
{
    public class DelayTaskConsumer : IDisposable
    {
        private IServiceProvider _serviceProvider;
        private ILogger<DelayTaskConsumer> _logger;
        private RedisMessageBusOptions _options;

        private volatile bool _isStart = false;
        ConnectionMultiplexer _connectionMultiplexer;
        IDatabase _database;
        DistributedLock _distributedLock;

        public DelayTaskConsumer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _logger = serviceProvider.GetService<ILogger<DelayTaskConsumer>>();
            _options = serviceProvider.GetService<RedisMessageBusOptions>();

            _connectionMultiplexer = serviceProvider.GetService<ConnectionMultiplexer>();
            _database = _connectionMultiplexer.GetDatabase();
            _distributedLock = new DistributedLock(_connectionMultiplexer);
        }

        public Task Start()
        {
            if (_isStart) return Task.CompletedTask;
            lock (this)
            {
                if (_isStart) return Task.CompletedTask;
                _isStart = true;
            }


            Task.Run(async () =>
            {
                while (_isStart)
                {
                    try
                    {
                        await Execute();
                    }
                    catch (Exception ex)
                    {
                        string errorMsg = $"执行DelayTaskConsumer异常：{ex.Message},{ex.StackTrace}";
                        _logger.LogError(errorMsg);
                    }

                }
            });
            return Task.CompletedTask;
        }

        int BatchCount = 50;
        public async Task Execute()
        {
            var lockKey = $"{_options.TopicPrefix}delay:lock";
            int delay = 1000; //毫秒
            await _distributedLock.Lock(lockKey, TimeSpan.FromMinutes(1), async () =>
            {
                var now = DateTime.Now;
                var maxScore = DateUtils.GetTimeStamp(now);
                var list = await GetTopDueDealyJobId(DateUtils.GetTimeStamp(now), BatchCount);
                foreach (var item in list)
                {
                    if (!_isStart) break;
                    // 延时任务到期加入即时任务队列
                    await DueDealyJobEnqueue(item.Key);
                }

                if (list.Count >= BatchCount) //还有数据时不等待 
                {
                    delay = 0;
                }
            }, () => Task.CompletedTask);

            if (delay > 0)
            {
                await Task.Delay(delay);
            }
        }
        public Task<IDictionary<string, long>> GetTopDueDealyJobId(long timeStamp, int count)
        {
            var nowTimeStamp = timeStamp;
            var result = _database.SortedSetRangeByScoreWithScores(Helper.GetDelaySortedSetName(_options), double.NegativeInfinity, nowTimeStamp, Exclude.None, Order.Ascending, 0, count);
            IDictionary<string, long> dict = new Dictionary<string, long>();
            foreach (SortedSetEntry item in result)
            {
                dict.Add(item.Element, (long)item.Score);
            }
            return Task.FromResult(dict);
        }

        public async Task DueDealyJobEnqueue(string jobId)
        {
            string topic = await _database.HashGetAsync(Helper.GetJobHashId(_options, jobId), nameof(JobData.Topic));

            var trans = _database.CreateTransaction();

#pragma warning disable CS4014
            trans.ListLeftPushAsync(topic, jobId);
            trans.SortedSetRemoveAsync(Helper.GetDelaySortedSetName(_options), jobId);
#pragma warning restore CS4014 // 由于此调用不会等待，因此在调用完成前将继续执行当前方法

            await trans.ExecuteAsync();

        }
        public void Close()
        {
            _isStart = false;
        }
        public void Dispose()
        {
            this.Close();
        }
    }
}
