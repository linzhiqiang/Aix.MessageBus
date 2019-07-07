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
using System.Threading;

namespace Aix.MessageBus.Redis.Impl
{
    public class DelayTaskConsumer : IDisposable
    {
        private IServiceProvider _serviceProvider;
        private ILogger<DelayTaskConsumer> _logger;
        private RedisMessageBusOptions _options;
        ConnectionMultiplexer _connectionMultiplexer;

        private volatile bool _isStart = false;
        DistributedLock _distributedLock;
        RedisStorage _redisStorage;

        public DelayTaskConsumer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _logger = serviceProvider.GetService<ILogger<DelayTaskConsumer>>();
            _options = serviceProvider.GetService<RedisMessageBusOptions>();
            _connectionMultiplexer = serviceProvider.GetService<ConnectionMultiplexer>();
            _distributedLock = new DistributedLock(_connectionMultiplexer);
            _redisStorage = serviceProvider.GetService<RedisStorage>();
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

        int BatchCount = 100;
        public async Task Execute()
        {
            var lockKey = $"{_options.TopicPrefix}delay:lock";
            int delay = 1000; //毫秒
            await _distributedLock.Lock(lockKey, TimeSpan.FromMinutes(1), async () =>
            {
                var now = DateTime.Now;
                var list = await _redisStorage.GetTopDueDealyJobId(DateUtils.GetTimeStamp(now), BatchCount);
                foreach (var item in list)
                {
                    if (!_isStart) break;
                    // 延时任务到期加入即时任务队列
                    await _redisStorage.DueDealyJobEnqueue(item.Key);
                }

                if (list.Count >= BatchCount) //还有数据时不等待 
                {
                    delay = 0;
                }
            }, () => Task.CompletedTask);

            if (delay > 0)
            {
                _redisStorage.WaitForDelayJob(TimeSpan.FromSeconds(1),default(CancellationToken));
            }
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
