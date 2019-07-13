using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Aix.MessageBus.Redis.RedisImpl;
using Aix.MessageBus.Utils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Aix.MessageBus.Redis.BackgroundProcess
{
    /// <summary>
    /// 延迟任务处理
    /// </summary>
    public class DelayedWorkProcess : IBackgroundProcess
    {
        private IServiceProvider _serviceProvider;
        private ILogger<DelayedWorkProcess> _logger;
        private RedisMessageBusOptions _options;
        private RedisStorage _redisStorage;
        int BatchCount = 100; //一次拉取多少条
        private bool _isStart = true;
        public DelayedWorkProcess(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
            _logger = _serviceProvider.GetService<ILogger<DelayedWorkProcess>>();
            _options = _serviceProvider.GetService<RedisMessageBusOptions>();
            _redisStorage = _serviceProvider.GetService<RedisStorage>();
        }

        public void Dispose()
        {
            _isStart = false;
            _logger.LogInformation("关闭后台任务：redis延迟任务处理");
        }

        public async Task Execute(BackgroundProcessContext context)
        {
            var lockKey = $"{_options.TopicPrefix}delay:lock";
            int delay = 1000; //毫秒
            await _redisStorage.Lock(lockKey, TimeSpan.FromMinutes(1), async () =>
            {
                var now = DateTime.Now;
                var list = await _redisStorage.GetTopDueDealyJobId(DateUtils.GetTimeStamp(now), BatchCount);
                foreach (var item in list)
                {
                    if (_isStart == false) return;
                    var jobId = item.Key;
                    // 延时任务到期加入即时任务队列
                    await _redisStorage.DueDealyJobEnqueue(jobId);
                }

                if (list.Count > 0)
                {
                    delay = 0;
                }
            }, () => Task.CompletedTask);

            if (delay > 0)
            {
                await Task.Delay(delay);
            }
        }
    }
}
