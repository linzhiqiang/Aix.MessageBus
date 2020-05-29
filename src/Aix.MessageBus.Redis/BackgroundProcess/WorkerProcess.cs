using Aix.MessageBus.Redis.Model;
using Aix.MessageBus.Redis.RedisImpl;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Aix.MessageBus.Exceptions;

namespace Aix.MessageBus.Redis.BackgroundProcess
{
    /// <summary>
    /// 及时任务执行
    /// </summary>
    internal class WorkerProcess : IBackgroundProcess
    {
        private IServiceProvider _serviceProvider;
        private ILogger<WorkerProcess> _logger;
        private RedisStorage _redisStorage;
        private string _topic;

        public event Func<MessageResult, Task> OnMessage;
        public WorkerProcess(IServiceProvider serviceProvider, string topic)
        {
            _serviceProvider = serviceProvider;
            _logger = _serviceProvider.GetService<ILogger<WorkerProcess>>();
            _topic = topic;
            _redisStorage = _serviceProvider.GetService<RedisStorage>();

        }

        public void Dispose()
        {
            _logger.LogInformation("关闭后台任务：redis即时任务处理");
        }
        public async Task Execute(BackgroundProcessContext context)
        {
            FetchJobData jobData = null;
            try
            {
                jobData = await _redisStorage.FetchNextJob(this._topic);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "redis获取任务失败");
                await Task.Delay(TimeSpan.FromSeconds(10));
            }

            if (jobData == null)
            {
                _redisStorage.WaitForJob(TimeSpan.FromSeconds(1));
                return;
            }

            var isSuccess = await DoWork(jobData);
            if (isSuccess)
            {
                await _redisStorage.SetSuccess(jobData.Topic, jobData.JobId);
            }
            else
            {
                await _redisStorage.SetFail(jobData.Topic, jobData.JobId);
            }
        }

        private async Task<bool> DoWork(FetchJobData fetchJobData)
        {
            MessageResult messageResult = new MessageResult
            {
                Data = fetchJobData.Data,
                Topic = fetchJobData.Topic,
                JobId = fetchJobData.JobId
            };
            var isSuccess = true;
            try
            {
                await OnMessage(messageResult);
            }
            catch (RetryException ex)
            {
                _logger.LogError(ex, $"redis消费失败重试,topic:{fetchJobData.Topic}");
                isSuccess = false;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"redis消费失败,topic:{fetchJobData.Topic}");
            }
            return isSuccess;
        }
    }
}
