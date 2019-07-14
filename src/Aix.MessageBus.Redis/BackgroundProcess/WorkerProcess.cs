using Aix.MessageBus.Redis.Model;
using Aix.MessageBus.Redis.RedisImpl;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Aix.MessageBus.Redis.BackgroundProcess
{
    /// <summary>
    /// 及时任务执行
    /// </summary>
    public class WorkerProcess : IBackgroundProcess
    {
        private IServiceProvider _serviceProvider;
        private ILogger<WorkerProcess> _logger;
        private RedisStorage _redisStorage;
        private string _topic;
        //private volatile bool _isStart = true;

        Func<MessageResult, Task<bool>> _messageHandler;
        public WorkerProcess(IServiceProvider serviceProvider, string topic, Func<MessageResult, Task<bool>> messageHandler)
        {
            _serviceProvider = serviceProvider;
            _logger = _serviceProvider.GetService<ILogger<WorkerProcess>>();
            _topic = topic;
            _messageHandler = messageHandler;

            _redisStorage = _serviceProvider.GetService<RedisStorage>();

        }

        public void Dispose()
        {
            //_isStart = false;
            _logger.LogInformation("关闭后台任务：redis即时任务处理");
        }
        public async Task Execute(BackgroundProcessContext context)
        {
            var jobData = await _redisStorage.FetchNextJob(this._topic);
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
            return await _messageHandler(messageResult);
        }
    }
}
