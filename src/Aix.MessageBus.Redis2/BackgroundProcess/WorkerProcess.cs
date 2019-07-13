using Aix.MessageBus.Redis2.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.Redis2.BackgroundProcess
{
    /// <summary>
    /// 及时任务执行
    /// </summary>
    public class WorkerProcess : IBackgroundProcess
    {
        private IServiceProvider _serviceProvider;
        public WorkerProcess(IServiceProvider serviceProvider,string topic,Func<MessageResult, Task<bool>> dataHandler)
        {
            _serviceProvider = serviceProvider;
        }
        public Task Execute(BackgroundProcessContext context)
        {
            return Task.CompletedTask;
        }
    }
}
