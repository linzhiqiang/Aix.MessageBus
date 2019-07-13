using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.Redis2.BackgroundProcess
{
    /// <summary>
    /// 延迟任务处理
    /// </summary>
    public class DelayedWorkProcess : IBackgroundProcess
    {
        private IServiceProvider _serviceProvider;
        public DelayedWorkProcess(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        public Task Execute(BackgroundProcessContext context)
        {
            return Task.CompletedTask;
        }
    }
}
