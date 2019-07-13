using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.Redis2.BackgroundProcess
{
    /// <summary>
    /// 执行中队列，错误数据处理 重新入队
    /// </summary>
    public class ErrorWorkerProcess : IBackgroundProcess
    {
        private IServiceProvider _serviceProvider;
        public ErrorWorkerProcess(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        public Task Execute(BackgroundProcessContext context)
        {
            //这个context是个全局的，在订阅时把订阅的topic加入到context，这里就能处理到了，每次循环这些队列
            return Task.CompletedTask;
        }
    }
}
