using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.Foundation.EventLoop
{
    public interface IRunnable
    {
        Task Run();
    }

    public class TaskRunnable : IRunnable
    {
        Func<Task> _action;
        public TaskRunnable(Func<Task> action)
        {
            _action = action;

        }
        public Task Run()
        {
            return _action();
        }
    }
}
