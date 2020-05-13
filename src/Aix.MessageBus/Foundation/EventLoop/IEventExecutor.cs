using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.Foundation.EventLoop
{
    public interface IEventExecutor : IDisposable
    {
        void Execute(IRunnable task);

        void Execute(Func<Task> action);

        void Schedule(IRunnable action, TimeSpan delay);

        void Schedule(Func<Task> action, TimeSpan delay);

        void Start();

        void Stop();

        event Func<Exception, Task> OnException;
    }
}
