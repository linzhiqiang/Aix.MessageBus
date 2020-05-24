using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.Foundation.EventLoop
{
    public class MultithreadEventLoopGroup : IDisposable
    {
        static readonly int DefaultEventLoopThreadCount = Environment.ProcessorCount * 2;
        static Func<SingleThreadEventExecutor> DefaultExecutorFactory = () => new SingleThreadEventExecutor();
        readonly IEventExecutor[] EventLoops;
        int requestId;

        public MultithreadEventLoopGroup() : this(DefaultEventLoopThreadCount)
        {

        }
        public MultithreadEventLoopGroup(int threadCount)
        {
            threadCount = threadCount > 0 ? threadCount : DefaultEventLoopThreadCount;
            this.EventLoops = new IEventExecutor[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                var eventLoop = DefaultExecutorFactory();
                this.EventLoops[i] = eventLoop;
                eventLoop.OnException += EventLoop_OnException;
            }
        }

        public IEventExecutor GetNext()
        {
            int id = Interlocked.Increment(ref this.requestId);
            return this.EventLoops[Math.Abs(id % this.EventLoops.Length)];
        }

        private async Task EventLoop_OnException(Exception ex)
        {
            if (OnException != null) await OnException(ex);
        }

        public event Func<Exception, Task> OnException;

        public void Start()
        {
            foreach (var item in this.EventLoops)
            {
                item.Start();
            }
        }

        public void Stop()
        {
            foreach (var item in this.EventLoops)
            {
                item.Stop();
            }
        }

        public void Dispose()
        {
            this.Stop();
        }

    }
}
