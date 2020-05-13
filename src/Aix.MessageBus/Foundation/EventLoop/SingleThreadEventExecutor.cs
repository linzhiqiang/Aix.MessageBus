using Aix.MessageBus.Utils;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.Foundation.EventLoop
{
    /// <summary>
    /// 单线程事件循环
    /// </summary>
    public class SingleThreadEventExecutor : IEventExecutor
    {
        IBlockingQueue<IRunnable> _taskQueue = QueueFactory.Instance.CreateBlockingQueue<IRunnable>();
        protected readonly PriorityQueue<IScheduledRunnable> ScheduledTaskQueue = new PriorityQueue<IScheduledRunnable>();
        volatile bool _isStart = false;

        private void StartRunTask()
        {
            Task.Factory.StartNew(async () =>
            {
                while (true)
                {
                    try
                    {
                        await RunTasks();
                    }
                    catch (Exception ex)
                    {
                        await handlerException(ex);
                    }
                }
            }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        private async Task RunTasks()
        {
            var item = _taskQueue.Dequeue();
            if (item != null)
            {
                await item.Run();
            }
        }

        private void StartRunDelayTask()
        {
            Task.Factory.StartNew(async () =>
            {
                while (true)
                {
                    try
                    {
                        RunDelayTask();
                    }
                    catch (Exception ex)
                    {
                        await handlerException(ex);
                    }
                }
            }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        private void RunDelayTask()
        {
            lock (ScheduledTaskQueue)
            {
                IScheduledRunnable nextScheduledTask = this.ScheduledTaskQueue.Peek();
                if (nextScheduledTask != null)
                {
                    var tempDelay = nextScheduledTask.TimeStamp - DateUtils.GetTimeStamp();
                    if (tempDelay > 0)
                    {
                        Monitor.Wait(ScheduledTaskQueue, (int)tempDelay);
                    }
                    else
                    {
                        this.ScheduledTaskQueue.Dequeue();
                        Execute(nextScheduledTask);
                    }
                }
                else
                {
                    Monitor.Wait(ScheduledTaskQueue);
                }
            }

        }

        private async Task handlerException(Exception ex)
        {
            if (OnException != null)
            {
                await OnException(ex);
            }
        }

        #region IEventExecutor

        public event Func<Exception, Task> OnException;

        public void Execute(IRunnable task)
        {
            _taskQueue.Enqueue(task);
        }

        public void Execute(Func<Task> action)
        {
            Execute(new TaskRunnable(action));
        }

        private void Schedule(IScheduledRunnable task)
        {
            this.Execute(() =>
            {
                lock (ScheduledTaskQueue)
                {
                    this.ScheduledTaskQueue.Enqueue(task);
                    Monitor.Pulse(ScheduledTaskQueue);
                }
                return Task.CompletedTask;
            });
        }
        public void Schedule(IRunnable action, TimeSpan delay)
        {
            if (delay <= TimeSpan.Zero)
            {
                Execute(action);
                return;
            }
            Schedule(new ScheduledRunnable(action, DateUtils.GetTimeStamp(DateTime.Now.Add(delay))));
        }

        public void Schedule(Func<Task> action, TimeSpan delay)
        {
            Schedule(new TaskRunnable(action), delay);
        }

        public void Start()
        {
            if (_isStart) return;
            _isStart = true;

            Task.Run(() =>
            {
                StartRunTask();
                StartRunDelayTask();
            });
        }

        public void Stop()
        {
            if (this._isStart == false) return;
            lock (this)
            {
                if (this._isStart)
                {
                    this._isStart = false;
                }
            }

        }

        public void Dispose()
        {
            this.Stop();
        }
        #endregion
    }
}
