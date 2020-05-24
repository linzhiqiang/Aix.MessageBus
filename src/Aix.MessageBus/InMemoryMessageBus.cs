using Aix.MessageBus.Foundation.EventLoop;
using Aix.MessageBus.Utils;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus
{
    public class InMemoryMessageBusOptions
    {
        public InMemoryMessageBusOptions()
        {
            ConsumerThreadCount = 4;
        }
        /// <summary>
        /// 默认消费线程数 默认4个
        /// </summary>
        public int ConsumerThreadCount { get; set; }
    }
    public class InMemoryMessageBus : IMessageBus
    {
        private IServiceProvider _serviceProvider;
        private ILogger<InMemoryMessageBus> _logger;

        private CancellationToken _cancellationToken;
        InMemoryMessageBusOptions _options;

        ConcurrentDictionary<string, List<InMemorySubscriberInfo>> _subscriberDict = new ConcurrentDictionary<string, List<InMemorySubscriberInfo>>();
        MultithreadEventLoopGroup Eventloop;

        public InMemoryMessageBus(IServiceProvider serviceProvider, ILogger<InMemoryMessageBus> logger, InMemoryMessageBusOptions options)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _options = options;
            Eventloop = new MultithreadEventLoopGroup(_options.ConsumerThreadCount);
            Eventloop.Start();
        }

        public async Task PublishAsync(Type messageType, object message)
        {
            await this.PublishDelayAsync(messageType, message, TimeSpan.Zero);
        }

        public async Task PublishDelayAsync(Type messageType, object message, TimeSpan delay)
        {
            AssertUtils.IsNotNull(message, "消息不能null");
            string handlerKey = GetHandlerKey(messageType);
            if (_subscriberDict.TryGetValue(handlerKey, out List<InMemorySubscriberInfo> list))
            {
                foreach (var item in list)
                {
                    if (this._cancellationToken != null && this._cancellationToken.IsCancellationRequested)
                        continue;

                    Func<Task> task = async () =>
                   {
                       if (this._cancellationToken != null && this._cancellationToken.IsCancellationRequested) return;
                       await With.NoException(_logger, () =>
                       {
                           return item.Action(message);
                       }, $"执行出错,{item.Type.FullName}");

                   };
                    if (delay <= TimeSpan.Zero)
                    {
                        Eventloop.GetNext().Execute(task);
                    }
                    else
                    {
                        Eventloop.GetNext().Schedule(task, delay);
                    }
                }
            }
            await Task.CompletedTask;
        }

        public Task SubscribeAsync<T>(Func<T, Task> handler, MessageBusContext context = null, CancellationToken cancellationToken = default(CancellationToken)) where T : class
        {
            string handlerKey = GetHandlerKey(typeof(T));
            var subscriber = new InMemorySubscriberInfo
            {
                Type = typeof(T),
                Action = (message) =>
                {
                    return handler((T)message);
                }
            };
            lock (typeof(T))
            {
                if (_subscriberDict.ContainsKey(handlerKey))
                {
                    _subscriberDict[handlerKey].Add(subscriber);
                }
                else
                {
                    _subscriberDict.TryAdd(handlerKey, new List<InMemorySubscriberInfo> { subscriber });
                }
            }

            return Task.CompletedTask;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            this._cancellationToken = cancellationToken;
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            Eventloop.Stop();
        }

        #region private 

        private string GetHandlerKey(Type type)
        {
            return type.FullName;
        }

        #endregion
    }
}
