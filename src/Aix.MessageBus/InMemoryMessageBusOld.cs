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



    public class InMemoryMessageBusOld : IMessageBus
    {
        private IServiceProvider _serviceProvider;
        private ILogger<InMemoryMessageBusOld> _logger;

        private CancellationToken _cancellationToken;

        ConcurrentDictionary<string, List<InMemorySubscriberInfo>> _subscriberDict = new ConcurrentDictionary<string, List<InMemorySubscriberInfo>>();

        public InMemoryMessageBusOld(IServiceProvider serviceProvider, ILogger<InMemoryMessageBusOld> logger)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
        }

        public Task PublishAsync(Type messageType, object message)
        {
            AssertUtils.IsNotNull(message, "消息不能null");
            string handlerKey = GetHandlerKey(messageType);
            if (_subscriberDict.TryGetValue(handlerKey, out List<InMemorySubscriberInfo> list))
            {
                foreach (var item in list)
                {
                    if (this._cancellationToken != null && this._cancellationToken.IsCancellationRequested)
                        continue;
                    Task.Run(async () =>
                    {
                        if (this._cancellationToken != null && this._cancellationToken.IsCancellationRequested) return;
                        await With.NoException(_logger, () =>
                        {
                            return item.Action(message);
                        }, $"执行出错,{item.Type.FullName}");

                    });
                }
            }
            return Task.CompletedTask;
        }

        public async Task PublishDelayAsync(Type messageType, object message, TimeSpan delay)
        {
            AssertUtils.IsNotNull(message, "消息不能null");
            await Task.Delay(delay);
            await this.PublishAsync(messageType, message);
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
        }

        #region private 

        private string GetHandlerKey(Type type)
        {
            return type.FullName;
        }

        #endregion
    }

    public class InMemorySubscriberInfo
    {
        public Type Type { get; set; }
        public Func<object, Task> Action { get; set; }
    }
}
