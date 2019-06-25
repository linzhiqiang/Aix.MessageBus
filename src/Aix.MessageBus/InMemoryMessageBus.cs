﻿using Microsoft.Extensions.Logging;
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
    }

    public class InMemoryMessageBus : IMessageBus
    {
        private IServiceProvider _serviceProvider;
        private ILogger<InMemoryMessageBus> _logger;
        private InMemoryMessageBusOptions _options;

        private CancellationToken _cancellationToken;

        ConcurrentDictionary<string, List<InMemorySubscriberInfo>> _subscriberDict = new ConcurrentDictionary<string, List<InMemorySubscriberInfo>>();

        public InMemoryMessageBus(IServiceProvider serviceProvider, ILogger<InMemoryMessageBus> logger, InMemoryMessageBusOptions options)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _options = options;
        }

        public Task PublishAsync(Type messageType, object message)
        {
            string handlerKey = GetHandlerKey(messageType);
            if (_subscriberDict.TryGetValue(handlerKey, out List<InMemorySubscriberInfo> list))
            {
                foreach (var item in list)
                {
                    if (this._cancellationToken != null && this._cancellationToken.IsCancellationRequested)
                        continue;
                    Task.Run(() =>
                    {
                        if (this._cancellationToken != null && this._cancellationToken.IsCancellationRequested) return;
                        item.Action(message);
                    });
                }
            }
            return Task.CompletedTask;
        }

        public Task SubscribeAsync<T>(Func<T, Task> handler, MessageBusContext context, CancellationToken cancellationToken)
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