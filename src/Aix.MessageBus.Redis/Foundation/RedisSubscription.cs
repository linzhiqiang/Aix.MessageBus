using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;

namespace Aix.MessageBus.Redis.Foundation
{
    internal class RedisSubscription
    {
        private IServiceProvider _serviceProvider;
        private ILogger<RedisSubscription> _logger;
        private RedisMessageBusOptions _options;

        ConnectionMultiplexer _connectionMultiplexer;
        private readonly ManualResetEvent _mre = new ManualResetEvent(false);
        private readonly ISubscriber _subscriber;

        public RedisSubscription(IServiceProvider serviceProvider, string subscriberChannel)
        {
            _serviceProvider = serviceProvider;

            _logger = _serviceProvider.GetService<ILogger<RedisSubscription>>();
            _options = _serviceProvider.GetService<RedisMessageBusOptions>();
            _connectionMultiplexer = _serviceProvider.GetService<ConnectionMultiplexer>();

            _subscriber = _connectionMultiplexer.GetSubscriber();
            Channel = subscriberChannel;
            _subscriber.Subscribe(Channel, (channel, value) =>
            {
                _mre.Set();
            });
        }

        public string Channel { get; }

        public void WaitForJob(TimeSpan timeout, CancellationToken cancellationToken)
        {
            _mre.Reset();
            WaitHandle.WaitAny(new[] { _mre, cancellationToken.WaitHandle }, timeout);

        }
    }
}
