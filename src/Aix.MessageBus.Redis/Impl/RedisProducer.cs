using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Aix.MessageBus.Redis.Model;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Aix.MessageBus.Utils;

namespace Aix.MessageBus.Redis.Impl
{
    public class RedisProducer : IRedisProducer
    {
        private IServiceProvider _serviceProvider;
        private ILogger<RedisProducer> _logger;
        private RedisMessageBusOptions _options;

        RedisStorage _redisStorage;

        public RedisProducer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _logger = _serviceProvider.GetService<ILogger<RedisProducer>>();
            _options = _serviceProvider.GetService<RedisMessageBusOptions>();

            _redisStorage = _serviceProvider.GetService<RedisStorage>();
        }

        public void Dispose()
        {
            _logger.LogInformation("redis关闭生产者");
        }

        public Task<bool> ProduceAsync(string topic, JobData jobData)
        {
            return _redisStorage.Enquene(topic, jobData);

        }

        public Task<bool> ProduceAsync(string topic, JobData jobData, TimeSpan delay)
        {
            return _redisStorage.EnqueneDelay(topic, jobData, delay);
        }
    }
}
