using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Aix.MessageBus.Redis.Model;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Aix.MessageBus.Redis.Impl
{
    public class RedisProducer : IRedisProducer
    {
        private IServiceProvider _serviceProvider;
        private ILogger<RedisProducer> _logger;
        private RedisMessageBusOptions _options;

        ConnectionMultiplexer _connectionMultiplexer;
        IDatabase _database;

        public RedisProducer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _logger = serviceProvider.GetService<ILogger<RedisProducer>>();
            _options = serviceProvider.GetService<RedisMessageBusOptions>();

            _connectionMultiplexer = serviceProvider.GetService<ConnectionMultiplexer>();
            _database = _connectionMultiplexer.GetDatabase();
        }

        public void Dispose()
        {

        }

        public Task<bool> ProduceAsync(string topic, JobData jobData)
        {
            //1加入hashset
            //插入list
            var values = jobData.ToDictionary();
            var hashJobId = GetJobHashId(jobData.JobId);

            var trans = _database.CreateTransaction();
            trans.HashSetAsync(hashJobId, values.ToArray());
            trans.KeyExpireAsync(hashJobId, TimeSpan.FromDays(7));
            trans.ListLeftPushAsync(topic, jobData.JobId);
            return trans.ExecuteAsync();
        }

        private string GetJobHashId(string jobId)
        {
            //return GetRedisKey("jobdata" + $":job:{jobId}");

            return $"{_options.TopicPrefix ?? ""}jobdata:{jobId}";
        }
    }
}
