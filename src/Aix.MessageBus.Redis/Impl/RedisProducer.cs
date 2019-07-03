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

        ConnectionMultiplexer _connectionMultiplexer;
        IDatabase _database;

        public RedisProducer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _logger = _serviceProvider.GetService<ILogger<RedisProducer>>();
            _options = _serviceProvider.GetService<RedisMessageBusOptions>();

            _connectionMultiplexer = _serviceProvider.GetService<ConnectionMultiplexer>();
            _database = _connectionMultiplexer.GetDatabase();
        }

        public void Dispose()
        {
            _logger.LogInformation("redis关闭生产者");
        }

        public Task<bool> ProduceAsync(string topic, JobData jobData)
        {
            var values = jobData.ToDictionary();
            var hashJobId = Helper.GetJobHashId(_options,jobData.JobId);

            var trans = _database.CreateTransaction();
            trans.HashSetAsync(hashJobId, values.ToArray());
            trans.KeyExpireAsync(hashJobId, TimeSpan.FromDays(_options.DataExpireDay));
            trans.ListLeftPushAsync(topic, jobData.JobId);

            return With.ReTry<bool>(this._logger, () =>
            {
                return trans.ExecuteAsync();
            }, "redismessagebus ProduceAsync");

        }

        public Task<bool> ProduceAsync(string topic, JobData jobData, TimeSpan delay)
        {
            var values = jobData.ToDictionary();
            var hashJobId = Helper.GetJobHashId(_options, jobData.JobId);

            var trans = _database.CreateTransaction();
            trans.HashSetAsync(hashJobId, values.ToArray());
            trans.KeyExpireAsync(hashJobId, TimeSpan.FromDays(_options.DataExpireDay));
            trans.SortedSetAddAsync(Helper.GetDelaySortedSetName(_options), jobData.JobId, DateUtils.GetTimeStamp(DateTime.Now.AddMilliseconds(delay.TotalMilliseconds))); //当前时间戳，

            return With.ReTry<bool>(this._logger, () =>
            {
                return trans.ExecuteAsync();
            }, "redismessagebus ProduceAsync");
        }
    }
}
