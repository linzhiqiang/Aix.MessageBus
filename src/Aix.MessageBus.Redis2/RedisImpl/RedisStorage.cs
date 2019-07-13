using Aix.MessageBus.Redis2.Foundation;
using Aix.MessageBus.Redis2.Model;
using Aix.MessageBus.Utils;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.Redis2.RedisImpl
{
  public  class RedisStorage
    {
        private IServiceProvider _serviceProvider;
        private ConnectionMultiplexer _redis = null;
        private IDatabase _database;
        private RedisMessageBusOptions _options;
        private readonly RedisSubscription _queueJobChannelSubscription;
        private readonly RedisSubscription _delayJobChannelSubscription;
        public RedisStorage(IServiceProvider serviceProvider, ConnectionMultiplexer redis, RedisMessageBusOptions options)
        {
            _serviceProvider = serviceProvider;
            this._redis = redis;
            this._options = options;
            _database = redis.GetDatabase();
            _queueJobChannelSubscription = new RedisSubscription(_serviceProvider, _redis.GetSubscriber(), Helper.GetQueueJobChannel(_options));
            _delayJobChannelSubscription = new RedisSubscription(_serviceProvider, _redis.GetSubscriber(),Helper.GetDelayChannel(_options));

        }

        #region 生产者

        /// <summary>
        /// 添加即时任务
        /// </summary>
        /// <param name="job"></param>
        /// <param name="queue"></param>
        /// <returns></returns>
        public Task<bool> Enqueue(JobData jobData)
        {
            var values = jobData.ToDictionary();
            var topic = jobData.Topic;
            var hashJobId = Helper.GetJobHashId(_options, jobData.JobId);

            var trans = _database.CreateTransaction();

            trans.HashSetAsync(hashJobId, values.ToArray());
            trans.KeyExpireAsync(hashJobId, TimeSpan.FromDays(_options.DataExpireDay));
            trans.ListLeftPushAsync(topic, jobData.JobId);
            trans.PublishAsync(_queueJobChannelSubscription.Channel, jobData.JobId);
           
            var result = trans.Execute();

            return Task.FromResult(result);
        }

        /// <summary>
        /// 延时任务
        /// </summary>
        /// <param name="job"></param>
        /// <param name="timeSpan"></param>
        /// <returns></returns>
        public Task<bool> EnqueueDealy(JobData jobData, TimeSpan delay)
        {
            var values = jobData.ToDictionary();
            var hashJobId = Helper.GetJobHashId(_options, jobData.JobId);

            var trans = _database.CreateTransaction();

            trans.HashSetAsync(hashJobId, values.ToArray());
            trans.KeyExpireAsync(hashJobId, TimeSpan.FromDays(_options.DataExpireDay));
            trans.SortedSetAddAsync(Helper.GetDelaySortedSetName(_options), jobData.JobId, DateUtils.GetTimeStamp(DateTime.Now.AddMilliseconds(delay.TotalMilliseconds))); //当前时间戳，
            trans.PublishAsync(_delayJobChannelSubscription.Channel, jobData.JobId);

            var result = trans.Execute();

            return Task.FromResult(result);
        }

        #endregion
    }
}
