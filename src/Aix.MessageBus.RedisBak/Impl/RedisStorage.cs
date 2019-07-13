using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using System.Threading.Tasks;
using Aix.MessageBus.Redis.Model;
using Aix.MessageBus.Utils;
using Aix.MessageBus.Redis.Foundation;
using System.Threading;

namespace Aix.MessageBus.Redis.Impl
{
    public class RedisStorage
    {
        private IServiceProvider _serviceProvider;
        private ILogger<RedisStorage> _logger;
        private RedisMessageBusOptions _options;

        ConnectionMultiplexer _connectionMultiplexer;
        IDatabase _database;

        private readonly RedisSubscription _queueJobChannelSubscription;
        private readonly RedisSubscription _delayJobChannelSubscription;
        public RedisStorage(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
            _logger = _serviceProvider.GetService<ILogger<RedisStorage>>();
            _options = _serviceProvider.GetService<RedisMessageBusOptions>();

            _connectionMultiplexer = serviceProvider.GetService<ConnectionMultiplexer>();
            _database = _connectionMultiplexer.GetDatabase();

            _queueJobChannelSubscription = new RedisSubscription(_serviceProvider, Helper.GetQueueJobChannel(_options));
            _delayJobChannelSubscription = new RedisSubscription(_serviceProvider, Helper.GetDelayChannel(_options));
        }

        /// <summary>
        /// 及时任务入队
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="jobData"></param>
        /// <returns></returns>
        public Task<bool> Enquene(string topic, JobData jobData)
        {
            var values = jobData.ToDictionary();
            var hashJobId = Helper.GetJobHashId(_options, jobData.JobId);

            var trans = _database.CreateTransaction();
            trans.HashSetAsync(hashJobId, values.ToArray());
            trans.KeyExpireAsync(hashJobId, TimeSpan.FromDays(_options.DataExpireDay));
            trans.ListLeftPushAsync(topic, jobData.JobId);
            trans.PublishAsync(_queueJobChannelSubscription.Channel, jobData.JobId);

            return With.ReTry<bool>(this._logger, () =>
            {
                return trans.ExecuteAsync();
            }, "redismessagebus ProduceAsync");

        }

            /// <summary>
            /// 插入延迟任务
            /// </summary>
            /// <param name="topic"></param>
            /// <param name="jobData"></param>
            /// <param name="delay"></param>
            /// <returns></returns>
            public Task<bool> EnqueneDelay(string topic, JobData jobData, TimeSpan delay)
        {
            var values = jobData.ToDictionary();
            var hashJobId = Helper.GetJobHashId(_options, jobData.JobId);

            var trans = _database.CreateTransaction();
            trans.HashSetAsync(hashJobId, values.ToArray());
            trans.KeyExpireAsync(hashJobId, TimeSpan.FromDays(_options.DataExpireDay));
            trans.SortedSetAddAsync(Helper.GetDelaySortedSetName(_options), jobData.JobId, DateUtils.GetTimeStamp(DateTime.Now.AddMilliseconds(delay.TotalMilliseconds))); //当前时间戳，

            trans.PublishAsync(_delayJobChannelSubscription.Channel, jobData.JobId);
            return With.ReTry<bool>(this._logger, () =>
            {
                return trans.ExecuteAsync();
            }, "redismessagebus ProduceAsync");
        }

        /// <summary>
        /// 查询到期的延迟任务
        /// </summary>
        /// <param name="timeStamp"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Task<IDictionary<string, long>> GetTopDueDealyJobId(long timeStamp, int count)
        {
            var nowTimeStamp = timeStamp;
            var result = _database.SortedSetRangeByScoreWithScores(Helper.GetDelaySortedSetName(_options), double.NegativeInfinity, nowTimeStamp, Exclude.None, Order.Ascending, 0, count);
            IDictionary<string, long> dict = new Dictionary<string, long>();
            foreach (SortedSetEntry item in result)
            {
                dict.Add(item.Element, (long)item.Score);
            }
            return Task.FromResult(dict);
        }

        /// <summary>
        /// 到期的延迟任务 插入及时任务
        /// </summary>
        /// <param name="jobId"></param>
        /// <returns></returns>
        public async Task DueDealyJobEnqueue(string jobId)
        {
            string topic = await _database.HashGetAsync(Helper.GetJobHashId(_options, jobId), nameof(JobData.Topic));

            var trans = _database.CreateTransaction();

#pragma warning disable CS4014
            trans.ListLeftPushAsync(topic, jobId);
            trans.SortedSetRemoveAsync(Helper.GetDelaySortedSetName(_options), jobId);
            trans.PublishAsync(_queueJobChannelSubscription.Channel, jobId);
#pragma warning restore CS4014 // 由于此调用不会等待，因此在调用完成前将继续执行当前方法

            await trans.ExecuteAsync();

        }

        /// <summary>
        /// 及时任务执行成功
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="jobId"></param>
        /// <returns></returns>
        public Task SetSuccess(string topic, string jobId)
        {
            var trans = _database.CreateTransaction();
            trans.ListRemoveAsync(Helper.GetProcessingQueueName(topic), jobId);
            trans.KeyDeleteAsync(Helper.GetJobHashId(_options, jobId));

            return With.ReTry(_logger, () =>
            {
                return trans.ExecuteAsync();
            }, "redismessagebus CommitACK");
        }

        /// <summary>
        /// 及时任务设置失败
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="jobId"></param>
        /// <returns></returns>
        public Task SetFail(string topic, string jobId)
        {
            var trans = _database.CreateTransaction();
            trans.HashIncrementAsync(Helper.GetJobHashId(_options, jobId), nameof(JobData.ErrorCount), 1);
            trans.HashSetAsync(Helper.GetJobHashId(_options, jobId), new HashEntry[] {
                     new HashEntry(nameof(JobData.Status),9) //0 待执行，1 执行中，2 成功，9 失败
             });
            return With.ReTry(_logger, () =>
            {
                return trans.ExecuteAsync();
            }, "redismessagebus SetFail");
        }



        /// <summary>
        /// 及时任务 重新入队
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="jobId"></param>
        /// <returns></returns>
        public Task ReEnquene(string topic, string jobId)
        {
            var trans = _database.CreateTransaction();

            trans.ListRemoveAsync(Helper.GetProcessingQueueName(topic), jobId);
            trans.ListLeftPushAsync(topic, jobId);
            trans.PublishAsync(_queueJobChannelSubscription.Channel, jobId);

            return trans.ExecuteAsync();
        }

        public Task ErrorReEnqueneDelay(string topic, string jobId,TimeSpan delay)
        {
            var trans = _database.CreateTransaction();

            trans.ListRemoveAsync(Helper.GetProcessingQueueName(topic), jobId);
            trans.SortedSetAddAsync(Helper.GetDelaySortedSetName(_options), jobId, DateUtils.GetTimeStamp(DateTime.Now.AddMilliseconds(delay.TotalMilliseconds))); //当前时间戳，

            trans.PublishAsync(_delayJobChannelSubscription.Channel, jobId);

            return trans.ExecuteAsync();
        }



        public void WaitForJob(TimeSpan timeSpan, CancellationToken cancellationToken)
        {
            _queueJobChannelSubscription.WaitForJob(timeSpan, cancellationToken);
        }

        public void WaitForDelayJob(TimeSpan timeSpan, CancellationToken cancellationToken)
        {
            _delayJobChannelSubscription.WaitForJob(timeSpan, cancellationToken);
        }
    }
}
