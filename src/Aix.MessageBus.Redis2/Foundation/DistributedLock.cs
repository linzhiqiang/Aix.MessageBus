using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.Redis2.Foundation
{
    /// <summary>
    /// 分布式锁
    /// </summary>
    public class DistributedLock
    {
        private readonly IDatabase _redisDb;
        public DistributedLock(IConnectionMultiplexer connectionMultiplexer)
        {
            _redisDb = connectionMultiplexer.GetDatabase();
        }


        public async Task Lock(string key, TimeSpan span, Func<Task> action, Func<Task> concurrentCallback = null)
        {
            string token = Guid.NewGuid().ToString();
            if (LockTake(key, token, span))
            {
                try
                {
                    await action();
                }
                catch
                {
                    throw;
                }
                finally
                {
                    LockRelease(key, token);
                }
            }
            else
            {
                if (concurrentCallback != null) await concurrentCallback();
                else throw new Exception($"出现并发key:{key}");
            }
        }


        #region private

        /// <summary>
        /// 获取一个锁
        /// </summary>
        /// <param name="key"></param>
        /// <param name="token"></param>
        /// <param name="expiry"></param>
        /// <returns></returns>
        private bool LockTake(string key, string token, TimeSpan expiry)
        {
            return _redisDb.LockTake(key, token, expiry);
        }

        /// <summary>
        /// 释放一个锁(需要token匹配)
        /// </summary>
        /// <param name="key"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        private bool LockRelease(string key, string token)
        {
            return _redisDb.LockRelease(key, token);
        }

        #endregion
    }
}
