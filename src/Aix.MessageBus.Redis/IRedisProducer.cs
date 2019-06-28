using Aix.MessageBus.Redis.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.Redis
{
    public interface IRedisProducer : IDisposable
    {
        Task<bool> ProduceAsync(string topic, JobData jobData);
    }
}
