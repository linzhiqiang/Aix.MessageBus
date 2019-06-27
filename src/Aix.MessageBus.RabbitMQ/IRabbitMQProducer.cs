using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.RabbitMQ
{
    public interface IRabbitMQProducer : IDisposable
    {
        Task<bool> ProduceAsync(string topic, byte[] data);
    }
}
