using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.RabbitMQ
{
    public interface IRabbitMQProducer : IDisposable
    {
        bool ProduceAsync(string topic, byte[] data);

        bool ProduceDelayAsync(string topic, byte[] data,TimeSpan delay);

        bool ErrorReProduceAsync(string topic, string groupId, byte[] data);
    }
}
