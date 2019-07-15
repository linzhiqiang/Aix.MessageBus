using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.RabbitMQ
{
    public interface IRabbitMQConsumer : IDisposable
    {
        Task Subscribe(string topic, string groupId, CancellationToken cancellationToken);

        event Func<MessageBusData, Task> OnMessage;
        void Close();
    }
}
