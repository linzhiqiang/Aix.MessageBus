using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.RabbitMQ
{
    public interface IRabbitMQConsumer<T> : IDisposable
    {
        Task Subscribe(string topic, CancellationToken cancellationToken);

        event Func<T, Task> OnMessage;
        void Close();
    }
}
