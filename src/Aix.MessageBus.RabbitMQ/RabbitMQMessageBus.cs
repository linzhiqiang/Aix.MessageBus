using System;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.RabbitMQ
{
    public class RabbitMQMessageBus : IMessageBus
    {
        public Task PublishAsync(Type messageType, object message)
        {
            throw new NotImplementedException();
        }

        public Task SubscribeAsync<T>(Func<T, Task> handler, MessageBusContext context = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
