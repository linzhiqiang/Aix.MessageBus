using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.Redis
{
    public interface IRedisConsumer<T> : IDisposable
    {
        Task Subscribe(string topic, CancellationToken cancellationToken);

        event Func<byte[], Task> OnMessage;
        void Close();
    }
}
