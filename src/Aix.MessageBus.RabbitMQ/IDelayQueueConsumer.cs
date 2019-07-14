using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.RabbitMQ
{
    public interface IDelayQueueConsumer : IDisposable
    {
        Task Subscribe();

        void Close();
    }
}
