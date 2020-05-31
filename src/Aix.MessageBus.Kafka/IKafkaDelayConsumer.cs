using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.Kafka
{
   public interface IKafkaDelayConsumer : IDisposable
    {
        Task Subscribe(CancellationToken cancellationToken);

        public void Close();
    }
}
