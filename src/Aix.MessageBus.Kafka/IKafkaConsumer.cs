using Aix.MessageBus.Kafka.Model;
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.Kafka
{
    internal interface IKafkaConsumer<TKey> : IDisposable
    {
        Task Subscribe(string topic, string groupId, CancellationToken cancellationToken);

        event Func<ConsumeResult<TKey, KafkaMessageBusData>, Task> OnMessage;
        void Close();

    }
}
