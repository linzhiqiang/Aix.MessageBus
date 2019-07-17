using Aix.MessageBus.Kafka.Model;
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.Kafka
{
    internal interface IKafkaProducer : IDisposable
    {
        Task<DeliveryResult<Null, KafkaMessageBusData>> ProduceAsync(KafkaMessageBusData data);

        Task<DeliveryResult<Null, KafkaMessageBusData>> ProduceDelayAsync(KafkaMessageBusData data, TimeSpan delay);
    }
}
