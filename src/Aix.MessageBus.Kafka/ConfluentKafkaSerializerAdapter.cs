using Confluent.Kafka;
using System;

namespace Aix.MessageBus.Kafka
{
    /// <summary>
    /// kafka序列化适配  转化为Confluent.Kafka的api要求
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class ConfluentKafkaSerializerAdapter<T> : Confluent.Kafka.ISerializer<T>, Confluent.Kafka.IDeserializer<T>
    {
        private ISerializer _serializer;
        public ConfluentKafkaSerializerAdapter(ISerializer  serializer)
        {
            _serializer = serializer;
        }
        public byte[] Serialize(T data, SerializationContext context)
        {
            return _serializer.Serialize<T>(data);
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (!isNull)
            {
                return _serializer.Deserialize<T>(data.ToArray());
            }
            return default(T);
        }

    }
}
