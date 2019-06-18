using MessagePack;
using MessagePack.Resolvers;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus
{
    public class MessagePackSerializer : ISerializer
    {
        private readonly IFormatterResolver _formatterResolver;
        private readonly bool _useCompression;

        public MessagePackSerializer(IFormatterResolver resolver = null, bool useCompression = false)
        {
            _useCompression = useCompression;
            _formatterResolver = resolver ?? ContractlessStandardResolver.Instance;
        }

        public T Deserialize<T>(byte[] bytes)
        {
            if (_useCompression)
            {
                return (T)MessagePack.LZ4MessagePackSerializer.NonGeneric.Deserialize(typeof(T), bytes, _formatterResolver);
            }
            else
            {
                return (T)MessagePack.MessagePackSerializer.NonGeneric.Deserialize(typeof(T), bytes, _formatterResolver);
            }
        }

        public byte[] Serialize<T>(T data)
        {
            if (_useCompression)
            {
                return MessagePack.LZ4MessagePackSerializer.NonGeneric.Serialize(data.GetType(), data, _formatterResolver);
            }
            else
            {
                return MessagePack.MessagePackSerializer.NonGeneric.Serialize(data.GetType(), data, _formatterResolver);
            }
        }
    }
}
