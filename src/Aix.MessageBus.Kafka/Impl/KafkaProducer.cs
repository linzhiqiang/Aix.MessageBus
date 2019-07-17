using Aix.MessageBus.Kafka.Model;
using Aix.MessageBus.Utils;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.Kafka.Impl
{
    /// <summary>
    /// kafka生产者实现
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    internal class KafkaProducer : IKafkaProducer
    {
        private IServiceProvider _serviceProvider;
        private ILogger<KafkaProducer> _logger;
        private KafkaMessageBusOptions _kafkaOptions;

        IProducer<Null, KafkaMessageBusData> _producer = null;
        public KafkaProducer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _logger = serviceProvider.GetService<ILogger<KafkaProducer>>();
            _kafkaOptions = serviceProvider.GetService<KafkaMessageBusOptions>();

            this.CreateProducer();
        }

        public Task<DeliveryResult<Null, KafkaMessageBusData>> ProduceAsync(KafkaMessageBusData data)
        {
            var message = new Message<Null, KafkaMessageBusData> { Value = data };
            return this._producer.ProduceAsync(message.Value.Topic, message);
        }
        public async Task<DeliveryResult<Null, KafkaMessageBusData>> ProduceDelayAsync(KafkaMessageBusData data, TimeSpan delay)
        {
            if (delay <= TimeSpan.Zero)
            {
                return await this.ProduceAsync(data);
            }
            else
            {
                var delayTopic = Helper.GetDelayTopic(_kafkaOptions, delay);
                var message = new Message<Null, KafkaMessageBusData> { Value = data };
              return   await  this._producer.ProduceAsync(delayTopic, message);
            }
        }

        public void Dispose()
        {
            _logger.LogInformation("Kafka关闭生产者");
            if (this._producer != null)
            {
                With.NoException(_logger, () => { this._producer.Dispose(); }, "Kafka关闭生产者");
            }
        }

        #region private

        private void CreateProducer()
        {
            if (this._producer != null) return;

            lock (this)
            {
                if (this._producer != null) return;

                if (_kafkaOptions.ProducerConfig == null) _kafkaOptions.ProducerConfig = new ProducerConfig();
                if (string.IsNullOrEmpty(_kafkaOptions.ProducerConfig.BootstrapServers))
                {
                    _kafkaOptions.ProducerConfig.BootstrapServers = _kafkaOptions.BootstrapServers;
                }
                if (string.IsNullOrEmpty(_kafkaOptions.ProducerConfig.BootstrapServers))
                {
                    throw new Exception("kafka BootstrapServers参数");
                }
                IProducer<Null, KafkaMessageBusData> producer = new ProducerBuilder<Null, KafkaMessageBusData>(_kafkaOptions.ProducerConfig)
                .SetErrorHandler((p, error) =>
                {
                    if (error.IsFatal)
                    {
                        string errorInfo = $"{error.Code}-{error.Reason}, IsFatal={error.IsFatal}, IsLocalError:{error.IsLocalError}, IsBrokerError:{error.IsBrokerError}";
                        _logger.LogError($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}Kafka生产者出错：{errorInfo}");
                    }
                })
               .SetValueSerializer(new ConfluentKafkaSerializerAdapter<KafkaMessageBusData>(_kafkaOptions.Serializer))
               .Build();

                this._producer = producer;
            }
        }

        #endregion


    }
}
