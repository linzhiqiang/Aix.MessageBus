﻿using Aix.MessageBus.Utils;
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
    internal class KafkaProducer<TKey, TValue> : IKafkaProducer<TKey, TValue>
    {
        private IServiceProvider _serviceProvider;
        private ILogger<KafkaProducer<TKey, TValue>> _logger;
        private KafkaMessageBusOptions _kafkaOptions;

        IProducer<TKey, TValue> _producer = null;
        public KafkaProducer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _logger = serviceProvider.GetService<ILogger<KafkaProducer<TKey, TValue>>>();
            _kafkaOptions = serviceProvider.GetService<KafkaMessageBusOptions>();

            this.CreateProducer();
        }

        public Task<DeliveryResult<TKey, TValue>> ProduceAsync(string topic, Message<TKey, TValue> message)
        {
            return this._producer.ProduceAsync(topic, message);
        }

        public void Dispose()
        {
            _logger.LogInformation("Kafka关闭生产者");
            if (this._producer != null)
            {
                With.NoException(_logger, () => { this._producer.Dispose(); }, "关闭生产者");
            }
        }

        #region private

        private void CreateProducer()
        {
            if (this._producer != null) return;

            lock (this)
            {
                if (this._producer != null) return;

                if (_kafkaOptions.ProducerConfig == null) throw new Exception("请配置ProducerConfig参数");
                if (string.IsNullOrEmpty(_kafkaOptions.ProducerConfig.BootstrapServers)) throw new Exception("请配置ProducerConfig.BootstrapServers参数");
                IProducer<TKey, TValue> producer = new ProducerBuilder<TKey, TValue>(_kafkaOptions.ProducerConfig)
                .SetErrorHandler((p, error) =>
                {
                    _logger.LogError($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}Kafka生产者出错：{error.Code}-{error.Reason},IsLocalError:{error.IsLocalError}, IsBrokerError:{error.IsBrokerError}");
                })
               .SetValueSerializer(new ConfluentKafkaSerializerAdapter<TValue>(_kafkaOptions.Serializer))
               .Build();

                this._producer = producer;
            }
        }

        #endregion


    }
}
