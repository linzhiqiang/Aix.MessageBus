﻿using Aix.MessageBus.Kafka.Model;
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
    internal class KafkaProducer<TKey, TValue> : IKafkaProducer<TKey, TValue>
    {
        private IServiceProvider _serviceProvider;
        private ILogger<KafkaProducer<TKey, TValue>> _logger;
        private KafkaMessageBusOptions _kafkaOptions;
        private static TimeSpan DefaultTimeout = TimeSpan.FromSeconds(5);

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

        /// <summary>
        /// 事务版本   貌似没必要 要求 配置TransactionalId，Acks必须等于all
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        public async Task<DeliveryResult<TKey, TValue>> ProduceAsync2(string topic, Message<TKey, TValue> message)
        {
            DeliveryResult<TKey, TValue> result = null;
            try
            {
                _producer.InitTransactions(DefaultTimeout);
                _producer.BeginTransaction();
                result = await this._producer.ProduceAsync(topic, message);
                _producer.CommitTransaction(DefaultTimeout);
            }
            catch (Exception)
            {
                _producer.AbortTransaction(DefaultTimeout);
                throw;
            }
            return result;
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
                IProducer<TKey, TValue> producer = new ProducerBuilder<TKey, TValue>(_kafkaOptions.ProducerConfig)
                .SetErrorHandler((p, error) =>
                {
                    if (error.IsFatal)
                    {
                        string errorInfo = $"{error.Code}-{error.Reason}, IsFatal={error.IsFatal}, IsLocalError:{error.IsLocalError}, IsBrokerError:{error.IsBrokerError}";
                        _logger.LogError($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}Kafka生产者出错：{errorInfo}");
                    }
                })
               .SetValueSerializer(new ConfluentKafkaSerializerAdapter<TValue>(_kafkaOptions.Serializer))
               .Build();

                this._producer = producer;
            }
        }

        #endregion


    }
}
