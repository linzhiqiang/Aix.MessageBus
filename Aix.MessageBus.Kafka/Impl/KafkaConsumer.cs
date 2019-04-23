﻿using Aix.MessageBus.Utils;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus.Kafka.Impl
{
    internal class KafkaConsumer<TKey, TValue> : IKafkaConsumer<TKey, TValue>
    {
        private IServiceProvider _serviceProvider;
        private ILogger<KafkaConsumer<TKey, TValue>> _logger;
        private KafkaMessageBusOptions _kafkaOptions;


        IConsumer<TKey, TValue> _consumer = null;
        /// <summary>
        /// 存储每个分区的最大offset，针对手工提交 
        /// </summary>
        private ConcurrentDictionary<TopicPartition, TopicPartitionOffset> _offsetDict = new ConcurrentDictionary<TopicPartition, TopicPartitionOffset>();
        private volatile bool _isStart = false;

        public event Func<ConsumeResult<TKey, TValue>, Task> OnMessage;
        public KafkaConsumer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _logger = serviceProvider.GetService<ILogger<KafkaConsumer<TKey, TValue>>>();
            _kafkaOptions = serviceProvider.GetService<KafkaMessageBusOptions>();
        }

        public Task Subscribe(string topic, CancellationToken cancellationToken)
        {
            Task.Run(async () =>
            {
                _isStart = true;
                this._consumer = this.CreateConsumer();
                this._consumer.Subscribe(topic);
                await StartPoll(cancellationToken);
            });

            return Task.CompletedTask;
        }

        public void Close()
        {
            With.NoException(_logger, () => { this._consumer?.Close(); }, "关闭消费者");
        }

        public void Dispose()
        {
            this.Close();
        }

        #region private

        private Task StartPoll(CancellationToken cancellationToken)
        {
            Task.Run(async () =>
            {
                try
                {
                    _logger.LogInformation("开始消费数据...");
                    while (_isStart && !cancellationToken.IsCancellationRequested)
                    {
                        var result = this._consumer.Consume(TimeSpan.FromSeconds(1));
                        if (result == null || result.IsPartitionEOF || result.Value == null)
                        {
                            continue;
                        }
                        //消费数据
                        await Handler(result);

                        //处理手动提交
                        if (EnableAutoCommit() == false)
                        {
                            var topicPartition = result.TopicPartition;
                            var topicPartitionOffset = new TopicPartitionOffset(topicPartition, result.Offset + 1);
                            AddToOffsetDict(topicPartition, topicPartitionOffset); //加入offset缓存

                            _offsetDict.TryGetValue(topicPartition, out TopicPartitionOffset maxOffset); //取出最大的offset提交，可能并发当前的不是最大的
                            this._consumer.Commit(new[] { maxOffset }); //if (maxOffset.Offset == topicPartitionOffset.Offset) 
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError($"消费异常退出消费循环, {ex.Message}, {ex.StackTrace}");
                }
                finally
                {
                    _logger.LogInformation("退出消费循环 关闭消费者...");
                    this.Close();
                }
            });


            return Task.CompletedTask;
        }

        private async Task Handler(ConsumeResult<TKey, TValue> consumeResult)
        {
            if (OnMessage != null)
            {
                await OnMessage(consumeResult);
            }
        }
        private void AddToOffsetDict(TopicPartition topicPartition, TopicPartitionOffset TopicPartitionOffset)
        {
            _offsetDict.AddOrUpdate(topicPartition, TopicPartitionOffset, (key, oldValue) =>
            {
                return TopicPartitionOffset.Offset > oldValue.Offset ? TopicPartitionOffset : oldValue;
            });
        }

        private IConsumer<TKey, TValue> CreateConsumer()
        {
            if (_kafkaOptions.ConsumerConfig == null) throw new Exception("请配置ProducerConfig参数");
            if (string.IsNullOrEmpty(_kafkaOptions.ConsumerConfig.BootstrapServers)) throw new Exception("请配置ConsumerConfig.BootstrapServers参数");
            if (string.IsNullOrEmpty(_kafkaOptions.ConsumerConfig.GroupId)) throw new Exception("请配置ConsumerConfig.GroupId参数");

            var consumer = new ConsumerBuilder<TKey, TValue>(_kafkaOptions.ConsumerConfig)
                  .SetErrorHandler((producer, error) =>
                  {
                      _logger.LogError($"Kafka生产者出错：{error.Reason}");
                  })
                  .SetPartitionsRevokedHandler((c, partitions) =>
                  {
                      //方法会在再均衡开始之前和消费者停止读取消息之后被调用。如果在这里提交偏移量，下一个接管partition的消费者就知道该从哪里开始读取了。
                      //Console.WriteLine($"Revoking assignment: [{string.Join(", ", partitions)}]");
                      if (EnableAutoCommit() == false)
                      {
                          //只提交当前消费者分配的分区
                          c.Commit(_offsetDict.Values.Where(x => partitions.Exists(current => current.Topic == x.Topic && current.Partition == x.Partition)));
                          _logger.LogInformation("Kafka再均衡提交");
                          _offsetDict.Clear();
                      }
                  })
                  .SetPartitionsAssignedHandler((c, partitions) =>
                  {
                      if (EnableAutoCommit() == false)
                      {
                          _offsetDict.Clear();
                      }
                      _logger.LogInformation($"MemberId:{c.MemberId}分配的分区：Assigned partitions: [{string.Join(", ", partitions)}]");
                  })
                //.SetKeyDeserializer(new ConfluentKafkaSerializerAdapter<TKey>(_kafkaOptions.Serializer))
                .SetValueDeserializer(new ConfluentKafkaSerializerAdapter<TValue>(_kafkaOptions.Serializer))
                .Build();

            return consumer;
        }

        private bool EnableAutoCommit()
        {
            var enableAutoCommit = this._kafkaOptions.ConsumerConfig.EnableAutoCommit;
            return !enableAutoCommit.HasValue || enableAutoCommit.Value == true;
        }

        #endregion
    }
}
