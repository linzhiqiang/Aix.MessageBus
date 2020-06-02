using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Concurrent;
using Aix.MessageBus.Utils;
using Aix.MessageBus.Kafka.Model;

namespace Aix.MessageBus.Kafka.Impl
{
    internal class KafkaDelayConsumer : IKafkaDelayConsumer
    {
        private IServiceProvider _serviceProvider;
        private ILogger<KafkaDelayConsumer> _logger;
        private KafkaMessageBusOptions _kafkaOptions;

        private string _topic;
        private TimeSpan _delay;

        IKafkaProducer<string, KafkaMessageBusData> _producer = null;
        IConsumer<string, KafkaMessageBusData> _consumer = null;
        /// <summary>
        /// 存储每个分区的最大offset，针对手工提交 
        /// </summary>
        private ConcurrentDictionary<TopicPartition, TopicPartitionOffset> _currentOffsets = new ConcurrentDictionary<TopicPartition, TopicPartitionOffset>();
        private volatile bool _isStart = false;
        private int Count = 0;
        public KafkaDelayConsumer(IServiceProvider serviceProvider, string topic, TimeSpan delay, IKafkaProducer<string, KafkaMessageBusData> producer)
        {
            _serviceProvider = serviceProvider;
            _topic = topic;
            _delay = delay;
            _producer = producer;

            _logger = serviceProvider.GetService<ILogger<KafkaDelayConsumer>>();
            _kafkaOptions = serviceProvider.GetService<KafkaMessageBusOptions>();
        }

        public Task Subscribe(CancellationToken cancellationToken)
        {
            string topic = this._topic;
            string groupId = "delaygroupId";
            return Task.Run(async () =>
            {
                _isStart = true;
                this._consumer = this.CreateConsumer(groupId);
                this._consumer.Subscribe(topic);
                await StartPoll(cancellationToken);
            });
        }

        public void Close()
        {
            if (this._isStart == false) return;
            lock (this)
            {
                if (this._isStart == false) return;
                this._isStart = false;
            }
            _logger.LogInformation("Kafka关闭延迟消费者");
            ManualCommitOffset();
            With.NoException(_logger, () => { this._consumer?.Close(); }, "关闭延迟消费者");
        }

        public void Dispose()
        {
            this.Close();
        }

        #region private

        private Task StartPoll(CancellationToken cancellationToken)
        {
            Task.Factory.StartNew(async () =>
            {
                _logger.LogInformation("开始延迟消费数据...");
                try
                {
                    while (_isStart && !cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            await Consumer(cancellationToken);//AccessViolationException
                        }
                        catch (OperationCanceledException)
                        {
                            //_logger.LogError(ex, $"延迟消费异常退出消费循环OperationCanceledException，操作取消");
                        }
                        catch (ConsumeException ex)
                        {
                            _logger.LogError(ex, $"延迟消费拉取消息ConsumeException");
                        }
                        catch (KafkaException ex)
                        {
                            _logger.LogError(ex, $"延迟消费拉取消息KafkaException");
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, $"延迟消费拉取消息系统异常Exception");
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"延迟消费异常退出消费循环Exception");
                }
                finally
                {
                    _logger.LogInformation("退出延迟消费循环 关闭消费者...");
                    this.Close();
                }
            });

            return Task.CompletedTask;
        }

        private async Task Consumer(CancellationToken cancellationToken)
        {
            var result = this._consumer.Consume(cancellationToken);// cancellationToken默认100毫秒
            if (result == null || result.IsPartitionEOF || result.Message == null || result.Message.Value == null)
            {
                return;
            }
            //消费数据
            await Handler(result, cancellationToken);

            //处理手动提交
            ManualCommitOffset(result); //采用后提交（至少一次）,消费前提交（至多一次）
        }

        /// <summary>
        /// 提交所有分区
        /// </summary>
        private void ManualCommitOffset()
        {
            With.NoException(_logger, () =>
            {
                if (_currentOffsets.Count > 0)
                {
                    this._consumer.Commit(_currentOffsets.Values);
                }
            }, "手动提交所有分区错误");

            ClearCurrentOffsets();
        }

        private void ClearCurrentOffsets()
        {
            Count = 0;
            _currentOffsets.Clear();
        }

        /// <summary>
        /// 手工提交offset
        /// </summary>
        /// <param name="result"></param>
        private void ManualCommitOffset(ConsumeResult<string, KafkaMessageBusData> result)
        {
            //处理手动提交
            if (EnableAutoCommit() == false)
            {
                Count++;
                var topicPartition = result.TopicPartition;
                var topicPartitionOffset = new TopicPartitionOffset(topicPartition, result.Offset + 1);
                AddToOffsetDict(topicPartition, topicPartitionOffset); //加入offset缓存 

                // if (Count % _kafkaOptions.ManualCommitBatch == 0)
                {
                    ManualCommitOffset();
                }
            }
        }

        private TimeSpan MinTimeSpan(params TimeSpan[] timeSpans)
        {
            if (timeSpans == null || timeSpans.Length == 0) return TimeSpan.Zero;

            var min = timeSpans[0];
            foreach (var item in timeSpans)
            {
                if (min > item)
                {
                    min = item;
                }
            }
            return min;

        }

        private TimeSpan GetMaxPollIntervalMs()
        {
            //var maxPoll = _kafkaOptions.ConsumerConfig.MaxPollIntervalMs;
            //if (maxPoll.HasValue) return TimeSpan.FromMilliseconds(maxPoll.Value);
            return TimeSpan.FromSeconds(60);
        }

        private async Task Handler(ConsumeResult<string, KafkaMessageBusData> consumeResult, CancellationToken cancellationToken)
        {
            try
            {
                var messageBusData = consumeResult.Message.Value;
                var delay = TimeSpan.FromMilliseconds(messageBusData.ExecuteTimeStamp - DateUtils.GetTimeStamp(DateTime.Now));
                //_logger.LogInformation($"------------{DateTime.Now.ToString("HH:mm:ss fff")}---------------{delayTime.TotalSeconds}----------------------------------------------");
                if (delay > TimeSpan.Zero)
                {
                    var minDelay = MinTimeSpan(GetMaxPollIntervalMs(), delay, this._delay);
                    await Task.Delay(minDelay, cancellationToken); // 这里超过300秒也不行，生产者内部会出错的 ,delay不太精准
                }

                delay = TimeSpan.FromMilliseconds(messageBusData.ExecuteTimeStamp - DateUtils.GetTimeStamp(DateTime.Now));
                if (delay > TimeSpan.Zero)
                {
                    //继续插入队列
                    var delayTopic = Helper.GetDelayTopic(_kafkaOptions, delay);
                    await _producer.ProduceAsync(delayTopic, consumeResult.Message);
                }
                else
                {
                    //插入及时队列
                    await _producer.ProduceAsync(messageBusData.Topic, consumeResult.Message);
                }
            }
            catch (OperationCanceledException)
            {
                // _logger.LogError(ex, $"kafka延迟消费失败OperationCanceledException，操作取消");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "kafka延迟消费失败");
            }
        }

        private void AddToOffsetDict(TopicPartition topicPartition, TopicPartitionOffset TopicPartitionOffset)
        {
            _currentOffsets.AddOrUpdate(topicPartition, TopicPartitionOffset, (key, oldValue) =>
            {
                return TopicPartitionOffset.Offset > oldValue.Offset ? TopicPartitionOffset : oldValue;
            });
        }

        /// <summary>
        /// 创建消费者对象
        /// </summary>
        /// <returns></returns>
        private IConsumer<string, KafkaMessageBusData> CreateConsumer(string groupId)
        {
            if (_kafkaOptions.ConsumerConfig == null) _kafkaOptions.ConsumerConfig = new ConsumerConfig();

            if (string.IsNullOrEmpty(_kafkaOptions.ConsumerConfig.BootstrapServers))
            {
                _kafkaOptions.ConsumerConfig.BootstrapServers = _kafkaOptions.BootstrapServers;

            }
            if (string.IsNullOrEmpty(_kafkaOptions.ConsumerConfig.BootstrapServers))
            {
                throw new Exception("请配置BootstrapServers参数");
            }

            var config = new Dictionary<string, string>(); //这里转成字典 便于不同消费者可以改变消费者配置（因为是就一个配置对象）
            lock (_kafkaOptions.ConsumerConfig)
            {
                config = _kafkaOptions.ConsumerConfig.ToDictionary(x => x.Key, v => v.Value);
            }
            if (!string.IsNullOrEmpty(groupId))
            {
                config["group.id"] = groupId;
            }

            var consumerConfig = new ConsumerConfig(config);
            consumerConfig.EnableAutoCommit = false;
            var builder = new ConsumerBuilder<string, KafkaMessageBusData>(consumerConfig)
                 .SetErrorHandler((producer, error) =>
                 {
                     if (error.IsFatal || error.IsBrokerError)
                     {
                         string errorInfo = $"Code:{error.Code}, Reason:{error.Reason}, IsFatal={error.IsFatal}, IsLocalError:{error.IsLocalError}, IsBrokerError:{error.IsBrokerError}";
                         _logger.LogError($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}Kafka延迟消费者出错：{errorInfo}");
                     }
                 })
                 .SetPartitionsRevokedHandler((c, partitions) =>
                 {
                     //方法会在再均衡开始之前和消费者停止读取消息之后被调用。如果在这里提交偏移量，下一个接管partition的消费者就知道该从哪里开始读取了。
                     //partitions表示再均衡前所分配的分区
                     if (EnableAutoCommit() == false)
                     {
                         ManualCommitOffset();
                     }
                 })
                 .SetPartitionsAssignedHandler((c, partitions) =>
                 {
                     //方法会在重新分配partition之后和消费者开始读取消息之前被调用。
                     if (EnableAutoCommit() == false)
                     {
                         ClearCurrentOffsets();
                     }
                     _logger.LogInformation($"MemberId:{c.MemberId}分配的分区：Assigned partitions: [{string.Join(", ", partitions)}]");
                 })
               .SetValueDeserializer(new ConfluentKafkaSerializerAdapter<KafkaMessageBusData>(_kafkaOptions.Serializer));

            //以下是内置的
            //if (typeof(TKey) == typeof(Null)) builder.SetKeyDeserializer((IDeserializer<TKey>)Confluent.Kafka.Deserializers.Null);
            //if (typeof(TKey) == typeof(string)) builder.SetKeyDeserializer((IDeserializer<TKey>)Confluent.Kafka.Deserializers.Utf8);
            //if (typeof(TKey) == typeof(int)) builder.SetKeyDeserializer((IDeserializer<TKey>)Confluent.Kafka.Deserializers.Int32);
            //if (typeof(TKey) == typeof(long)) builder.SetKeyDeserializer((IDeserializer<TKey>)Confluent.Kafka.Deserializers.Int64);
            //if (typeof(TKey) == typeof(float)) builder.SetKeyDeserializer((IDeserializer<TKey>)Confluent.Kafka.Deserializers.Single);
            //if (typeof(TKey) == typeof(double)) builder.SetKeyDeserializer((IDeserializer<TKey>)Confluent.Kafka.Deserializers.Double);
            //if (typeof(TKey) == typeof(byte[])) builder.SetKeyDeserializer((IDeserializer<TKey>)Confluent.Kafka.Deserializers.ByteArray);
            //if (typeof(TKey) == typeof(Ignore)) builder.SetKeyDeserializer((IDeserializer<TKey>)Confluent.Kafka.Deserializers.Ignore);
            //if (typeof(TKey) == typeof(object)) builder.SetKeyDeserializer(new ConfluentKafkaSerializerAdapter<TKey>(_kafkaOptions.Serializer));

            var consumer = builder.Build();
            return consumer;
        }

        /// <summary>
        /// 是否是自动提交
        /// </summary>
        /// <returns></returns>
        private bool EnableAutoCommit()
        {
            return false;
        }

        #endregion
    }
}
