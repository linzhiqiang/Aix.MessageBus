# Aix.MessageBus
messagebus定义
 public interface IMessageBus : IDisposable
    {
        /// <summary>
        /// 发布消息
        /// </summary>
        /// <param name="messageType"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        Task PublishAsync(Type messageType, object message);

        /// <summary>
        /// 订阅消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="handler"></param>
        /// <returns></returns>
        Task SubscribeAsync<T>(Func<T, Task> handler);

        /// <summary>
        /// messagebus开启,重复调用只会启动一次
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task StartAsync(CancellationToken cancellationToken);

    }
message kafka实现
 public interface KafkaMessageBus : IMessageBus
    {
       ...
    }
    
    
