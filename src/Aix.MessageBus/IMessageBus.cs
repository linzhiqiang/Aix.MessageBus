using System;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus
{
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
}
