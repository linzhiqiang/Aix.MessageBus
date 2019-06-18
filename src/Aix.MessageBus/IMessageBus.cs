using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Aix.MessageBus
{

    public  class MessageBusContext
    {

        private IDictionary<string, string> _config;

        /// <summary>
        /// 配置
        /// </summary>
        public IDictionary<string, string> Config
        {
            get
            {
                if (_config == null) _config = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
                return _config;
            }
        }
    }

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
        /// <param name="context"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task SubscribeAsync<T>(Func<T, Task> handler, MessageBusContext context, CancellationToken cancellationToken=default);

    }


}
