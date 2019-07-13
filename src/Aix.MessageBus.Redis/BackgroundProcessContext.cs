using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Aix.MessageBus.Redis
{
    public class BackgroundProcessContext
    {
        public BackgroundProcessContext(CancellationToken cancellationToken)
        {
            CancellationToken = cancellationToken;
        }
        public CancellationToken CancellationToken { get; }

        /// <summary>
        /// 订阅topic列表
        /// </summary>
        public List<string> SubscriberTopics { get; } = new List<string>();

        public bool IsShutdownRequested => CancellationToken.IsCancellationRequested;
    }
}
