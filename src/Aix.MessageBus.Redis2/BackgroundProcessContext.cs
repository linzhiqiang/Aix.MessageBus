using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Aix.MessageBus.Redis2
{
    public class BackgroundProcessContext
    {
        public BackgroundProcessContext(CancellationToken cancellationToken)
        {
            CancellationToken = cancellationToken;
        }
        public CancellationToken CancellationToken { get; }

        public List<string> SubscriberTopics { get; } = new List<string>();

        public bool IsShutdownRequested => CancellationToken.IsCancellationRequested;
    }
}
