﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus
{
    public static class MessageBusExtensions
    {
        public static Task PublishAsync<T>(this IMessageBus messageBus, T message)
        {
            return messageBus.PublishAsync(typeof(T), message);
        }
    }
}
