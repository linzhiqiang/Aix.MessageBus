﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus
{
    public class SubscriberInfo
    {
        public Type Type { get; set; }

        public Func<byte[], Task> Action { get; set; }
    }
}
