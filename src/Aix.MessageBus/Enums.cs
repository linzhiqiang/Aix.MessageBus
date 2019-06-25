using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus
{
    [Flags]
    public enum ClientMode
    {
        Producer = 1,
        Consumer = 2
    }
}
