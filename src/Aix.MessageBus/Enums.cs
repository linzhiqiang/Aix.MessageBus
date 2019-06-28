using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus
{


    public enum ConsumerMode
    {
        /// <summary>
        /// 至少一次
        /// </summary>
        AtLeastOnce = 0,

        /// <summary>
        /// 至多一次
        /// </summary>
        AtMostOnce = 1
    }
}
