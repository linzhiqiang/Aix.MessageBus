using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Foundation
{
    internal interface IBlockingQueue<T>
    {
        int Count { get; }

        void Enqueue(T item);

        /// <summary>
        /// 阻塞
        /// </summary>
        /// <returns></returns>
        T Dequeue();

        bool TryDequeue(out T item);

    }
}
