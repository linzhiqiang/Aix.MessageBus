using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.Redis
{
    public interface IBackgroundProcess:IDisposable
    {
        Task Execute(BackgroundProcessContext context);
    }


}
