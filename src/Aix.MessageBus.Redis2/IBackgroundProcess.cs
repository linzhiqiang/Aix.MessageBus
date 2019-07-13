using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Aix.MessageBus.Redis2
{
    public interface IBackgroundProcess
    {
        Task Execute(BackgroundProcessContext context);
    }


}
