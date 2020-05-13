using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddInMemoryMessageBus(this IServiceCollection services, InMemoryMessageBusOptions options)
        {
            if (options == null) options = new InMemoryMessageBusOptions();
            services
               .AddSingleton<InMemoryMessageBusOptions>(options)
               .AddSingleton<IMessageBus, InMemoryMessageBus>();

            return services;
        }
    }
}
