using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.RabbitMQ
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddRabbitMQMessageBus(this IServiceCollection services, RabbitMQMessageBusOptions options)
        {
            services
               .AddSingleton<RabbitMQMessageBusOptions>(options)
               .AddSingleton<IMessageBus, RabbitMQMessageBus>();

            return services;
        }
    }
}
