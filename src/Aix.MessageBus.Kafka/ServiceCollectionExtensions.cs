using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Kafka
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddKafkaMessageBus(this IServiceCollection services, KafkaMessageBusOptions options)
        {
            services
               .AddSingleton<KafkaMessageBusOptions>(options)
               .AddSingleton<IMessageBus, KafkaMessageBus>();

            return services;
        }
    }
}
