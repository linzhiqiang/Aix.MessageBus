using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.RabbitMQ
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddRabbitMQMessageBus(this IServiceCollection services, RabbitMQMessageBusOptions options)
        {
            var connection = CreateConnection(options);
            services
               .AddSingleton<RabbitMQMessageBusOptions>(options)
               .AddSingleton(connection)
               .AddSingleton<IMessageBus, RabbitMQMessageBus>();

            return services;
        }

        private static IConnection CreateConnection(RabbitMQMessageBusOptions options)
        {
            var factory = new ConnectionFactory()
            {
                HostName = options.HostName,
                Port = options.Port,
                VirtualHost = options.VirtualHost,
                UserName = options.UserName,
                Password = options.Password,

                AutomaticRecoveryEnabled = true,
                Protocol = Protocols.DefaultProtocol
            };

            return factory.CreateConnection();
        }
    }
}
