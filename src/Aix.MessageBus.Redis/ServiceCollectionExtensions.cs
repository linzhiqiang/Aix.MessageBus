using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Redis
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddRedisMessageBus(this IServiceCollection services, RedisMessageBusOptions options)
        {
            AddService(services, options);
            services.AddSingleton<IMessageBus, RedisMessageBus>();
            return services;
        }

        public static IServiceCollection AddRedisMessageBusPubSub(this IServiceCollection services, RedisMessageBusOptions options)
        {
            AddService(services, options);
            services.AddSingleton<IMessageBus, RedisMessageBus_Subscriber>();
            return services;
        }

        private static IServiceCollection AddService( IServiceCollection services, RedisMessageBusOptions options)
        {
            //var exists = services.FirstOrDefault(x => x.ImplementationType == typeof(ConnectionMultiplexer));
            if (options.ConnectionMultiplexer != null)
            {
                services.AddSingleton(options.ConnectionMultiplexer);
            }
            else if (!string.IsNullOrEmpty(options.ConnectionString))
            {
                var redis = ConnectionMultiplexer.Connect(options.ConnectionString);
                services.AddSingleton(redis);
            }
            else
            {
                throw new Exception("ConnectionMultiplexer或RedisConnectionString为空");
            }

            services.AddSingleton(options);

            return services;
        }
    }
}
