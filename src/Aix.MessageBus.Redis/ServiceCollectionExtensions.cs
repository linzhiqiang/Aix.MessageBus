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
        /// <summary>
        /// 利用list实现队列功能，不支持同一类型多次订阅
        /// </summary>
        /// <param name="services"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public static IServiceCollection AddRedisMessageBus(this IServiceCollection services, RedisMessageBusOptions options)
        {
            AddService(services, options);
            services.AddSingleton<IMessageBus, RedisMessageBus>();
            return services;
        }

        /// <summary>
        /// 利用redis发布订阅功能实现
        /// </summary>
        /// <param name="services"></param>
        /// <param name="options"></param>
        /// <returns></returns>
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
