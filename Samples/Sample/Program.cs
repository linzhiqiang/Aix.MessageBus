using Aix.MessageBus;
using Aix.MessageBus.Kafka;
using Aix.MessageBus.RabbitMQ;
using Aix.MessageBus.Redis;
using CommandLine;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Sample
{
    /*
    dotnet run -m 1 -q 10000  //生产者测试
    dotnet run -m 2  //消费者测试
    dotnet run -m 3 -q 10000 //生产者消费者一起测试
     */

    class Program
    {
        static void Main(string[] args)
        {
            Parser parser = new Parser((setting) =>
            {
                setting.CaseSensitive = false;
            });
            //Parser.Default.ParseArguments<CmdOptions>(args).WithParsed(Run);
            parser.ParseArguments<CmdOptions>(args).WithParsed(Run);
        }
        static void Run(CmdOptions options)
        {
            TaskScheduler.UnobservedTaskException += TaskScheduler_UnobservedTaskException;
            var host = new HostBuilder()
                .ConfigureHostConfiguration(builder =>
                {
                    //https://www.cnblogs.com/subendong/p/8834902.html
                    builder.AddEnvironmentVariables(prefix: "Demo_"); //配置环境变量 Demo_ENVIRONMENT Development/Staging/Production(默认值)
                })
                .ConfigureAppConfiguration((hostContext, config) =>
                {
                    config.AddJsonFile("appsettings.json", optional: true);
                    config.AddJsonFile($"appsettings.{hostContext.HostingEnvironment.EnvironmentName}.json", optional: true);// 覆盖前面的相同内容
                })
                .ConfigureLogging((context, factory) =>
                {
                    factory.AddConsole();
                })
                .ConfigureServices((context, services) =>
                {
                    //var environment = Environment.GetEnvironmentVariable("Demo_ENVIRONMENT");
                    //if (context.HostingEnvironment.IsEnvironment("Development"))
                    if (context.HostingEnvironment.IsDevelopment()) //IsStaging(),IsProduction()
                    {
                    }
                    services.AddSingleton(options);

                    //测试哪个messagebus
                    var messagebusType = context.Configuration.GetSection("messagebusType").Get<int>();// 0=memory 1=kafka  2=redis 3 rabbitmq

                    switch (messagebusType)
                    {
                        case 0:
                            services.AddInMemoryMessageBus(new InMemoryMessageBusOptions());
                            break;
                        case 1:
                            var kafkaMessageBusOptions = context.Configuration.GetSection("kafka").Get<KafkaMessageBusOptions>();
                            services.AddKafkaMessageBus(kafkaMessageBusOptions);
                            break;
                        case 2:
                            var redisMessageBusOptions = context.Configuration.GetSection("redis-messagebus").Get<RedisMessageBusOptions>();
                            redisMessageBusOptions.RetryStrategy = new int[] { 1,1,1,1,1,1,1};
                            services.AddRedisMessageBus(redisMessageBusOptions); //list实现
                            //services.AddRedisMessageBusPubSub(redisMessageBusOptions);//发布订阅实现
                            break;
                        case 3:
                            var rabbitMQMessageBusOptions = context.Configuration.GetSection("rabbitmq").Get<RabbitMQMessageBusOptions>();
                            rabbitMQMessageBusOptions.RetryStrategy = new int[] { 0, 0, 5, 10, 10, 10, 10 };
                            rabbitMQMessageBusOptions.MaxErrorReTryCount = 5;
                            services.AddRabbitMQMessageBus(rabbitMQMessageBusOptions);
                            break;
                        default:
                            break;
                    }

                    if ((options.Mode & (int)ClientMode.Consumer) > 0)
                    {
                        services.AddHostedService<MessageBusConsumeService>();
                    }
                    if ((options.Mode & (int)ClientMode.Producer) > 0)
                    {
                        services.AddHostedService<MessageBusProduerService>();
                    }

                });

            Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}开始");
            host.RunConsoleAsync().Wait();
            Console.WriteLine("服务已退出");
        }

        private static void TaskScheduler_UnobservedTaskException(object sender, UnobservedTaskExceptionEventArgs e)
        {
            try
            {
                Console.WriteLine(e.Exception.ToString());
            }
            catch
            {
            }
        }


        private static void AddKafkaMessageBus(IServiceCollection services, ClientMode mode)
        {
            var bootstrapServers = "192.168.111.132:9092,192.168.111.132:9093,192.168.111.132:9094";// com 虚拟机
                                                                                                    // bootstrapServers = "192.168.72.132:9092,192.168.72.132:9093,192.168.72.132:9094";//home 虚拟机
            var options = new KafkaMessageBusOptions
            {
                TopicPrefix = "kafka-", //项目名称
                Serializer = new MessagePackSerializer(), //默认也是该值
                DefaultConsumerThreadCount = 4, //总部署线程数不要大于分区数
                ManualCommitBatch = 100,
                ProducerConfig = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers,
                    Acks = Acks.Leader,
                    Partitioner = Partitioner.ConsistentRandom
                },
                ConsumerConfig = new ConsumerConfig
                {
                    GroupId = "kafka-messagebus",
                    BootstrapServers = bootstrapServers,
                    AutoOffsetReset = AutoOffsetReset.Latest,////AutoOffsetReset.Earliest,
                    EnableAutoCommit = false,
                    AutoCommitIntervalMs = 5000, //自动提交偏移量间隔 ，每5秒同步一次,当再均衡时，如果有消费者一直没有poll，会等到所有的消费者都poll之后才再均衡处理
                    CancellationDelayMaxMs = 1000 //poll等待时间，如果自己 consumer.Consume(TimeSpan.FromSeconds(1));写的时间就不用这个配置了

                }
            };

            services.AddKafkaMessageBus(options);
        }

        private static void AddInMemoryMessageBus(IServiceCollection services)
        {
            var options = new InMemoryMessageBusOptions();
            services.AddInMemoryMessageBus(options);
        }

    }
}
