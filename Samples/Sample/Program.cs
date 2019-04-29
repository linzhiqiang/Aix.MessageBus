using Aix.MessageBus;
using Aix.MessageBus.Kafka;
using CommandLine;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;

namespace Sample
{
    /*
    dotnet run -m p -q 100  //生产者测试
    dotnet run -m c  //消费者测试
    dotnet run -m a -q 100 //生产者消费者一起测试
     */


    /// <summary>
    /// 组件 commandlineparser
    /// </summary>
    public class CmdOptions
    {
        [Option('m', "mode", Required = false, Default = "c", HelpText = "p=生产者测试，c=消费者测试,a=同时测试")]
        public string Mode { get; set; }

        [Option('q', "quantity", Required = false, Default = 1, HelpText = "测试生产数量")]
        public int Count { get; set; }
    }
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
            var host = new HostBuilder()
                .ConfigureAppConfiguration((hostContext, config) =>
                {
                })
                .ConfigureLogging((context, factory) =>
                {
                    factory.AddConsole();
                })
                .ConfigureServices((context, services) =>
                {

                    KafkaMessageBusMode mode = GetTestMode(options);

                    services.AddSingleton(options);
                    AddKafkaMessageBus(services, mode);
                    //AddInMemoryMessageBus(services);

                    if ((mode & KafkaMessageBusMode.Consumer) > 0)
                    {
                        services.AddHostedService<MessageBusConsumeService>();
                    }
                    if ((mode & KafkaMessageBusMode.Producer) > 0)
                    {
                        services.AddHostedService<MessageBusProduerService>();
                    }

                });
            Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}开始");
            host.RunConsoleAsync().Wait();
            Console.WriteLine("服务已退出");
        }

        private static KafkaMessageBusMode GetTestMode(CmdOptions options)
        {
            KafkaMessageBusMode mode = 0;
            if (options.Mode.ToLower() == "c")
            {
                mode = KafkaMessageBusMode.Consumer;
            }
            if (options.Mode.ToLower() == "p")
            {
                mode = KafkaMessageBusMode.Producer;
            }
            if (options.Mode.ToLower() == "a")
            {
                mode = KafkaMessageBusMode.Both;
            }

            return mode;
        }

        private static void AddKafkaMessageBus(IServiceCollection services, KafkaMessageBusMode mode)
        {
            var bootstrapServers = "192.168.111.132:9092,192.168.111.132:9093,192.168.111.132:9094";// com 虚拟机
            //bootstrapServers = "192.168.72.131:9092,192.168.72.131:9093,192.168.72.131:9094";//home 虚拟机
            
            var options = new KafkaMessageBusOptions
            {
                KafkaMessageBusMode = mode,
                TopicPrefix = "kafka1", //项目名称
                TopicMode = TopicMode.multiple,
                Serializer = new MessagePackSerializer(), //默认也是该值
                ConsumerThreadCount = 4, //总部署线程数不要大于分区数
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
                    AutoOffsetReset = AutoOffsetReset.Earliest,
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
