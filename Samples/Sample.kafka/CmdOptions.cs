using Aix.MessageBus;
using Aix.MessageBus.Kafka;
using CommandLine;
using System;
using System.Collections.Generic;
using System.Text;

namespace Sample.kafka
{


    /// <summary>
    /// 组件 commandlineparser
    /// </summary>
    public class CmdOptions
    {
        [Option('m', "mode", Required = false, Default = 1, HelpText = "1=生产者测试，2=消费者测试,3=同时测试")]
        public ClientMode Mode { get; set; }

        [Option('q', "quantity", Required = false, Default = 1, HelpText = "测试生产数量")]
        public int Count { get; set; }
    }
}
