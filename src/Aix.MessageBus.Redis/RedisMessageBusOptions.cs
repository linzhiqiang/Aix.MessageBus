﻿using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Redis
{
  public  class RedisMessageBusOptions
    {
        private int[] DefaultRetryStrategy = new int[] { 1,5, 10, 30,  60,  60, 2 * 60, 2 * 60, 5 * 60, 5 * 60 };
        public RedisMessageBusOptions()
        {
            this.TopicPrefix = "redis:messagebus:";
            this.Serializer = new MessagePackSerializer();
            this.DataExpireDay = 7;
            this.DefaultConsumerThreadCount = 2;
            this.ErrorReEnqueueIntervalSecond = 30;
            this.ExecuteTimeoutSecond = 60;
            this.MaxErrorReTryCount = 10;
            this.CrontabLockSecond = 60;
            this.RetryStrategy = DefaultRetryStrategy;
        }

        /// <summary>
        /// RedisConnectionString和ConnectionMultiplexer传一个即可
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        ///  RedisConnectionString和ConnectionMultiplexer传一个即可
        /// </summary>
        public ConnectionMultiplexer ConnectionMultiplexer { get; set; }

        /// <summary>
        /// topic前缀，为了防止重复，建议用项目名称
        /// </summary>
        public string TopicPrefix { get; set; }

        /// <summary>
        /// 自定义序列化，默认为MessagePack
        /// </summary>
        public ISerializer Serializer { get; set; }

        /// <summary>
        /// 任务数据有效期 默认7天 单位  天
        /// </summary>
        public int DataExpireDay { get; set; }

        /// <summary>
        /// 定时任务锁定时间
        /// </summary>
        public int CrontabLockSecond { get; set; }

        /// <summary>
        /// 默认每个类型的消费线程数 默认2个
        /// </summary>
        public int DefaultConsumerThreadCount { get; set; }

        /// <summary>
        /// 错误数据重新入队  线程执行间隔
        /// </summary>
        public int ErrorReEnqueueIntervalSecond { get; set; }

        /// <summary>
        /// 执行超时时间，超过该时间，任务执行错误尝试重试
        /// </summary>
        public int ExecuteTimeoutSecond { get; set; }

        /// <summary>
        /// 最大错误重试次数 默认10次
        /// </summary>
        public int MaxErrorReTryCount { get; set; }

        private int[] _retryStrategy;
        /// <summary>
        /// 失败重试延迟策略 单位：秒  默认失败次数对应值延迟时间[ 1,5, 10, 30,  60,  60, 2 * 60, 2 * 60, 5 * 60, 5 * 60  ];
        /// </summary>
        public int[] RetryStrategy
        {
            get
            {
                if (_retryStrategy == null || _retryStrategy.Length == 0) _retryStrategy = DefaultRetryStrategy;
                return _retryStrategy;
            }
            set { _retryStrategy = value; }
        }
    }
}
