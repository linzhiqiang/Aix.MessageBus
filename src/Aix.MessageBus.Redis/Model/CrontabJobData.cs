﻿using Aix.MessageBus.Utils;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Redis.Model
{
    public class CrontabJobData
    {
        /// <summary>
        /// 定时任务标识  不能重复
        /// </summary>
        public string JobId { get; set; }

        /// <summary>
        /// 定时任务名称
        /// </summary>
        public string JobName { get; set; }

        /// <summary>
        /// 定时表达式
        /// </summary>
        public string CrontabExpression { get; set; }

        /// <summary>
        /// 业务数据
        /// </summary>
        public byte[] Data { get; set; }

        public string Topic { get; set; }

        /// <summary>
        /// 执行时间 
        /// </summary>
        public string LastExecuteTime { get; set; } 

        public List<HashEntry> ToDictionary()
        {
            var result = new List<HashEntry>
            {
                new HashEntry("JobId",JobId),
                new HashEntry("JobName",JobName),
                new HashEntry("CrontabExpression", CrontabExpression ?? string.Empty),
                new HashEntry("Data",Data ?? new byte[0]),
                new HashEntry("Topic",Topic),
                new HashEntry("LastExecuteTime", LastExecuteTime ?? string.Empty),
            };

            return result;
        }
    }
}
