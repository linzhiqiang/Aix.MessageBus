using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Redis.Model
{
  public  class JobData
    {
        public static JobData CreateJobData(byte[] data, string queue)
        {
            return new JobData
            {
                JobId = Guid.NewGuid().ToString(),
                Data = data,
                CreateTime = DateTime.Now,
                Status = 0,
                ErrorCount = 0,
                Queue = queue
            };
        }
    public string JobId { get; set; }

        /// <summary>
        /// 创建时间
        /// </summary>
        public DateTime CreateTime { get; set; }
        /// <summary>
        /// 执行时间 
        /// </summary>
        public DateTime? ExecuteTime { get; set; }

        /// <summary>
        /// 0 待执行，1 执行中，2 成功，9 失败
        /// </summary>
        public int Status { get; set; }

        /// <summary>
        /// 业务数据
        /// </summary>
        public byte[] Data { get; set; }

        public int ErrorCount { get; set; }

        public DateTime? CheckedTime { get; set; }

        public string Queue { get; set; }

        public List<HashEntry> ToDictionary()
        {
            var result = new List<HashEntry>
            {
                new HashEntry("JobId",JobId),
                new HashEntry("CreateTime",TimeToString(CreateTime)),
                new HashEntry("ExecuteTime",TimeToString(ExecuteTime)),
                new HashEntry("Status",Status),
                new HashEntry("Data",Data),
                new HashEntry("ErrorCount",ErrorCount),
                new HashEntry("CheckedTime",TimeToString(CheckedTime)),
                new HashEntry("Queue",Queue)
            };

            return result;
        }

        private static string TimeToString(DateTime? time)
        {
            if (time != null) return time.Value.ToString("yyyy-MM-dd HH:mm:ss");
            return string.Empty;
        }

    }
}
