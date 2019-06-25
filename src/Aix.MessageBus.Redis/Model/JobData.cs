using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace Aix.MessageBus.Redis.Model
{
  public  class JobData
    {
        public static JobData CreateJobData(byte[] data)
        {
            return new JobData
            {
                JobId = Guid.NewGuid().ToString(),
                Data = data,
                CreateTime = DateTime.Now
            };
        }
    public string JobId { get; set; }

        /// <summary>
        /// 创建时间
        /// </summary>
        public DateTime CreateTime { get; set; }

        /// <summary>
        /// 业务数据
        /// </summary>
        public byte[] Data { get; set; }

        public DateTime? ExecuteTime { get; set; }

        public List<HashEntry> ToDictionary()
        {
            var result = new List<HashEntry>
            {
                new HashEntry("JobId",JobId),
                new HashEntry("CreateTime",TimeToString(CreateTime)),
                new HashEntry("ExecuteTime", TimeToString(ExecuteTime)),
                new HashEntry("Data",Data)
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
