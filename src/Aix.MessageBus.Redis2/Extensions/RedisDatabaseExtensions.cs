using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Aix.MessageBus.Redis2.Extensions
{
  public static  class RedisDatabaseExtensions
    {
        public static HashEntry[] ToHashEntries(this IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            var hashEntry = new HashEntry[keyValuePairs.Count()];
            int i = 0;
            foreach (var kvp in keyValuePairs)
            {
                hashEntry[i] = new HashEntry(kvp.Key, kvp.Value);
                i++;
            }
            return hashEntry;
        }
    }
}
