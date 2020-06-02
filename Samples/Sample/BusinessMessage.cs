using Aix.MessageBus;
using System;
using System.Collections.Generic;
using System.Text;

namespace Sample
{
    [TopicAttribute(Name = "BusinessMessage")]
    public class BusinessMessage
    {
        [RouteKeyAttribute]
        public string MessageId { get; set; }
        public string Content { get; set; }

        public DateTime CreateTime { get; set; }
    }

    [TopicAttribute(Name = "BusinessMessage2")]
    public class BusinessMessage2
    {
        public string MessageId { get; set; }
        public string Content { get; set; }

        public DateTime CreateTime { get; set; }
    }

    [TopicAttribute(Name = "EventMessage")]
    public class EventBase
    {
        [RouteKeyAttribute]
        public string RouteId { get; set; }

        public string EventName { get; set; }

    }


}
