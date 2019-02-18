using System;

namespace NewLife.MessageQueue
{
    /// <summary>消费者。相同标识的多个订阅者，构成消费组，共享消费一个队列</summary>
    public class Consumer
    {
        /// <summary>主题</summary>
        public Topic Topic { get; }

        /// <summary>用户</summary>
        public String User { get; }

        /// <summary>标记</summary>
        public String Tag { get; set; }

        /// <summary>偏移量。消费位置</summary>
        public Int64 Offset { get; set; }

        /// <summary>确认偏移量。已确认的消费位置</summary>
        public Int64 CommitOffset { get; set; }

        /// <summary>实例化</summary>
        /// <param name="topic"></param>
        /// <param name="user"></param>
        public Consumer(Topic topic, String user)
        {
            Topic = topic;
            User = user;
        }
    }
}