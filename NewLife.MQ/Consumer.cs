using System;
using System.Collections.Generic;

namespace NewLife.MessageQueue
{
    /// <summary>消费者。相同标识的多个订阅者，构成消费组，共享消费一个队列</summary>
    public class Consumer
    {
        #region 属性
        /// <summary>主题</summary>
        public Topic Topic { get; }

        /// <summary>用户</summary>
        public String User { get; }

        /// <summary>标记</summary>
        public String Tag { get; set; }

        /// <summary>偏移量。消费位置</summary>
        public Int64 Offset { get; set; } = 1;

        /// <summary>确认偏移量。已确认的消费位置</summary>
        public Int64 CommitOffset { get; set; }
        #endregion

        #region 构造
        /// <summary>实例化</summary>
        /// <param name="topic"></param>
        /// <param name="user"></param>
        public Consumer(Topic topic, String user)
        {
            Topic = topic;
            User = user;
        }
        #endregion

        #region 消费
        /// <summary>拉取指定个数消息</summary>
        /// <param name="maxNums"></param>
        /// <returns></returns>
        public IList<Message> Pull(Int32 maxNums)
        {
            var tp = Topic;
            if (tp.Count <= 0 || Offset > tp.MaxOffset) return null;

            lock (this)
            {
                var list = new List<Message>();
                while (maxNums > 0)
                {
                    var msg = tp.Get(Offset);
                    if (msg == null) break;

                    list.Add(msg);

                    Offset++;
                    maxNums--;
                }

                return list;
            }
        }
        #endregion
    }
}