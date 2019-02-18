using System;
using System.Collections.Concurrent;
using System.Threading;
using NewLife.Log;

namespace NewLife.MessageQueue
{
    /// <summary>主题</summary>
    /// <remarks>
    /// 每一个主题可选广播消费或集群消费，默认集群消费。
    /// </remarks>
    public class Topic
    {
        #region 属性
        /// <summary>名称</summary>
        public String Name { get; }

        /// <summary>主机</summary>
        public MQHost Host { get; internal set; }

        /// <summary>最小偏移</summary>
        public Int64 MinOffset { get; set; }

        /// <summary>最大偏移</summary>
        public Int64 MaxOffset { get; set; }

        /// <summary>消费者集群</summary>
        private ConcurrentDictionary<String, Consumer> Consumers { get; } = new ConcurrentDictionary<String, Consumer>();

        /// <summary>消息队列</summary>
        public ConcurrentQueue<Message> Queue { get; } = new ConcurrentQueue<Message>();
        #endregion

        #region 构造函数
        /// <summary>实例化</summary>
        public Topic(MQHost host, String name)
        {
            Host = host;
            Name = name;
        }
        #endregion

        #region 方法
        public Consumer GetConsumer(String user, String tag)
        {
            var cs = Consumers.GetOrAdd(user, e => new Consumer(this, e));
            if (!tag.IsNullOrEmpty()) cs.Tag = tag;

            return cs;
        }
        #endregion

        #region 进入队列
        private Int64 _gid;

        /// <summary>进入队列</summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        public Int32 Send(Message msg)
        {
            if (Queue.Count > 10000) return -1;

            msg.ID = Interlocked.Increment(ref _gid);

            Queue.Enqueue(msg);
            MaxOffset = msg.ID;

            return Consumers.Count;
        }
        #endregion

        #region 日志
        /// <summary>日志</summary>
        public ILog Log { get; set; } = Logger.Null;

        /// <summary>写日志</summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        public void WriteLog(String format, params Object[] args) => Log?.Info(format, args);
        #endregion
    }
}