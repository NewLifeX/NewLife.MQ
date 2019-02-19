using System;
using System.Collections.Concurrent;
using System.Threading;
using NewLife.Threading;

namespace NewLife.MessageQueue
{
    /// <summary>主题</summary>
    /// <remarks>
    /// 每一个主题可选广播消费或集群消费，默认集群消费。
    /// </remarks>
    public class Topic : DisposeBase
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

        private Int32 _Count;
        /// <summary>消息个数</summary>
        public Int32 Count { get => _Count; set => _Count = value; }

        /// <summary>容量。超过时删除，默认100_000</summary>
        public Int32 Capacity { get; set; } = 100_000;

        /// <summary>过期时间。默认2天</summary>
        public TimeSpan Expire { get; set; } = TimeSpan.FromDays(2);

        /// <summary>消费者集群</summary>
        private ConcurrentDictionary<String, Consumer> Consumers { get; } = new ConcurrentDictionary<String, Consumer>();

        /// <summary>消息队列。基于消息编号定位</summary>
        public ConcurrentDictionary<Int64, Message> Queue { get; } = new ConcurrentDictionary<Int64, Message>();
        #endregion

        #region 构造函数
        /// <summary>实例化</summary>
        public Topic(MQHost host, String name)
        {
            Host = host;
            Name = name;
        }

        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            _timer.TryDispose();
        }
        #endregion

        #region 方法
        /// <summary>获取消费者</summary>
        /// <param name="user"></param>
        /// <param name="tag"></param>
        /// <returns></returns>
        public Consumer GetConsumer(String user, String tag)
        {
            var cs = Consumers.GetOrAdd(user, e => new Consumer(this, e));
            if (!tag.IsNullOrEmpty()) cs.Tag = tag;

            return cs;
        }
        #endregion

        #region 收发消息
        private Int64 _gid;

        /// <summary>进入队列</summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        public Int32 Send(Message msg)
        {
            //if (Queue.Count > 10000) return -1;

            // 设定创建时间，用于过期删除
            if (msg.CreateTime.Year < 2000) msg.CreateTime = DateTime.Now;

            // 自增消息ID
            var id = msg.ID = Interlocked.Increment(ref _gid);

            // 进入队列，基于消息ID的字典，便于查找和删除
            if (!Queue.TryAdd(id, msg)) return -1;

            // 更新最大最小偏移量
            MaxOffset = id;
            if (MinOffset == 0 || MinOffset > id) MinOffset = id;

            // 增加消息数
            var count = Interlocked.Increment(ref _Count);

            // 定时器
            if (_timer == null)
            {
                lock (this)
                {
                    if (_timer == null) _timer = new TimerX(DoCheckQueue, null, 5_000, 5_000) { CanExecute = () => Count > 0, Async = true };
                }
            }

            return count;
        }

        /// <summary>获取消息</summary>
        /// <param name="msgid"></param>
        /// <returns></returns>
        public Message Get(Int64 msgid) => Queue.TryGetValue(msgid, out var msg) ? msg : null;

        private TimerX _timer;
        private void DoCheckQueue(Object state)
        {
            var count = Count;
            if (count == 0) return;

            var dic = Queue;

            // 容量超限，删除旧数据
            var n = count - Capacity;
            if (n > 0)
            {
                for (var i = 0; i < n; i++)
                {
                    dic.TryRemove(MinOffset++, out _);
                }

                Interlocked.Add(ref _Count, -n);
            }

            // 检查过期
            var exp = DateTime.Now.Subtract(Expire);
            for (var i = MinOffset; i < MaxOffset; i++)
            {
                if (!dic.TryGetValue(i, out var msg)) break;

                if (msg.CreateTime < exp && dic.TryRemove(i, out _)) Interlocked.Decrement(ref _Count);
            }
        }
        #endregion
    }
}