using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using NewLife.Data;
using NewLife.Log;
using NewLife.Net;
using NewLife.Reflection;
using NewLife.Remoting;
using NewLife.Serialization;

namespace NewLife.MessageQueue
{
    /// <summary>MQ客户端</summary>
    public class MQClient : ApiClient
    {
        #region 属性
        /// <summary>主题</summary>
        public String Topic { get; set; }

        /// <summary>用户名</summary>
        public String UserName { get; set; }

        /// <summary>密码</summary>
        public String Password { get; set; }

        /// <summary>是否已登录</summary>
        public Boolean Logined { get; set; }

        /// <summary>最后一次登录成功后的消息</summary>
        public IDictionary<String, Object> Info { get; private set; }
        #endregion

        #region 构造函数
        /// <summary>实例化</summary>
        public MQClient()
        {
        }
        #endregion

        #region 登录
        /// <summary>连接后自动登录</summary>
        /// <param name="client">客户端</param>
        /// <param name="force">强制登录</param>
        protected override async Task<Object> OnLoginAsync(ISocketClient client, Boolean force)
        {
            if (Logined && !force) return null;

            var asmx = AssemblyX.Entry;
            if (UserName.IsNullOrEmpty()) UserName = asmx?.Name;

            var arg = new
            {
                user = UserName,
                pass = Password.MD5(),
                machine = Environment.MachineName,
                processid = Process.GetCurrentProcess().Id,
                version = asmx?.Version,
                compile = asmx?.Compile,
            };

            var rs = await base.InvokeWithClientAsync<Object>(client, "MQ/Login", arg);
            if (Setting.Current.Debug) XTrace.WriteLine("登录{0}成功！{1}", client, rs.ToJson());

            Logined = true;

            return Info = rs as IDictionary<String, Object>;
        }
        #endregion

        #region 发布
        /// <summary>发布消息</summary>
        /// <param name="msg">消息</param>
        /// <returns></returns>
        public async Task<Int64> Public(Message msg)
        {
            Log.Info("{0} 发布消息 {1}", Name, msg);

            if (msg.Topic.IsNullOrEmpty()) msg.Topic = Topic;
            if (msg.CreateTime.Year < 2000) msg.CreateTime = DateTime.Now;

            var pk = msg.ToPacket();

            return await base.InvokeAsync<Int64>("MQ/Public", pk);
        }

        /// <summary>发布消息</summary>
        /// <param name="body">消息体</param>
        /// <param name="tag">标签</param>
        /// <param name="key">主键</param>
        /// <returns></returns>
        public async Task<Int64> Public(Object body, String tag = null, String key = null)
        {
            if (!(body is Packet pk))
            {
                if (!(body is Byte[] buf))
                {
                    if (!(body is String str)) str = body.ToJson();

                    buf = str.GetBytes();
                }
                pk = new Packet(buf);
            }

            var msg = new Message
            {
                Body = pk
            };

            if (!tag.IsNullOrEmpty()) msg.Tag = tag;
            if (!key.IsNullOrEmpty()) msg.Key = key;

            return await Public(msg);
        }
        #endregion

        #region 消费
        /// <summary>拉取的批大小。默认32</summary>
        public Int32 BatchSize { get; set; } = 32;

        public event EventHandler<EventArgs> OnConsume;

        public async Task<Message[]> Pull(Int64 offset, Int32 maxNums, Int32 msTimeout)
        {
            var pk = await InvokeAsync<Packet>("MQ/Pull", new { offset, maxNums, msTimeout });
            if (pk == null || pk.Total == 0) return new Message[0];

            var ms = pk.GetStream();
            var reader = new BinaryReader(ms);
            var count = reader.ReadInt16();

            var list = new List<Message>();
            while (count-- > 0)
            {
                var pk2 = pk.Slice((Int32)ms.Position);
                var msg = Message.Read(pk, reader);
                list.Add(msg);
            }

            return null;
        }
        #endregion
    }
}