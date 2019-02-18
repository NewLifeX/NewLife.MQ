using System;
using System.Collections.Generic;
using System.Diagnostics;
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

        /// <summary>客户端标识</summary>
        public String ClientId { get; set; }

        /// <summary>消费组</summary>
        public String Group { get; set; }

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
            ClientId = $"{NetHelper.MyIP()}@{Process.GetCurrentProcess().Id}";
        }

        /// <summary>实例化</summary>
        /// <param name="uri"></param>
        public MQClient(String uri)
        {
            if (!uri.IsNullOrEmpty())
            {
                var u = new Uri(uri);

                Servers = new[] { "{2}://{0}:{1}".F(u.Host, u.Port, u.Scheme) };

                var us = u.UserInfo.Split(":");
                if (us.Length > 0) UserName = us[0];
                if (us.Length > 1) Password = us[1];
            }
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

        #region 发布订阅
        ///// <summary>发布主题</summary>
        ///// <param name="topic"></param>
        ///// <returns></returns>
        //public async Task<Boolean> CreateTopic(String topic)
        //{
        //    Open();

        //    Log.Info("{0} 创建主题 {1}", Name, topic);

        //    var rs = await Client.InvokeAsync<Boolean>("Topic/Create", new { topic });

        //    return rs;
        //}

        /// <summary>订阅主题</summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        public async Task<Boolean> Subscribe(String topic)
        {
            Open();

            Log.Info("{0} 订阅主题 {1}", Name, topic);

            var rs = await InvokeAsync<Boolean>("MQ/Subscribe", new { topic });

            //if (rs) Client.Register<ClientController>();

            return rs;
        }
        #endregion

        #region 收发消息
        /// <summary>发布消息</summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        public async Task<Int64> Public(Packet msg)
        {
            Log.Info("{0} 发布消息 {1}", Name, msg);

            var m = new Message
            {
                Topic = Topic,
                //Sender = Name,
                CreateTime = DateTime.Now,
                //ExpireTime = DateTime.Now.AddSeconds(60),
                Body = msg
            };

            var rs = await InvokeAsync<Int64>("MQ/Public", new { msg = m });

            return rs;
        }

        public async Task<Int64> Public(String msg) => await Public(msg.GetBytes());
        #endregion
    }
}