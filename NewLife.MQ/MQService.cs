using System;
using System.ComponentModel;
using NewLife.Log;
using NewLife.Model;
using NewLife.Remoting;

namespace NewLife.MessageQueue
{
    /// <summary>消息队列服务</summary>
    public class MQService : IApi
    {
        #region 属性
        /// <summary>主机</summary>
        public static MQHost Host { get; set; }

        /// <summary>会话</summary>
        public IApiSession Session { get; set; }
        #endregion

        /// <summary>登录</summary>
        /// <param name="user"></param>
        /// <param name="pass"></param>
        /// <returns></returns>
        [DisplayName("登录")]
        public Boolean Login(String user, String pass)
        {
            XTrace.WriteLine("登录 {0}/{1}", user, pass);

            if (pass != user.MD5()) throw new Exception("密码不正确");

            // 记录已登录用户
            Session["user"] = user;

            return true;
        }

        /// <summary>订阅</summary>
        /// <param name="topic">主题。沟通生产者消费者之间的桥梁</param>
        /// <param name="tag">标签。消费者用于在主题队列内部过滤消息</param>
        /// <returns></returns>
        [DisplayName("订阅主题")]
        public Boolean Subscribe(String topic, String tag)
        {
            XTrace.WriteLine("订阅主题 {0} @{1}", topic, Session["user"]);

            var tp = Host.Subscribe(null, topic, tag, null);

            var user = Session["User"] as IManageUser;

            // 退订旧的
            var old = Session["Topic"] as Topic;
            //if (old != null) old.Remove(user);

            // 订阅新的
            Session["Topic"] = tp;
            //tp.Add(user, Session);

            return true;
        }

        /// <summary>发布消息</summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        [DisplayName("发布消息")]
        public Boolean Public(Message msg)
        {
            XTrace.WriteLine("发布消息 {0}", msg);

            var user = Session["user"] as String;

            var tp = Session["Topic"] as Topic;
            if (tp == null) throw new Exception("未订阅");

            msg.Sender = user;
            tp.Send(msg);

            return true;
        }
    }
}