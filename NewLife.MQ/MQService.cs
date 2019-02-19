using System;
using System.Threading;
using NewLife.Data;
using NewLife.Log;
using NewLife.Net;
using NewLife.Remoting;

namespace NewLife.MessageQueue
{
    /// <summary>消息队列服务</summary>
    [Api("MQ")]
    public class MQService : IApi, IActionFilter
    {
        #region 属性
        /// <summary>主机</summary>
        public static MQHost Host { get; set; }

        /// <summary>会话</summary>
        public IApiSession Session { get; set; }
        #endregion

        #region 登录
        /// <summary>
        /// 传入应用名和密钥登陆，
        /// 返回应用名和应用显示名
        /// </summary>
        /// <param name="user">应用名</param>
        /// <param name="pass"></param>
        /// <returns></returns>
        [Api(nameof(Login))]
        public Object Login(String user, String pass)
        {
            if (user.IsNullOrEmpty()) throw new ArgumentNullException(nameof(user));
            if (pass.IsNullOrEmpty()) throw new ArgumentNullException(nameof(pass));

            var ns = Session as INetSession;
            var ip = ns.Remote.Host;
            var ps = ControllerContext.Current.Parameters;

            WriteLog("[{0}]从[{1}]登录", user, ns.Remote);

            //// 找应用
            //var app = App.FindByName(user);
            //if (app == null || app.Secret.IsNullOrEmpty())
            //{
            //    if (app == null) app = new App();

            //    if (app.ID == 0)
            //    {
            //        app.Name = user;
            //        //app.Secret = pass;
            //        app.CreateIP = ip;
            //        app.CreateTime = DateTime.Now;
            //        app.Enable = true;
            //    }

            //    var name = ps["name"] + "";
            //    if (!name.IsNullOrEmpty()) app.DisplayName = name;

            //    app.UpdateIP = ip;
            //    app.UpdateTime = DateTime.Now;

            //    app.Save();
            //}

            //if (!app.Enable) throw new Exception("已禁用！");

            //// 核对密码
            //if (!app.Secret.IsNullOrEmpty())
            //{
            //    var pass2 = app.Secret.MD5();
            //    if (pass != pass2) throw new Exception("密码错误！");
            //}

            //// 应用上线
            //CreateOnline(app, ns, ps);

            //app.LastIP = ip;
            //app.LastLogin = DateTime.Now;
            //app.Save();

            // 记录当前用户
            //Session["App"] = app;
            Session["User"] = user;

            //return new
            //{
            //    app.Name,
            //    app.DisplayName,
            //};

            return new { Name = user };
        }

        void IActionFilter.OnActionExecuting(ControllerContext filterContext)
        {
            var act = filterContext.ActionName;
            if (act.EndsWithIgnoreCase("/Login")) return;

            if (Session["User"] is String app)
            {
                //var online = GetOnline(app, Session as INetSession);
                //online.UpdateTime = DateTime.Now;
                //online.SaveAsync();
            }
            else
            {
                var ns = Session as INetSession;
                throw new ApiException(401, "{0}未登录！不能执行{1}".F(ns.Remote, act));
            }
        }

        void IActionFilter.OnActionExecuted(ControllerContext filterContext)
        {
            var ex = filterContext.Exception;
            if (ex != null && !filterContext.ExceptionHandled)
            {
                // 显示错误
                if (ex is ApiException)
                    XTrace.Log.Error(ex.Message);
                else
                    XTrace.WriteException(ex);
            }
        }
        #endregion

        #region 核心方法
        /// <summary>发布消息</summary>
        /// <param name="data"></param>
        /// <returns></returns>
        [Api(nameof(Public))]
        public Int64 Public(Packet data)
        {
            // 解析消息
            var msg = Message.Read(data, null);

            var user = Session["User"] as String;
            msg.Sender = user;

#if DEBUG
            XTrace.WriteLine("[{0}]发布 {1}", user, msg);
#endif

            Host.Send(msg);

            return msg.ID;
        }

        /// <summary>长连接拉取消息</summary>
        /// <param name="topic"></param>
        /// <param name="maxNums"></param>
        /// <param name="msTimeout"></param>
        /// <returns></returns>
        [Api(nameof(Pull))]
        public Packet Pull(String topic, Int32 maxNums, Int32 msTimeout)
        {
            ////todo 待填充拉取消息
            //var list = new Message[16];
            //for (var i = 0; i < list.Length; i++)
            //{
            //    list[i] = new Message { BodyString = Rand.NextString(16) };
            //}

            var user = Session["User"] as String;

#if DEBUG
            XTrace.WriteLine("[{0}]拉取 {1}", user, maxNums);
#endif
            var list = Host.Pull(user, topic, maxNums);

            // 如果没有消息，则阻塞等待
            if (list == null || list.Count == 0)
            {
                var end = DateTime.Now.AddMilliseconds(msTimeout);
                do
                {
                    Thread.Sleep(10);

                    list = Host.Pull(user, topic, maxNums);
                    if (list != null && list.Count > 0) break;
                } while (DateTime.Now < end);
            }
            if (list == null || list.Count == 0) return null;

            // 写入个数后，链式输出
            var count = (Int16)list.Count;
            var pk = new Packet(count.GetBytes());

            foreach (var item in list)
            {
                var pk2 = item.ToPacket();
                pk.Append(pk2);
            }

            return pk;
        }

        /// <summary>确认偏移量</summary>
        /// <param name="topic"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        [Api(nameof(Commit))]
        public Int64 Commit(String topic, Int64 offset)
        {
            var user = Session["User"] as String;

#if DEBUG
            XTrace.WriteLine("[{0}]提交 {1}", user, offset);
#endif
            return Host.Commit(user, topic, offset);
        }
        #endregion

        #region 日志
        /// <summary>日志</summary>
        public static ILog Log { get; set; }

        /// <summary>写日志</summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        public static void WriteLog(String format, params Object[] args) => Log?.Info(format, args);
        #endregion
    }
}