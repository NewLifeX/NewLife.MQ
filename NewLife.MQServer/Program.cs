using System;
using NewLife.Agent;
using NewLife.Log;
using NewLife.MessageQueue;
using NewLife.Remoting;

namespace NewLife.MQServer
{
    class Program
    {
        static void Main(String[] args) => MyService.ServiceMain();
    }

    /// <summary>服务类。名字可以自定义</summary>
    class MyService : AgentServiceBase<MyService>
    {
        public MyService()
        {
            ServiceName = "MQServer";
        }

        private ApiServer _Server;

        /// <summary>开始服务</summary>
        /// <param name="reason"></param>
        protected override void StartWork(String reason)
        {
            var set = Setting.Current;

            var svr = new ApiServer
            {
                Port = set.Port,
                Log = XTrace.Log
            };

            if (set.Debug)
            {
                svr.ShowError = true;
            }

            // 注册服务
            svr.Register<MQService>();

            MQService.Host = MQHost.Instance;
            MQService.Log = XTrace.Log;

            svr.Start();

            _Server = svr;

            base.StartWork(reason);
        }

        /// <summary>停止服务</summary>
        /// <param name="reason"></param>
        protected override void StopWork(String reason)
        {
            _Server.TryDispose();
            _Server = null;

            base.StopWork(reason);
        }
    }
}