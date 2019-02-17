using NewLife.Remoting;

namespace NewLife.MessageQueue
{
    /// <summary>MQ会话</summary>
    [Api(null)]
    public class MQSession : IApi
    {
        public IApiSession Session { get; set; }
    }
}