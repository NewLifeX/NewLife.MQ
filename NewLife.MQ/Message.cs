using System;
using System.Xml.Serialization;
using NewLife.Data;

namespace NewLife.MessageQueue
{
    /// <summary>消息</summary>
    public class Message
    {
        #region 属性
        /// <summary>唯一标识</summary>
        public Int64 ID { get; set; }

        /// <summary>主题</summary>
        public String Topic { get; set; }

        /// <summary>标签</summary>
        public String Tag { get; set; }

        /// <summary>键</summary>
        public String Key { get; set; }

        /// <summary>发送者</summary>
        public String Sender { get; set; }

        /// <summary>创建时间</summary>
        public DateTime CreateTime { get; set; }

        /// <summary>激活时间。消息等到该时间点才允许消费</summary>
        public DateTime ActiveTime { get; set; }

        /// <summary>消息体</summary>
        [XmlIgnore]
        public Packet Body { get; set; }

        /// <summary>消息体。字符串格式</summary>
        [XmlIgnore]
        public String BodyString { get => Body?.ToStr(); set => Body = value?.GetBytes(); }
        #endregion

        #region 方法
        #endregion

        #region 辅助
        /// <summary>已重载</summary>
        /// <returns></returns>
        public override String ToString() => "{0}@{1}[{2}]".F(ID, Topic, Body == null ? 0 : Body.Total);
        #endregion
    }
}