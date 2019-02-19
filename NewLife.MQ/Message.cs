using System;
using System.IO;
using System.Xml.Serialization;
using NewLife.Data;
using NewLife.Serialization;

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
        /// <summary>从数据包解析得到消息</summary>
        /// <param name="data"></param>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static Message Read(Packet data, BinaryReader reader)
        {
            if (reader == null) reader = new BinaryReader(data.GetStream());

            // 消息格式：2长度+N属性+消息数据
            var len = reader.ReadInt16();
            var json = reader.ReadBytes(len).ToStr();

            var msg = json.ToJsonEntity<Message>();

            var len2 = reader.ReadInt16();
            msg.Body = data.Slice(2 + len + 2, len2);

            reader.BaseStream.Seek(len2, SeekOrigin.Current);

            return msg;
        }

        /// <summary>把消息打包成为数据包</summary>
        /// <returns></returns>
        public Packet ToPacket()
        {
            // 消息格式：2长度+N属性+消息数据
            var json = JsonWriter.ToJson(this, false, false, false);
            var args = json.GetBytes();
            var len = (Int16)args.Length;
#if DEBUG
            //Log.XTrace.WriteLine(json);
#endif

            var pk = new Packet(len.GetBytes());
            pk.Append(args);

            len = (Int16)Body.Total;
            pk.Append(len.GetBytes());
            pk.Append(Body);

            return pk;
        }
        #endregion

        #region 辅助
        /// <summary>已重载</summary>
        /// <returns></returns>
        public override String ToString() => "{0}@{1}[{2}]".F(ID, Topic, Body == null ? 0 : Body.Total);
        #endregion
    }
}