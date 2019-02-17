using System;
using System.ComponentModel;
using NewLife.Xml;

namespace NewLife.MQServer
{
    /// <summary>配置</summary>
    [XmlConfigFile("Config/MQServer.config", 15000)]
    public class Setting : XmlConfig<Setting>
    {
        #region 属性
        /// <summary>调试开关。默认true</summary>
        [Description("调试开关。默认true")]
        public Boolean Debug { get; set; } = true;

        /// <summary>端口</summary>
        [Description("端口。默认6789")]
        public Int32 Port { get; set; } = 6789;
        #endregion
    }
}