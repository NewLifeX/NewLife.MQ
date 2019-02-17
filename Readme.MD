## 消息队列 NewLife.MQ
NewLife.MQ 是一个轻量级消息队列，无延迟分发，支持消费组和消息去重，不支持集群部署。  

主要特点：  
+ 轻量级。服务端和客户端都是通过`nuget`引用`NewLife.MQ`即可，也可以在Windows上直接安装发行版`NewLife.MQServer`。  

```csharp
public static async Task Main2()
{
    var host = new MQHost();
    host.Log = XTrace.Log;
    host.Tip = true;

    host.Subscribe("aaa", "ttt", null, OnMessage);
    host.Subscribe("bbb", "ttt", "t1||t2", OnMessage);
    host.Subscribe("ccc", "ttt", "t1||t3", OnMessage, 111);
    host.Subscribe("ccc", "ttt", "t1||t3", OnMessage, 222);

    var tags = "t1,t2,t3,t4".Split(",");
    for (int i = 0; i < 1000; i++)
    {
        Console.WriteLine();
        host.Send("大石头", "ttt", tags[Rand.Next(tags.Length)], Rand.NextString(16));
        Thread.Sleep(1000);
    }
}

static async Task OnMessage(Subscriber sb, Message m)
{
    await Task.Delay(Rand.Next(200));
    XTrace.WriteLine("{0}=>{3} [{1}]: {2} {4}", m.Sender, m.Tag, m.Content, sb.Host.User, sb.User);
}
```

主要术语：
+ `MQHost` 消息队列主机，管理多主题的订阅和发布  
+ `Topic` 主题队列，每个主题有一个队列用于缓冲保存消息，同时记录多个消费者  
+ `Consumer` 消费者，发布到主题的消息，会广播给各个消费者。多个订阅者构成消费者集群，消息只推送给其中一个订阅者。  
+ `Subscriber` 订阅者，使用相同消费者标识的不同来源，视为不同订阅者，比如同一个消费者账号的不同网络连接。  
## 新生命开源项目矩阵

| 项目 | 年份 | 状态 | .NET Core | 说明 |
| :------: | :------: | :------: | :------: | ------ |
| [NewLife.Core](https://github.com/NewLifeX/X) | 2002 | 维护中 | √ | 日志、网络、RPC、序列化、缓存、Windows服务、多线程 |
| [XCode](https://github.com/NewLifeX/X) | 2005 | 维护中 | √ | 数据中间件，MySQL、SQLite、SqlServer、Oracle |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | 2010 | 维护中 | √ | Web魔方，权限基础框架，集成OAuth |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | 2005 | 维护中 | √ | 网络库，千万级吞吐率，学习gRPC、Thrift |
| [XAgent](https://github.com/NewLifeX/XAgent) | 2008 | 重构中 | √ | 集群节点分布式计算平台，学习Hadoop |
| [Stardust](https://github.com/NewLifeX/Stardust) | 2018 | 设计中 | √ | 星尘，微服务平台，学习Dubbo |
| [XCoder](https://github.com/NewLifeX/XCoder) | 2006 | 维护中 | × | 码神工具，开发者必备 |
| [XProxy](https://github.com/NewLifeX/XProxy) | 2005 | 维护中 | √ | 产品级反向代理 |
| [XScript](https://github.com/NewLifeX/XScript) | 2010 | 维护中 | × | C#脚本引擎 |
| [XTemplate](https://github.com/NewLifeX/XTemplate) | 2008 | 维护中 | × | 模版引擎，学习TT |
| [NewLife.DNS](https://github.com/NewLifeX/NewLife.DNS) | 2011 | 维护中 | × | DNS代理服务器 |
| [NewLife.CMX](https://github.com/NewLifeX/NewLife.CMX) | 2013 | 维护中 | × | 内容管理系统 |
| [SmartOS](https://github.com/NewLifeX/SmartOS) | 2014 | 保密中 | C++11 | 嵌入式操作系统，面向ARM Cortex-M芯片架构 |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | 2015 | 维护中 | × | Git管理系统 |
| [XLink](https://github.com/NewLifeX/XLink) | 2016 | 保密中 | √ | 物联网云平台 |
| [NoDb](https://github.com/NewLifeX/NoDb) | 2017 | 开发中 | √ | NoSQL数据库，百万级读写性能，学习Redis+MongoDB |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | 2017 | 维护中 | √ | Redis客户端，微秒级延迟，百亿级项目验证 |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | 2018 | 开发中 | √ | MySql驱动 |
| [NewLife.Cache](https://github.com/NewLifeX/NewLife.Cache) | 2018 | 维护中 | √ | 自定义缓存服务器 |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | 2018 | 维护中 | √ | 支持Apache RocketMQ和阿里云消息队列 |
| NewLife.Kafka | 2019 | 设计中 | √ | Kafka组件 |
| ConfigServer | 2019 | 设计中 | √ | 分布式配置中心，学习Apollo |
| [X组件 .NET2.0](https://github.com/NewLifeX/X_NET20) | 2002 | 存档中 | .NET2.0 | 日志、网络、RPC、序列化、缓存、Windows服务、多线程 |
| [X组件 .NET4.0](https://github.com/NewLifeX/X_NET40) | 2002 | 存档中 | .NET4.0 | 日志、网络、RPC、序列化、缓存、Windows服务、多线程 |

#### 新生命开发团队
网站：http://www.NewLifeX.com
博客：https://nnhy.cnblogs.com
QQ群：1600800
