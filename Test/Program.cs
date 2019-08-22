using System;
using System.Linq;
using System.Threading;
using NewLife.Log;
using NewLife.MessageQueue;
using NewLife.Security;

namespace Test
{
    class Program
    {
        static void Main(String[] args)
        {
            XTrace.UseConsole();

            try
            {
                Console.Write("模式（1生产者，2消费者）：");
                if (Console.ReadLine() == "1")
                    Test1();
                else
                    Test2();
            }
            catch (Exception ex)
            {
                XTrace.WriteException(ex);
            }

            Console.WriteLine("OK!");
            Console.ReadKey();
        }

        static void Test1()
        {
            var client = new MQClient
            {
                Servers = new[] { "tcp://127.0.0.1:6789" },
                Log = XTrace.Log,

                Topic = "测试主题",
            };

            var msgid = client.PublicAsync("发布测试").Result;
            XTrace.WriteLine("msgid={0}", msgid);

            while (true)
            {
                for (var i = 0; i < 10; i++)
                {
                    Thread.Sleep(200);

                    msgid = client.PublicAsync(Rand.NextString(16)).Result;
                    XTrace.WriteLine("msgid={0}", msgid);
                }

                var key = Console.ReadKey(true);
                if (key.Key != ConsoleKey.C) break;
            }
        }

        static void Test2()
        {
            var client = new MQClient
            {
                Servers = new[] { "tcp://127.0.0.1:6789" },
                Log = XTrace.Log,

                Topic = "测试主题",
            };

            client.OnConsume = msgs =>
            {
                foreach (var item in msgs)
                {
                    XTrace.WriteLine("消费到 {0}", item);
                }

                //return msgs.Max(e => e.ID);
            };
            client.StartConsume();
        }

        static void Test3()
        {

        }
    }
}