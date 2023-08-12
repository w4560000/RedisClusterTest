using StackExchange.Redis;

namespace RedisClusterTest
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var configuration = new ConfigurationOptions()
            {
                EndPoints = {
                        { "10.240.0.11:6379" },
                        { "10.240.0.12:6379" },
                        { "10.240.0.13:6379" },
                        { "10.240.0.14:6379" },
                        { "10.240.0.15:6379" },
                        { "10.240.0.16:6379" },
                    },
                //EndPoints = {
                //    "35.194.230.192:6379"
                //},
                AbortOnConnectFail = true,
                ConnectTimeout = 10000,
                //SyncTimeout = 10000,
                ConnectRetry = 5
            };

            var redisConnectionManager = ConnectionMultiplexer.Connect(configuration);// new RedisConnectionManager(configuration, retryPolicy);

            while (true)
            {
                var key1 = redisConnectionManager.GetDatabase().StringGet("Key1");
                var key2 = redisConnectionManager.GetDatabase().StringGet("Key2");
                var key3 = redisConnectionManager.GetDatabase().StringGet("Key3");
                var key4 = redisConnectionManager.GetDatabase().StringGet("Key4");
                //Console.WriteLine($"是否已連接: {redisConnection.IsConnected}");
                //Console.WriteLine($"Key1:{key1}");
                Console.WriteLine($"Key2:{key2}");
                Console.WriteLine($"Key3:{key3}");
                Console.WriteLine($"Key4:{key4}");
                Console.WriteLine();
                Thread.Sleep(1000);
            }


            Console.WriteLine("Hello, World!");
        }
    }
}