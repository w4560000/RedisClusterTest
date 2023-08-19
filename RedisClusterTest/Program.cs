using Newtonsoft.Json;
using Polly;
using Polly.Retry;
using StackExchange.Redis;

namespace RedisClusterTest
{
    public class RedisConnectionManager
    {
        private readonly object _lock = new object();
        private IConnectionMultiplexer _connectionMultiplexer;
        private readonly ConfigurationOptions _configurationOptions;
        private readonly RetryPolicy _retryPolicy;

        public RedisConnectionManager(ConfigurationOptions configurationOptions, RetryPolicy retryPolicy)
        {
            _configurationOptions = configurationOptions;
            _retryPolicy = retryPolicy;
            SetConnection();
        }

        public IConnectionMultiplexer SetConnection()
        {
            return _retryPolicy.Execute(() =>
            {
                if (_connectionMultiplexer == null || !_connectionMultiplexer.IsConnected)
                {
                    lock (_lock)
                    {
                        _connectionMultiplexer?.Close();
                        _connectionMultiplexer = ConnectionMultiplexer.Connect(_configurationOptions);
                    }
                }

                return _connectionMultiplexer;
            });
        }

        public T? Get<T>(string key)
        {
            return _retryPolicy.Execute(() =>
            {
                try
                {
                    var value = _connectionMultiplexer.GetDatabase().StringGet(key);

                    if (value.IsNullOrEmpty)
                        return default;

                    return JsonConvert.DeserializeObject<T>(value);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Get 發生錯誤, Error:{ex.Message}");
                    throw ex;
                }
            });
        }

        public void Update(string key, string data)
        {
            _retryPolicy.Execute(() =>
            {
                try
                {
                    _connectionMultiplexer.GetDatabase().StringSet(key, data);
                    Console.WriteLine("已更新");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Update 發生錯誤, Error:{ex.Message}");
                    throw ex;
                }
            });
        }
    }

    internal class Program
    {
        static void Main(string[] args)
        {

            var retryPolicy = Policy.Handle<RedisConnectionException>()
                        .Or<RedisTimeoutException>()
                        .Or<RedisServerException>()
                        .Retry(3, (exception, retryCount) =>
                        {
                            Console.WriteLine($"Redis connection failed. Retrying ({retryCount})...");
                        });


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
                ConnectTimeout = 1000,
                SyncTimeout = 1000,
                ConnectRetry = 5
            };

            //var redisConnectionManager =  new RedisConnectionManager(configuration, retryPolicy);

            var redisConnectionManager = new RedisConnectionManager(configuration, retryPolicy);

            while (true)
            {
                var key1 = redisConnectionManager.Get<string>("Key1");
                var key2 = redisConnectionManager.Get<string>("Key2");
                var key3 = redisConnectionManager.Get<string>("Key3");
                var key4 = redisConnectionManager.Get<string>("Key4");
                //Console.WriteLine($"是否已連接: {redisConnection.IsConnected}");
                Console.WriteLine(DateTime.Now);
                Console.WriteLine($"Key1:{key1}");
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