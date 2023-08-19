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
                    var value = _connectionMultiplexer.GetDatabase().StringGet(key, flags: CommandFlags.NoRedirect);

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
                    _connectionMultiplexer.GetDatabase().StringSet(key, data, flags: CommandFlags.NoRedirect);
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
                        .WaitAndRetry(3, _ => TimeSpan.FromSeconds(1), (exception, retryCount) =>
                        {
                            Console.WriteLine($"{DateTime.Now} Redis connection failed. Retrying ({retryCount})...");
                        });


            var configuration = new ConfigurationOptions()
            {
                EndPoints = {
                        { "10.240.0.12:6379" }
                    },
                AbortOnConnectFail = true,
                ConnectTimeout = 1000,
                SyncTimeout = 1000,
                ConnectRetry = 5
            };

            var redisConnectionManager = new RedisConnectionManager(configuration, retryPolicy);

            while (true)
            {
                var value = redisConnectionManager.Get<string>("Key1");
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} Key1 = {value}");

                var newValue = Convert.ToInt32(value) + 1;

                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} Key1 預計更新為 {newValue}");
                redisConnectionManager.Update("Key1", newValue.ToString());
                Console.WriteLine($"更新後確認 Key1 = {redisConnectionManager.Get<string>("Key1")}\n");
                Thread.Sleep(1000);
            }
        }
    }
}