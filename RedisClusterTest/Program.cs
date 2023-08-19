using Newtonsoft.Json;
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

        public RedisConnectionManager(ConfigurationOptions configurationOptions, RetryPolicy retryPolicy, TextWriter log)
        {
            _configurationOptions = configurationOptions;
            _retryPolicy = retryPolicy;
            SetConnection(log);
        }

        public IConnectionMultiplexer SetConnection(TextWriter log)
        {
            return _retryPolicy.Execute(() =>
            {
                if (_connectionMultiplexer == null || !_connectionMultiplexer.IsConnected)
                {
                    lock (_lock)
                    {
                        _connectionMultiplexer?.Close();
                        _connectionMultiplexer = ConnectionMultiplexer.Connect(_configurationOptions, log);
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
        private readonly object _lock = new object();
        private static IConnectionMultiplexer _masterConnectionMultiplexer;
        private static IConnectionMultiplexer _replicaConnectionMultiplexer;
        private static IConnectionMultiplexer _sentinelConnectionMultiplexer;

        private static void Main(string[] args)
        {
            InitSentinelConnection();
            ResetConnection();

            while (true)
            {
                try
                {
                    var value = _replicaConnectionMultiplexer.GetDatabase().StringGet("Key1");
                    Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} Key1 = {value}");
                    var newValue = Convert.ToInt32(value) + 1;
                    Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} Key1 預計更新為 {newValue}");
                    _masterConnectionMultiplexer.GetDatabase().StringSet("Key1", newValue.ToString());
                    Console.WriteLine($"更新後確認 Key1 = {_replicaConnectionMultiplexer.GetDatabase().StringGet("Key1")}\n");

                    Thread.Sleep(1000);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + $" Error {ex.Message}");
                }
            }

            Console.ReadLine();

            //    var retryPolicy = Policy.Handle<RedisConnectionException>()
            //                .Or<RedisTimeoutException>()
            //                .Or<RedisServerException>()
            //                .WaitAndRetry(3, _ => TimeSpan.FromSeconds(1), (exception, retryCount) =>
            //                {
            //                    Console.WriteLine($"{DateTime.Now} Redis connection failed. Retrying ({retryCount})...");
            //                });

            //    var configuration = new ConfigurationOptions()
            //    {
            //        EndPoints = {
            //                { "34.80.222.88:6379" },
            //                { "34.81.158.85:6379" },
            //                { "35.229.161.113:6379" },
            //                { "34.81.112.56:6379" },
            //                { "107.167.177.175:6379" },
            //                { "35.221.130.206:6379" },
            //            },
            //        AbortOnConnectFail = false,
            //        ConnectTimeout = 1000,
            //        SyncTimeout = 10000,
            //        ConnectRetry = 5
            //    };

            //    using (TextWriter log = File.CreateText("D:\\redis_log.txt"))
            //    {
            //        var redisConnectionManager = new RedisConnectionManager(configuration, retryPolicy, log);

            //        var value = redisConnectionManager.Get<string>("Key1");
            //        Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} Key1 = {value}");

            //        var newValue = Convert.ToInt32(value) + 1;

            //        Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} Key1 預計更新為 {newValue}");
            //        redisConnectionManager.Update("Key1", newValue.ToString());
            //        Console.WriteLine($"更新後確認 Key1 = {redisConnectionManager.Get<string>("Key1")}\n");
            //        Thread.Sleep(1000);
            //    }
        }

        private static void InitSentinelConnection()
        {
            var sentinelConfig = new ConfigurationOptions
            {
                EndPoints = {
                        { "34.80.222.88:26379" }
                    },
                AbortOnConnectFail = false,
                ServiceName = "mymaster",
                TieBreaker = string.Empty,
                CommandMap = CommandMap.Sentinel,
                AllowAdmin = true,
                Ssl = false,
                ConnectTimeout = 1000,
                SyncTimeout = 1000,
                ConnectRetry = 5,
            };

            _sentinelConnectionMultiplexer = ConnectionMultiplexer.SentinelConnect(sentinelConfig);
            ISubscriber subscriber = _sentinelConnectionMultiplexer.GetSubscriber();

            subscriber.Subscribe("+switch-master", (channel, message) =>
            {
                Console.WriteLine($"Channel: {channel}, Message: {message}");
                ResetConnection();
            });

            Console.WriteLine("哨兵連線成功");
        }

        private static void ResetConnection()
        {
            var endPoint = _sentinelConnectionMultiplexer.GetEndPoints().First();
            var server = _sentinelConnectionMultiplexer.GetServer(endPoint);

            var masterEndPoint = server.SentinelGetMasterAddressByName("mymaster");
            var masterConfiguration = new ConfigurationOptions()
            {
                AbortOnConnectFail = false,
                ConnectTimeout = 1000,
                SyncTimeout = 1000,
                ConnectRetry = 5,
            };
            masterConfiguration.EndPoints.Add(masterEndPoint);
            _masterConnectionMultiplexer = ConnectionMultiplexer.Connect(masterConfiguration);

            var replicaEndPoint = server.SentinelGetReplicaAddresses("mymaster").AsEnumerable();
            var replicaConfiguration = new ConfigurationOptions()
            {
                AbortOnConnectFail = false,
                ConnectTimeout = 1000,
                SyncTimeout = 1000,
                ConnectRetry = 5,
            };

            replicaEndPoint.ToList().ForEach(x => masterConfiguration.EndPoints.Add(x));
            _replicaConnectionMultiplexer = ConnectionMultiplexer.Connect(masterConfiguration);

            Console.WriteLine($"MasterEndPoint: {masterEndPoint}, ReplicaEndPoint: {string.Join(',', replicaEndPoint)}");
        }
    }
}