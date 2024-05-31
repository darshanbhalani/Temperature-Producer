using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

namespace Temperature_Producer
{
    internal class Program
    {
        static async Task Main(string[] args)
        {

            List<long> pollNumbers = [2442,3254,1248,3567,15497,55479,74,974,546,7912,8794,1247,1246,79461,39,313,544,68,154,8478];

            var configuration = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

            var config = new ProducerConfig
            {
                BootstrapServers = $"{configuration["BootstrapService:Server"]}:{configuration["BootstrapService:Port"]}"
            };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                while (true)
                {
                    List<TemperatureModel> dataModels = new List<TemperatureModel>();

                    foreach (var v in pollNumbers)
                    {
                        dataModels.Add(new TemperatureModel
                        {
                            PollNumber = v,
                            Temperature = randomTemperature(),
                            TimeStamp = DateTime.Now,
                            Area = ""
                        });
                    }

                    try
                    {
                        await producer.ProduceAsync(configuration["BootstrapService:Topic"], new Message<Null, string> { Value = JsonConvert.SerializeObject(dataModels) });
                        await Console.Out.WriteLineAsync(JsonConvert.SerializeObject(dataModels));
                        await Console.Out.WriteLineAsync();
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                    }
                    Thread.Sleep(1000);
                }
            }
        }
        private static int randomTemperature()
        {
            int max = 55;
            Random random = new Random();
            return random.Next(30, max + 1);
        }
    }
}
