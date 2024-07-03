using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace KafkaConsumerService
{
    public class Program
    {
        private static async Task Main(string[] args)
        {
            Console.WriteLine("Another Seperate API");

            // Reading appsettings file 
            var config = new ConfigurationBuilder().SetBasePath("C:\\Users\\proir\\source\\repos\\FundooWebAPI\\ConsumeApp").AddJsonFile("appsettings.json", optional: false, reloadOnChange: true).Build();

            // kafka
            var cancellationTokenSource = new CancellationTokenSource();
            var consumer = new KafkaConsumer(config);
            var consumerTask = consumer.StartConsumingAsync(cancellationTokenSource.Token);

            Console.WriteLine("Consumer started. Press [Enter] to stop.");
            Console.ReadLine();

            cancellationTokenSource.Cancel(); // Stop consuming

            try
            {
                await consumerTask; // Wait for the consumer to complete
            }
            catch (OperationCanceledException ie)
            {
                Console.WriteLine(ie.Message);
            }
        }
    }

    public class KafkaConsumer
    {
        private readonly IConfiguration _configuration;
        private IConsumer<Ignore, string> _consumer;

        public KafkaConsumer(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task StartConsumingAsync(CancellationToken cancellationToken)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _configuration["Kafka:BootstrapServers"],
                GroupId = "MyConsumer",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (_consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
            {
                _consumer.Subscribe(_configuration["Kafka:Topic"]);

                try
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var consumeResult = _consumer.Consume(cancellationToken);
                        await ProcessMessageAsync(consumeResult.Message.Value);
                    }
                }
                catch (OperationCanceledException ie)
                {
                    Console.WriteLine(ie.Message);
                }
                finally
                {
                    _consumer.Close();
                }
            }
        }

        private async Task ProcessMessageAsync(string message)
        {
            Console.WriteLine($"Processed message: {message}");
        }
    }

}
