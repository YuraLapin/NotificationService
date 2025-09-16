using Confluent.Kafka;

namespace NotificationService.Services
{
    public class ConsumerService : BackgroundService
    {
        private readonly IConsumer<Ignore, string> _consumer;
        private readonly ILogger<ConsumerService> _logger;

        public ConsumerService(IConfiguration configuration, ILogger<ConsumerService> logger)
        {
            _logger = logger;

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                GroupId = "notification-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            _consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
        }

        protected override async Task ExecuteAsync(CancellationToken ct)
        {
            _consumer.Subscribe("notification-topic");

            while (!ct.IsCancellationRequested)
            {
                ProcessKafkaMessage(ct);
                Task.Delay(TimeSpan.FromMinutes(1), ct);
            }

            _consumer.Close();
        }

        public void ProcessKafkaMessage(CancellationToken ct)
        {
            try
            {
                var consumed = _consumer.Consume(ct);
                var message = consumed.Message.Value;
                _logger.LogCritical($"\n\n\nReceived: { message }\n\n\n");
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"Error processing Kafka message: { ex.Message }");
            }
        }
    }
}
