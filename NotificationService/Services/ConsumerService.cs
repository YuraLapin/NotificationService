using Confluent.Kafka;
using NotificationService.Hubs;

namespace NotificationService.Services
{
    // <summary>
    // Сервис, работающий в фоне и читающий сообщения из Kafka
    // </summary>
    public class ConsumerService : BackgroundService
    {
        private readonly IConsumer<Ignore, string> _consumer;
        private readonly ILogger<ConsumerService> _logger;
        private readonly NotificaionHub _websocketHub;

        public ConsumerService(IConfiguration configuration, ILogger<ConsumerService> logger, NotificaionHub websocketHub)
        {
            _logger = logger;
            _websocketHub = websocketHub;

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                GroupId = "notification-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            _consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
        }

        // <summary>
        // Запускает цикл чтения и отправки в websocket сообщения из Kafka,
        // цикл прерывается через cancellationToken
        // </summary>
        // <param name="ct">
        // Токен отмены
        // </param>
        protected override async Task ExecuteAsync(CancellationToken ct)
        {
            _consumer.Subscribe("notification-topic");

            while (!ct.IsCancellationRequested)
            {
                await ProcessKafkaMessage(ct);
                Task.Delay(TimeSpan.FromMinutes(1), ct);
            }

            _consumer.Close();
        }

        // <summary>
        // Обрабатывает полученное из Kafka сообщение - 
        // Отправляет его содержимое в websocket
        // </summary>
        // <param name="ct">
        // Токен отмены
        // </param>
        public async Task ProcessKafkaMessage(CancellationToken ct)
        {
            try
            {
                var consumed = _consumer.Consume(ct);
                var message = consumed.Message.Value;
                _logger.LogCritical($"Получено сообщение: { message }");
                await _websocketHub.SendMessage(message);
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"Ошибка при получении сообщения: { ex.Message }");
            }
        }
    }
}
