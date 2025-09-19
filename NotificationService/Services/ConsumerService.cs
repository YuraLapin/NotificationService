using Confluent.Kafka;
using NotificationService.Hubs;
using Microsoft.AspNetCore.SignalR;

namespace NotificationService.Services
{
    // <summary>
    // Сервис, работающий в фоне и читающий сообщения из Kafka
    // </summary>
    public class ConsumerService : BackgroundService
    {
        private readonly ILogger<ConsumerService> _logger;
        private readonly IConsumer<Ignore, string> _consumer;
        private readonly IHubContext<NotificationHub, INotificator> _hubContext;

        public ConsumerService(IConfiguration configuration, ILogger<ConsumerService> logger, IHubContext<NotificationHub, INotificator> hubContext)
        {
            _logger = logger;
            _hubContext = hubContext;

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
                await Task.Delay(1000, ct);
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
                await _hubContext.Clients.All.Notify(message);
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"Ошибка при получении сообщения: { ex.Message }");
            }
        }
    }
}
