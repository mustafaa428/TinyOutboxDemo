using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using TinyOutbox.Core;
using TinyOutboxDemo.Events;

namespace TinyOutboxDemo.Consumers;

public class OrderCreatedConsumer : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<OrderCreatedConsumer> _logger;
    private readonly IConfiguration _configuration;

    public OrderCreatedConsumer(
        IServiceProvider serviceProvider,
        ILogger<OrderCreatedConsumer> logger,
        IConfiguration configuration)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _configuration = configuration;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // RabbitMQ'nun hazır olması için kısa bir gecikme
        await Task.Delay(3000, stoppingToken);

        var factory = new ConnectionFactory
        {
            HostName = _configuration["RabbitMQ:HostName"] ?? "localhost",
            Port = int.Parse(_configuration["RabbitMQ:Port"] ?? "5672"),
            UserName = _configuration["RabbitMQ:UserName"] ?? "guest",
            Password = _configuration["RabbitMQ:Password"] ?? "guest"
        };

        var connection = await factory.CreateConnectionAsync(stoppingToken);
        var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

        const string exchangeName = "tiny.outbox.exchange";
        const string queueName = "tiny-outbox-queue";

        // 1. Exchange tanımla
        await channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Topic,
            durable: true,
            cancellationToken: stoppingToken);

        // 2. Queue tanımla
        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: stoppingToken);

        // 3. Kuyruğu exchange'e bağla (Tüm eventleri yakalamak için routing key: #)
        await channel.QueueBindAsync(
            queue: queueName,
            exchange: exchangeName,
            routingKey: "#",
            cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var messageJson = Encoding.UTF8.GetString(body);

                var @event = JsonSerializer.Deserialize<OrderCreatedEvent>(messageJson);
                if (@event != null)
                {
                    using var scope = _serviceProvider.CreateScope();
                    var tinyInbox = scope.ServiceProvider.GetRequiredService<ITinyInbox>();

                    // Idempotent çalıştırma
                    bool executed = await tinyInbox.ExecuteAsync(
                        @event.OrderId,
                        nameof(OrderCreatedEvent),
                        async () =>
                        {
                            _logger.LogInformation("📩 [Consumer] Sipariş işleniyor... ID: {OrderId}, Tutar: {Amount}",
                                @event.OrderId, @event.TotalAmount);

                            // İş kuralı simülasyonu
                            await Task.Delay(200, stoppingToken);

                            _logger.LogInformation("✅ [Consumer] Sipariş başarıyla işlendi.");
                        },
                        stoppingToken
                    );

                    if (!executed)
                    {
                        _logger.LogWarning("⚠️ [Inbox] Mükerrer (duplicate) mesaj engellendi! Sipariş ID: {OrderId}", @event.OrderId);
                    }

                    // Mesajı onayla (Ack)
                    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, cancellationToken: stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ [Consumer] Mesaj işlenirken hata oluştu.");
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true, cancellationToken: stoppingToken);
            }
        };

        await channel.BasicConsumeAsync(
            queue: queueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        _logger.LogInformation("🚀 [Consumer Worker] RabbitMQ dinlemede ve Inbox koruması aktif...");

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken);
        }
    }
}