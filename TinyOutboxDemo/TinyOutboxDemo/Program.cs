using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using TinyOutbox.Core;
using TinyOutbox.Hosting;
using TinyOutbox.Storage.PostgreSql;
using TinyOutbox.Transport.RabbitMQ;
using TinyOutboxDemo.Consumers;
using TinyOutboxDemo.Context;
using TinyOutboxDemo.Events;
using TinyOutboxDemo.Models;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")!;

// 1. EF Core DbContext
builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(connectionString));

// 2. TinyOutbox Konfigürasyonu
builder.Services.AddTinyOutbox()
    .UsePostgreSql(connectionString)
    .UseRabbitMQ(options =>
    {
        options.HostName = "localhost";
        options.Port = 5672;
        options.UserName = "guest";
        options.Password = "guest";
    });

builder.Services.AddHostedService<OrderCreatedConsumer>();

var app = builder.Build();

// Veritabanı ve tabloları otomatik oluştur
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    await db.Database.EnsureCreatedAsync();
}

// 4. Sipariş Endpoint'i
app.MapPost("/orders", async (CreateOrderRequest request, AppDbContext db, ITinyOutbox outbox) =>
{
    await using var transaction = await db.Database.BeginTransactionAsync();

    try
    {
        var order = new Order
        {
            CustomerEmail = request.CustomerEmail,
            TotalAmount = request.TotalAmount
        };

        await db.Orders.AddAsync(order);
        await db.SaveChangesAsync();

        var @event = new OrderCreatedEvent(order.Id, order.CustomerEmail, order.TotalAmount);

        // Transactional Outbox yazımı
        await outbox.PublishAsync(@event, transaction.GetDbTransaction());

        await transaction.CommitAsync();

        return Results.Created($"/orders/{order.Id}", new { order.Id, Status = "Order Created & Enqueued" });
    }
    catch (Exception ex)
    {
        await transaction.RollbackAsync();
        return Results.Problem($"Hata oluştu: {ex.Message}");
    }
});

app.Run();