# TinyOutbox

[![NuGet](https://img.shields.io/nuget/v/TinyOutbox.Core.svg)](https://www.nuget.org/packages/TinyOutbox.Core/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%209.0-blue.svg)](https://dotnet.microsoft.com/)

**TinyOutbox**, mikroservis ve dağıtık mimarilerde karşılaşılan **Dual-Write** problemini ortadan kaldırmak, mesaj kaybını sıfırlamak ve mükerrer işlem (duplicate processing) riskini önlemek için tasarlanmış hafif, modüler bir **Transactional Outbox & Inbox (Idempotency)** kütüphanesidir.

Ağır konfigürasyonlara girmeden Entity Framework Core ve ADO.NET transaction yapılarıyla doğrudan uyum sağlar[cite: 3].

---

## 📌 Neden TinyOutbox?

* **Garantili İletim (At-least-once Delivery):** İş mantığı ile fırlatılacak mesaj tek bir veritabanı transaction'ında atomik kaydedilir; ağ kopması veya broker kesintilerinde mesaj kaybolmaz[cite: 2, 5].
* **Uçtan Uca Idempotency (Inbox Pattern):** Ağ tekrarları veya broker yeniden denemeleriyle aynı mesaj iki kez geldiğinde, alıcı tarafta mükerrer faturalandırma veya çifte işlem engellenir[cite: 1, 6].
* **Modüler ve Esnek:** İhtiyaç duyulmayan bağımlılıkları projeye yüklemez; depolama ve taşıyıcı katmanları ayrı paketler halindedir.

---

## 📦 Paket Ailesi

| Paket Adı | Görevi | Sürüm |
| :--- | :--- | :--- |
| **`TinyOutbox.Core`** | Çekirdek modeller, Outbox & Inbox sözleşmeleri[cite: 3] | [![NuGet](https://img.shields.io/nuget/v/TinyOutbox.Core.svg)](https://www.nuget.org/packages/TinyOutbox.Core/) |
| **`TinyOutbox.Hosting`** | Arka plan worker döngüsü ve DI yaşam döngüsü | [![NuGet](https://img.shields.io/nuget/v/TinyOutbox.Hosting.svg)](https://www.nuget.org/packages/TinyOutbox.Hosting/) |
| **`TinyOutbox.Storage.PostgreSql`** | PostgreSQL depolama entegrasyonu (`Npgsql`) | [![NuGet](https://img.shields.io/nuget/v/TinyOutbox.Storage.PostgreSql.svg)](https://www.nuget.org/packages/TinyOutbox.Storage.PostgreSql/) |
| **`TinyOutbox.Transport.RabbitMQ`** | RabbitMQ broker iletim altyapısı[cite: 4] | [![NuGet](https://img.shields.io/nuget/v/TinyOutbox.Transport.RabbitMQ.svg)](https://www.nuget.org/packages/TinyOutbox.Transport.RabbitMQ/) |

---

## 🚀 Hızlı Başlangıç

### 1. Kurulum

```bash
dotnet add package TinyOutbox.Core
dotnet add package TinyOutbox.Hosting
dotnet add package TinyOutbox.Storage.PostgreSql
dotnet add package TinyOutbox.Transport.RabbitMQ
2. Servis Kaydı (Program.cs)C#using TinyOutbox.Core;
using TinyOutbox.Hosting;
using TinyOutbox.Storage.PostgreSql;
using TinyOutbox.Transport.RabbitMQ;

var builder = WebApplication.CreateBuilder(args);
var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")!;

builder.Services.AddTinyOutbox()
    .UsePostgreSql(connectionString)
    .UseRabbitMQ(options =>
    {
        options.HostName = "localhost";
        options.Port = 5672;
        options.UserName = "guest";
        options.Password = "guest";
    });

var app = builder.Build();
3. Mesaj Yayınlama (Transactional Outbox)Veritabanı kaydı ile Outbox mesajı aynı DbTransaction kapsamında atomik olarak işletilir:   C#app.MapPost("/orders", async (CreateOrderRequest request, AppDbContext db, ITinyOutbox outbox) =>
{
    await using var transaction = await db.Database.BeginTransactionAsync();

    try
    {
        // 1. Siparişi kaydet
        var order = new Order
        {
            CustomerEmail = request.CustomerEmail,
            TotalAmount = request.TotalAmount
        };
        await db.Orders.AddAsync(order);
        await db.SaveChangesAsync();

        // 2. Outbox event'ini aynı transaction'a bağla
        var @event = new OrderCreatedEvent(order.Id, order.CustomerEmail, order.TotalAmount);
        await outbox.PublishAsync(@event, transaction.GetDbTransaction());

        // 3. İki işlemi birlikte commit et
        await transaction.CommitAsync();

        return Results.Created($"/orders/{order.Id}", new { order.Id, Status = "Order Created & Enqueued" });
    }
    catch (Exception ex)
    {
        await transaction.RollbackAsync();
        return Results.Problem(ex.Message);
    }
});
4. Mesaj Tüketme (Idempotent Consumer / Inbox)Tüketici katmanında ITinyInbox.ExecuteAsync kullanılarak iş kuralının sadece bir defa çalışması garanti edilir:   C#using TinyOutbox.Core.Services.Abstract;

public class OrderCreatedConsumer : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;

    public OrderCreatedConsumer(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // ... RabbitMQ bağlantısı ve dinleme bloğu ...
        
        var @event = JsonSerializer.Deserialize<OrderCreatedEvent>(messageJson);

        using var scope = _serviceProvider.CreateScope();
        var tinyInbox = scope.ServiceProvider.GetRequiredService<ITinyInbox>();

        // Mesaj daha önce işlenmediyse action yürütülür ve tabloya mühürlenir[cite: 1]
        bool executed = await tinyInbox.ExecuteAsync(
            @event.OrderId,
            nameof(OrderCreatedEvent),
            async () =>
            {
                // Fatura kesme, kargo entegrasyonu veya e-posta bildirim adımı
                await ProcessOrderAsync(@event);
            },
            stoppingToken
        );

        if (!executed)
        {
            // Mükerrer (duplicate) mesaj algılandı, işlem tetiklenmeden ack verildi
        }
    }
}
🗄️ Veritabanı Tablo ŞemasıKütüphane PostgreSQL üzerinde iki tablo yönetir:   tiny_outbox_messages: Gönderilmeyi bekleyen ve arka plan servisiyle işlenen giden mesajlar.   tiny_inbox_messages: Tüketici tarafından başarıyla tamamlanan ve tekrar işlenmesi engellenen gelen mesajlar.   🤝 Katkıda Bulunma (Contributing)Topluluk katkıları projeye değer katar. Destek olmak isterseniz:Depoyu forklayın (Fork).Özellik dalınızı açın (git checkout -b feature/YeniDepolama).Değişikliklerinizi commit edin (git commit -m 'feat: SQL Server sağlayıcısı eklendi').Dalı push edin (git push origin feature/YeniDepolama).Bir Pull Request açın.Yeni depolama (SQL Server, MySQL, MongoDB) ve taşıma (Kafka, Azure Service Bus, Amazon SQS) adaptörleri için PR'lar memnuniyetle incelenir.📄 LisansBu kütüphane MIT Lisansı kapsamında açık kaynak olarak sunulmuştur.