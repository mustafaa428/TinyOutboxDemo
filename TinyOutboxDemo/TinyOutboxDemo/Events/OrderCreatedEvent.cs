namespace TinyOutboxDemo.Events
{
    public record OrderCreatedEvent(Guid OrderId, string CustomerEmail, decimal TotalAmount);
}
