namespace TinyOutboxDemo.Events
{
    public record CreateOrderRequest(string CustomerEmail, decimal TotalAmount);
}
