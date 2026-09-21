namespace TinyOutboxDemo.Models
{
    public class Order
    {
        public Guid Id { get; set; } = Guid.NewGuid();
        public string CustomerEmail { get; set; } = string.Empty;
        public decimal TotalAmount { get; set; }
        public string Status { get; set; } = "Pending";
        public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
    }
}
