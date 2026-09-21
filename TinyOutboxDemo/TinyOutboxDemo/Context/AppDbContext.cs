using Microsoft.EntityFrameworkCore;
using System.Reflection.Emit;
using TinyOutboxDemo.Models;

namespace TinyOutboxDemo.Context
{
    public class AppDbContext : DbContext
    {
        public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

        public DbSet<Order> Orders => Set<Order>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            // Eğer TinyOutbox'ın modelBuilder için bir extension'ı varsa buraya eklenebilir:
            // modelBuilder.UseTinyOutbox(); 
        }
    }
}
