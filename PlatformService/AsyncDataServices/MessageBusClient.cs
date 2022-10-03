using PlatformService.Dtos;
using RabbitMQ.Client;
using System.Text.Json;
using System.Text;

namespace PlatformService.AsyncDataServices
{
    public class MessageBusClient : IMessageBusClient
    {
        private readonly IConfiguration configuration;
        private readonly IConnection connection;
        private readonly IModel channel;

        public MessageBusClient(IConfiguration configuration)
        {
            this.configuration = configuration;
            var factory = new ConnectionFactory() { HostName = configuration["RabbitMQHost"], Port = int.Parse(configuration["RabbitMQPort"])};
            try
            {
                connection = factory.CreateConnection();
                channel = connection.CreateModel();

                channel.ExchangeDeclare(exchange: "trigger", type: ExchangeType.Fanout);

                connection.ConnectionShutdown += RabbitMQ_ConnectionShutdown;

                Console.WriteLine("--> Connected to Message Bus");
            }
            catch(Exception ex)
            {
                Console.WriteLine($"--> Could not connnect to the Message Bus: {ex.Message}");
            }
        }

        public void PublishNewPlatform(PlatformPublishedDto platformPublishedDto)
        {
            var message = JsonSerializer.Serialize(platformPublishedDto);

            if (connection.IsOpen)
            {
                Console.WriteLine("--> RabbitMQ Connection open, sending message");
                SendMessage(message);
            }
            else
            {
                Console.WriteLine("--> RabbitMQ Connection is closed, not sending");
            }
        }

        public void Dispose()
        {
            Console.WriteLine("--> Message bus disposed");
            if (channel.IsOpen)
            {
                channel.Close();
                connection.Close();
            }
        }

        private void SendMessage(string message)
        {
            var body = Encoding.UTF8.GetBytes(message);

            channel.BasicPublish(exchange: "trigger",
                                 routingKey: "",
                                 basicProperties: null,
                                 body: body);

            Console.WriteLine($"--> We have sent {message}");
        }

        private void RabbitMQ_ConnectionShutdown(object? sender, ShutdownEventArgs e)
        {
            Console.WriteLine("--> RabbitMQ Connection Shutdown");
        }
    }
}