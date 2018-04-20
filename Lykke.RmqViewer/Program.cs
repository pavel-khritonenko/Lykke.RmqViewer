using System;
using System.Collections.Generic;
using System.Reactive;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Lykke.RmqViewer
{
    public sealed class Settings
    {
        public string ConnectionString { get; set; }
        public string Exchanger { get; set; }
        public bool Follow { get; set; }
        public int Top { get; set; } = 10;
        public string RoutingKey { get; set; } = "";
    }


    static class Program
    {
        private static readonly IObservable<Unit> CtrlC = Observable.FromEventPattern<ConsoleCancelEventArgs>(
            x => Console.CancelKeyPress += new ConsoleCancelEventHandler(x),
            x => Console.CancelKeyPress -= new ConsoleCancelEventHandler(x))
            .Select(x =>
            {
                x.EventArgs.Cancel = false;
                return Unit.Default;
            }).Take(1);

        static async Task<int> Main(string[] args)
        {
            var root = new ConfigurationBuilder()
                .AddJsonFile("settings.json", optional: true)
                .AddCommandLine(args, SwitchMapping)
                .Build();

            var settings = new Settings();
            root.Bind(settings);

            if (string.IsNullOrEmpty(settings.ConnectionString) ||
                string.IsNullOrEmpty(settings.Exchanger))
            {
                Console.WriteLine(@"
Usage: dotnet Lykke.RmqViewer.dll [OPTIONS]:

    -cs=<rmq-connection-string>     Rmq connection string (REQUIRED)
    -ex=<rmq-exchanger>             Exchanger to listen (REQUIRED)
    -r=<routing-key>                Routing key to bind queue (DEFAULT="""")
    -t=5, -n=5, --top=5             Take first n events and exit (DEFAULT=10)
    -f=true --follow=true           Follow the events stream until Ctrl+C pressed");

                return -1;
            }

            var connection = CreateRmqListener(settings);

            var asStrings = connection
                .Select(x => Encoding.UTF8.GetString(x));

            if (!settings.Follow)
            {
                asStrings = asStrings.Take(settings.Top);
            }

            await Observable.Amb(
                    CtrlC,
                    asStrings.Do(Console.WriteLine).Select(_ => Unit.Default))
                .LastOrDefaultAsync();

            return 0;
        }

        private static readonly IDictionary<string, string> SwitchMapping = new Dictionary<string, string>()
        {
            { "-t", "Top" },
            { "-n", "Top" },
            { "-r", "RoutingKey" },
            { "-f", "Follow" },
            { "-cs", "ConnectionString" },
            { "-ex", "Exchange" }
        };

        private static IObservable<byte[]> CreateRmqListener(Settings settings)
        {
            return Observable.Using(
                () => new ConnectionSession(
                    new Uri(settings.ConnectionString),
                    settings.Exchanger,
                    settings.RoutingKey),
                s => Observable.Amb(
                    Observable.FromEventPattern<BasicDeliverEventArgs>(
                            x => s.Consumer.Received += x,
                            x => s.Consumer.Received -= x)
                        .Select(x => x.EventArgs.Body),
                    Observable.Defer(() =>
                    {
                        s.BindQueueToExchangerAndStartConsume();
                        return Observable.Return(0);
                    }).SelectMany(_ => Observable.Never<byte[]>())));
        }
    }

    public sealed class ConnectionSession : IDisposable
    {
        private readonly string _exchanger;
        private readonly string _routingKey;
        public readonly EventingBasicConsumer Consumer;
        private readonly IConnection _c;
        private readonly IModel _m;
        private string _tag;

        public ConnectionSession(Uri uri, string exchanger, string routingKey)
        {
            _exchanger = exchanger;
            _routingKey = routingKey;
            var f = new ConnectionFactory {Uri = uri};

            Console.WriteLine("Connecting to RMQ...");
            _c = f.CreateConnection();
            _m = _c.CreateModel();

            Consumer = new EventingBasicConsumer(_m);
        }

        public void BindQueueToExchangerAndStartConsume()
        {
            Console.WriteLine("Creating queue and binding to exchange");
            var queueDeclareOk = _m.QueueDeclare($"lykke-rmq-viewer-{Guid.NewGuid()}",
                durable: false, exclusive: true, autoDelete: false);
            _m.QueueBind(queueDeclareOk.QueueName, _exchanger, routingKey: _routingKey);
            _tag = _m.BasicConsume(queueDeclareOk.QueueName, consumer: Consumer, autoAck: true);
        }

        public void Dispose()
        {
            Console.WriteLine("Closing RMQ connection...");

            if (_tag != null)
            {
                _m?.BasicCancel(_tag);
            }

            _m?.Dispose();
            _c?.Dispose();

            Console.WriteLine("RMQ connection closed.");
        }
    }
}