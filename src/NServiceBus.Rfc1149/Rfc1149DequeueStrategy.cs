using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.CircuitBreakers;
using NServiceBus.Logging;
using NServiceBus.Serializers.Json;
using NServiceBus.Transports;

namespace NServiceBus.Rfc1149
{
    public class Rfc1149DequeueStrategy : IDequeueMessages
    {
        public bool PurgeOnStartup { get; set; }

        private DirectoryInfo queueDir;
        private CancellationTokenSource tokenSource;
        private Func<TransportMessage, bool> tryProcessMessage;
        private Action<TransportMessage, Exception> endProcessMessage;

        static readonly ILog Logger = LogManager.GetLogger(typeof(Rfc1149DequeueStrategy));
        static readonly JsonMessageSerializer Serializer = new JsonMessageSerializer(null);

        private readonly RepeatedFailuresOverTimeCircuitBreaker circuitBreaker = new RepeatedFailuresOverTimeCircuitBreaker("Rfc1149TransportConnectivity",
                    TimeSpan.FromMinutes(2),
                    ex => Configure.Instance.RaiseCriticalError("Repeated failures when communicating with removable device. Probably attached to a bird.", ex),
                    TimeSpan.FromSeconds(10));

        public void Init(Address address, Unicast.Transport.TransactionSettings transactionSettings, Func<TransportMessage, bool> tryProcessMessage, Action<TransportMessage, Exception> endProcessMessage)
        {
            this.tryProcessMessage = tryProcessMessage;
            this.endProcessMessage = endProcessMessage;
            this.queueDir = Utils.GetQueueDirectory(address);

            if (PurgeOnStartup)
                Purge();
        }

        public void Start(int maximumConcurrencyLevel)
        {
            tokenSource = new CancellationTokenSource();

            for (var i = 0; i < maximumConcurrencyLevel; i++)
            {
                StartThread();
            }
        }

        private void StartThread()
        {
            var token = tokenSource.Token;

            Task.Factory
                .StartNew(Action, token, token, TaskCreationOptions.LongRunning, TaskScheduler.Default)
                .ContinueWith(t =>
                {
                    t.Exception.Handle(ex =>
                    {
                        Logger.Warn("Failed to connect to the removable device.");
                        circuitBreaker.Failure(ex);
                        return true;
                    });

                    StartThread();
                }, TaskContinuationOptions.OnlyOnFaulted);
        }

        private void Action(object obj)
        {
            var cancellationToken = (CancellationToken)obj;
            var backOff = new BackOff(1000);

            while (!cancellationToken.IsCancellationRequested)
            {
                var result = new ReceiveResult();

                try
                {
                    result = TryReceive();
                }
                finally
                {
                    //since we're polling the message will be null when there was nothing in the queue
                    if (result.Message != null)
                    {
                        endProcessMessage(result.Message, result.Exception);
                        if (result.Reader != null)
                        {
                            result.Reader.Dispose();
                            File.Delete(result.Path);
                        }
                    }
                }

                circuitBreaker.Success();
                backOff.Wait(() => result.Message == null);
            }
        }

        public void Stop()
        {
            tokenSource.Cancel();
        }

        private ReceiveResult TryReceive()
        {
            var result = Receive();

            if (result.Message == null)
                return result;

            try
            {
                tryProcessMessage(result.Message);
            }
            catch (Exception ex)
            {
                result.Exception = ex;
            }

            return result;
        }

        private ReceiveResult Receive()
        {
            foreach(var file in queueDir.EnumerateFiles())
            {
                for (int i = 0; i < 3; i++)
                {
                    try
                    {
                        return TryReadFile(file);
                    }
                    catch (IOException)
                    {
                        Thread.Sleep(10);
                    }
                }
            }
            return new ReceiveResult();
        }

        private ReceiveResult TryReadFile(FileInfo file)
        {
            StreamReader sr = null;
            try
            {
                sr = new StreamReader(file.FullName, Encoding.UTF8);
                string id = sr.ReadLine();
                string correlationId = sr.ReadLine();
                string replyTo = sr.ReadLine();
                string recoverable = sr.ReadLine();
                string timeToBeReceived = sr.ReadLine();
                string headers = sr.ReadLine();
                string base64Body = sr.ReadLine();

                var headersDict = Serializer.DeserializeObject<Dictionary<string, string>>(headers);

                TransportMessage tm = new TransportMessage(id, headersDict);
                tm.CorrelationId = correlationId;
                tm.ReplyToAddress = String.IsNullOrWhiteSpace(replyTo) ? null : Address.Parse(replyTo);
                tm.Recoverable = bool.Parse(recoverable ?? "True");
                if (String.IsNullOrWhiteSpace(timeToBeReceived))
                    tm.TimeToBeReceived = TimeSpan.MaxValue;
                else
                {
                    DateTime expireDateTime = DateTime.Parse(timeToBeReceived);
                    tm.TimeToBeReceived = TimeSpan.FromTicks(expireDateTime.Ticks - DateTime.UtcNow.Ticks);
                }
                if (!String.IsNullOrWhiteSpace(base64Body))
                    tm.Body = Convert.FromBase64String(base64Body);

                return new ReceiveResult
                {
                    Message = tm,
                    Reader = sr,
                    Path = file.FullName
                };
            }
            catch (Exception)
            {
                if (sr != null)
                    sr.Dispose();
                throw;
            }
        }

        private void Purge()
        {
            var files = queueDir.GetFiles();
            foreach (var file in files)
                file.Delete();
        }

        class ReceiveResult : IDisposable
        {
            public Exception Exception { get; set; }
            public TransportMessage Message { get; set; }
            public StreamReader Reader { get; set; }
            public string Path { get; set; }

            public void Dispose()
            {
                throw new NotImplementedException();
            }
        }
    }
}
