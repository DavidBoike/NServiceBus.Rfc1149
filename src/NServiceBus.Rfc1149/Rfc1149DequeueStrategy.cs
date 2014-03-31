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
    /// <summary>
    /// A transport needs a class that implements IDequeueMessages to serve as the message receiver.
    /// </summary>
    public class Rfc1149DequeueStrategy : IDequeueMessages
    {
        /// <summary>
        /// The dependency injection container will set this to true if we need to clear out our input queue.
        /// </summary>
        public bool PurgeOnStartup { get; set; }

        private Address address;
        private CancellationTokenSource tokenSource;
        private Func<TransportMessage, bool> tryProcessMessage;
        private Action<TransportMessage, Exception> endProcessMessage;

        static readonly ILog Logger = LogManager.GetLogger(typeof(Rfc1149DequeueStrategy));
        static readonly JsonMessageSerializer Serializer = new JsonMessageSerializer(null);

        private readonly RepeatedFailuresOverTimeCircuitBreaker circuitBreaker = new RepeatedFailuresOverTimeCircuitBreaker("Rfc1149TransportConnectivity",
                    TimeSpan.FromMinutes(2),
                    ex => Configure.Instance.RaiseCriticalError("Repeated failures when communicating with removable device. Probably attached to a bird.", ex),
                    TimeSpan.FromSeconds(10));

        /// <summary>
        ///     Initializes the <see cref="IDequeueMessages" />.
        /// </summary>
        /// <param name="address">The address to listen on.</param>
        /// <param name="transactionSettings">
        ///     The <see cref="TransactionSettings" /> to be used by <see cref="IDequeueMessages" />.
        /// </param>
        /// <param name="tryProcessMessage">Called when a message has been dequeued and is ready for processing.</param>
        /// <param name="endProcessMessage">
        ///     Needs to be called by <see cref="IDequeueMessages" /> after the message has been processed regardless if the outcome was successful or not.
        /// </param>
        public void Init(Address address, Unicast.Transport.TransactionSettings transactionSettings, Func<TransportMessage, bool> tryProcessMessage, Action<TransportMessage, Exception> endProcessMessage)
        {
            // The actual input queue may disappear at any time when the flash drive is attached to a bird
            // so we store the address to use later.
            this.address = address;

            // These delegates are from the NServiceBus framework and must be called during message processing.
            this.tryProcessMessage = tryProcessMessage;
            this.endProcessMessage = endProcessMessage;

            // Try to purge the input queue on startup if asked to do so
            if (PurgeOnStartup)
                Purge();
        }

        /// <summary>
        /// Called by NServiceBus to start the transport.
        /// </summary>
        /// <param name="maximumConcurrencyLevel">The number of processing threads requested.</param>
        public void Start(int maximumConcurrencyLevel)
        {
            // A CancellationTokenSource provides a method to stop the threads once they're started.
            tokenSource = new CancellationTokenSource();

            // Start the number of threads requested.
            for (var i = 0; i < maximumConcurrencyLevel; i++)
                StartThread();
        }

        private void StartThread()
        {
            var token = tokenSource.Token;

            // Start a new thread via the Task Factory running the ReceiveMessagesAction, passing the cancellation
            // token as the state, and configuring the task as long-running on the default scheduler.
            Task.Factory
                .StartNew(ReceiveMessagesAction, token, token, TaskCreationOptions.LongRunning, TaskScheduler.Default)
                .ContinueWith(t =>
                {
                    // This is what to do with any exception that may bubble out of the thread. We log it, register
                    // a failure on the circuit-breaker (so that after many failures we can bail out and fail the
                    // process) and then start the thread back up to try again. This does NOT happen from standard
                    // message exception failures.
                    t.Exception.Handle(ex =>
                    {
                        Logger.Warn("Failed to connect to the removable device.");
                        circuitBreaker.Failure(ex);
                        return true;
                    });

                    StartThread();
                }, TaskContinuationOptions.OnlyOnFaulted);
        }

        private void ReceiveMessagesAction(object obj)
        {
            var cancellationToken = (CancellationToken)obj;

            // The longest we want to delay between checks for the flash drive is 10 seconds.
            var backOff = new BackOff(10000);

            // If cancellation has been called for, we give up and return.
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = new ReceiveResult();
                try
                {
                    // Attempt to receive a message.
                    result = TryReceive();
                }
                finally
                {
                    // Since we're polling the message will be null when there was nothing in the queue
                    if (result.Message != null)
                    {
                        // This must be called whether or not the message was successfully processed
                        endProcessMessage(result.Message, result.Exception);

                        // If the result contains a StreamReader, it means we are "locking" the file, as much
                        // as we're able to with the filesystem. It also means we need to remove the message
                        // from the queue, i.e. delete the file. It would sure be nice if we had a transactional
                        // file system and we could just count on the file being deleted when the
                        // ambient transaction commits, but we can't. So we dispose the StreamReader and then
                        // instantly try to delete the file. 
                        if (result.Reader != null)
                        {
                            result.Reader.Dispose();
                            File.Delete(result.Path);
                        }
                    }
                }

                // Successfully processing a file will reset the circuit breaker so that the occasional failure
                // will not bring down the process, but repeated failures will.
                circuitBreaker.Success();

                // If we found no message, then we want to back off a little bit before checking again
                // so as not to swamp the filesystem.
                backOff.Wait(() => result.Message == null);
            }
        }

        public void Stop()
        {
            // This will signal all the running threads to stop after they finish their current
            // iteration of processing.
            tokenSource.Cancel();
        }

        private ReceiveResult TryReceive()
        {
            // Look in the queue and try to get a message.
            var result = Receive();

            // If no message is available, just return.
            if (result.Message == null)
                return result;

            try
            {
                // Allow NServiceBus to run the message handlers.
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
            // Look for the queue directory, if the flash drive is currently available.
            var queueDir = Utils.GetQueueDirectory(address);

            if (queueDir != null)
            {
                // Enumerate through the files in the directory. If a message is locked by another thread or process, we may not be able
                // to open it, so try 3 times (in case it's currently being written) and then just move on to the next one.
                foreach (var file in queueDir.EnumerateFiles())
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
            }
            return new ReceiveResult();
        }

        private ReceiveResult TryReadFile(FileInfo file)
        {
            StreamReader sr = null;
            try
            {
                // Open the file and read all the information from each line.
                sr = new StreamReader(file.FullName, Encoding.UTF8);
                string id = sr.ReadLine();
                string correlationId = sr.ReadLine();
                string replyTo = sr.ReadLine();
                string recoverable = sr.ReadLine();
                string timeToBeReceived = sr.ReadLine();
                string headers = sr.ReadLine();
                string base64Body = sr.ReadLine();

                // Deserialize the headers dictionary
                var headersDict = Serializer.DeserializeObject<Dictionary<string, string>>(headers);

                // Construct a TransportMessage with the message information.
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

                // Remember that some messages (subscription requests, etc.) do not have a message body.
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
                // If an exception occurred, dispose the StreamReader and rethrow.
                if (sr != null)
                    sr.Dispose();
                throw;
            }
        }

        private void Purge()
        {
            // If the queue directory can be found on the flash drive, delete all the files in that directory.
            var queueDir = Utils.GetQueueDirectory(address);
            if (queueDir != null)
            {
                var files = queueDir.GetFiles();
                foreach (var file in files)
                    file.Delete();
            }
        }

        class ReceiveResult
        {
            public Exception Exception { get; set; }
            public TransportMessage Message { get; set; }
            public StreamReader Reader { get; set; }
            public string Path { get; set; }
        }
    }
}
