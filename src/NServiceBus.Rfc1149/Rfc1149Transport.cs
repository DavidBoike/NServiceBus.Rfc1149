using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Features;
using NServiceBus.Logging;
using NServiceBus.Transports;

namespace NServiceBus.Rfc1149
{
    /// <summary>
    /// The class that configures the transport to run and wires up all of its necessary dependencies.
    /// </summary>
    public class Rfc1149Transport : ConfigureTransport<Rfc1149>, IDisposable
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof (Rfc1149Transport));

        /// <summary>
        /// Our timer is to examine the outgoing queues to see where we should send our carrier pigeon.
        /// Most serious transports probably won't need this.
        /// </summary>
        private Timer timer;

        /// <summary>
        /// Some transports will require a connection string, but not this one! If it did, here is where
        /// we would provide a sample string for NServiceBus to display in an error message.
        /// </summary>
        protected override string ExampleConnectionStringForErrorMessage
        {
            get { return @"Rfc1149 transport does not require a connection string."; }
        }

        protected override bool RequiresConnectionString
        {
            get { return false;  }
        }

        /// <summary>
        /// We need to specify the features our transport enables or needs. Each of these features
        /// (inheriting from the abstract Feature class) can itself configure other required dependencies.
        /// It's a good way to group up related bits of dependencies into more manageable chunks.
        /// </summary>
        /// <param name="config"></param>
        protected override void InternalConfigure(Configure config)
        {
            Enable<Rfc1149Transport>();
            Enable<MessageDrivenSubscriptions>();
        }

        public override void Initialize()
        {
            // Every transport will need to configure its implementation of ICreateQueues
            NServiceBus.Configure.Component<Rfc1149QueueCreator>(DependencyLifecycle.InstancePerCall);

            // Every transport will need to configure its implementation of ISendMessages
            NServiceBus.Configure.Component<Rfc1149MessageSender>(DependencyLifecycle.InstancePerCall);

            // Every transport will need to configure its implementation of IDequeueMessages
            NServiceBus.Configure.Component<Rfc1149DequeueStrategy>(DependencyLifecycle.InstancePerCall)
                  .ConfigureProperty(p => p.PurgeOnStartup, ConfigurePurging.PurgeRequested);

            // Our transport also needs to configure its timer to check the outgoing message checker.
            timer = new Timer(OnCheckOutgoing, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }

        private void OnCheckOutgoing(object ignoredState)
        {
            try
            {
                // Try to get the working directory
                var workingDir = Utils.GetWorkingDirectory();

                // Count up the messages bound for each server
                var outgoing = workingDir.GetDirectories()
                    .Where(d => d.Name != Environment.MachineName)
                    .Select(d => new
                    {
                        Server = d.Name,
                        MsgCount = d.GetFiles("*", System.IO.SearchOption.AllDirectories).Length
                    })
                    .Where(x => x.MsgCount > 0)
                    .OrderByDescending(x => x.MsgCount)
                    .ToArray();

                int msgCount = outgoing.Sum(x => x.MsgCount);
                var highest = outgoing.FirstOrDefault();

                // Report where we should send our avian carrier to next.
                if (highest != null)
                {
                    Logger.InfoFormat("{0} messages awaiting delivery in outgoing queues. Consider sending avian carrier to {1} ({2} pending messages).",
                        msgCount, highest.Server, highest.MsgCount);
                }
            }
            catch (Exception)
            {
                // An exception will generally be caused by no flash drive present, in which case we really don't
                // need to report anything. Plus catching and swallowing all exceptions is always a good idea, right?
            }
        }

        public void Dispose()
        {
            if (timer != null)
            {
                timer.Dispose();
                timer = null;
            }
        }
    }
}
