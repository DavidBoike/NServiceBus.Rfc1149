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
    public class Rfc1149Transport : ConfigureTransport<Rfc1149>, IDisposable
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof (Rfc1149Transport));

        private Timer timer;

        protected override string ExampleConnectionStringForErrorMessage
        {
            get { return @"None"; }
        }

        protected override bool RequiresConnectionString
        {
            get { return false;  }
        }

        protected override void InternalConfigure(Configure config)
        {
            Enable<Rfc1149Transport>();
            Enable<MessageDrivenSubscriptions>();
        }

        public override void Initialize()
        {
            NServiceBus.Configure.Component<Rfc1149QueueCreator>(DependencyLifecycle.InstancePerCall);

            NServiceBus.Configure.Component<Rfc1149MessageSender>(DependencyLifecycle.InstancePerCall);

            NServiceBus.Configure.Component<Rfc1149DequeueStrategy>(DependencyLifecycle.InstancePerCall)
                  .ConfigureProperty(p => p.PurgeOnStartup, ConfigurePurging.PurgeRequested);

            timer = new Timer(OnCheckOutgoing, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }

        private void OnCheckOutgoing(object ignoredState)
        {
            try
            {
                var workingDir = Utils.GetWorkingDirectory();

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

                if (highest != null)
                {
                    Logger.InfoFormat("{0} messages awaiting delivery in outgoing queues. Consider sending avian carrier to {1} ({2} pending messages).",
                        msgCount, highest.Server, highest.MsgCount);
                }
            }
            catch (Exception)
            {
                
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
