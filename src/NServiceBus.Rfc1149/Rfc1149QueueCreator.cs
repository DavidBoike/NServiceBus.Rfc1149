using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Transports;

namespace NServiceBus.Rfc1149
{
    /// <summary>
    /// The Queue Creator is executed at endpoint startup in order to create queues, so it needs
    /// to check for the existence of the queue and then create it if it does not exist.
    /// </summary>
    public class Rfc1149QueueCreator : ICreateQueues
    {
        private static readonly string MachineName = Environment.MachineName;

        public void CreateQueueIfNecessary(Address address, string account)
        {
            // Our utility to get the queue directory will always create the directory (queue) if it can,
            // so there's not much to do here. Other transports would be more complicated here.
            Utils.GetQueueDirectory(address);
        }
    }
}
