using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Transports;

namespace NServiceBus.Rfc1149
{
    public class Rfc1149QueueCreator : ICreateQueues
    {
        private static readonly string MachineName = Environment.MachineName;

        public void CreateQueueIfNecessary(Address address, string account)
        {
            Utils.GetQueueDirectory(address);
        }
    }
}
