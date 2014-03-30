using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus.Serializers.Json;
using NServiceBus.Transports;
using NServiceBus.Unicast.Queuing;

namespace NServiceBus.Rfc1149
{
    public class Rfc1149MessageSender : ISendMessages
    {
        static readonly JsonMessageSerializer Serializer = new JsonMessageSerializer(null);

        public void Send(TransportMessage message, Address address)
        {
            try
            {
                var queueDir = Utils.GetQueueDirectory(address);
                if (queueDir == null)
                    return;

                string fileName = String.Format("{0}.rfc1149", message.Id);
                string filePath = Path.Combine(queueDir.FullName, fileName);

                using (StreamWriter sw = new StreamWriter(filePath, false, Encoding.UTF8))
                {
                    sw.WriteLine(message.Id);
                    sw.WriteLine(message.CorrelationId);
                    if (message.ReplyToAddress != null)
                        sw.WriteLine(message.ReplyToAddress);
                    else
                        sw.WriteLine();
                    sw.WriteLine(message.Recoverable);
                    if (message.TimeToBeReceived == TimeSpan.MaxValue)
                        sw.WriteLine();
                    else
                        sw.WriteLine(DateTime.UtcNow.Add(message.TimeToBeReceived));
                    sw.WriteLine(Serializer.SerializeObject(message.Headers));
                    if (message.Body == null)
                        sw.WriteLine();
                    else
                        sw.WriteLine(Convert.ToBase64String(message.Body));
                }

            }
            catch (Exception ex)
            {
                if (address == null)
                    throw new FailedToSendMessageException("Failed to send message.", ex);

                throw new FailedToSendMessageException(
                    string.Format("Failed to send message to address: {0}@{1}", address.Queue, address.Machine), ex);
            }
        }
    }
}
