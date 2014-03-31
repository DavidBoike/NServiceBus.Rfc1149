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
    /// <summary>
    /// A transport needs a class that implements ISendMessages to send messages when needed. In the case
    /// of publishing messages, this class will be called once for each recipient. If we were creating a
    /// transport for a queueing system that had built-in support for Publish/Subscribe (like ActiveMQ
    /// and RabbitMQ do) then we would also create a class implementing IPublishMessages.
    /// </summary>
    public class Rfc1149MessageSender : ISendMessages
    {
        static readonly JsonMessageSerializer Serializer = new JsonMessageSerializer(null);

        public void Send(TransportMessage message, Address address)
        {
            try
            {
                // If the outgoing queue can not be found, then quietly fail. In real life we'd probably throw a
                // FailedToSendMessageException in that case, but then it would be impossible to even start an
                // NServiceBus endpoint without a flash drive connected. We could try to store these messages in memory,
                // but that's not exactly very reliable either.
                var queueDir = Utils.GetQueueDirectory(address);
                if (queueDir == null)
                    return;

                // Determine the filename and path for the new message.
                string fileName = String.Format("{0}.rfc1149", message.Id);
                string filePath = Path.Combine(queueDir.FullName, fileName);

                // Write out the message details to the file, one item per line.
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

                    // Message headers get serialized into a single line of JSON.
                    sw.WriteLine(Serializer.SerializeObject(message.Headers));

                    // Remember that message body can be null for things like subscription messages.
                    if (message.Body == null)
                        sw.WriteLine();
                    else
                        sw.WriteLine(Convert.ToBase64String(message.Body));
                }

            }
            catch (Exception ex)
            {
                // This is the appropriate thing to do when unable to send a message.
                if (address == null)
                    throw new FailedToSendMessageException("Failed to send message.", ex);

                throw new FailedToSendMessageException(
                    string.Format("Failed to send message to address: {0}@{1}", address.Queue, address.Machine), ex);
            }
        }
    }
}
