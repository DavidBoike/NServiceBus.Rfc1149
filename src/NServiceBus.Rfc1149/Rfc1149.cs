using NServiceBus.Transports;

namespace NServiceBus.Rfc1149
{
    /// <summary>
    /// The class that defines the name of the transport. This is what we refer to in the
    /// .UseTransport<Rfc1149> method that picks this transport over the default
    /// Msmq transport.
    /// 
    /// There is no implementation to this class, but by definition NServiceBus will look for
    /// a class that inherits from ConfigureTransport<Rfc1149> in order to set everything up.
    /// In our case that is the Rfc1149Transport class.
    /// </summary>
    public class Rfc1149 : TransportDefinition
    {
    }
}
