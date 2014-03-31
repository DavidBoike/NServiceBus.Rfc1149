namespace NServiceBus.Rfc1149
{
    using System;
    using System.Threading;

    /// <summary>
    /// A utility class that does a sleep on very call up to a limit based on a condition.
    /// This is totally stolen from the NServiceBus.SqlServer transport. For RFC 1149, if
    /// no flash drive is present, we don't want to check again a millisecond later, so the
    /// backoff causes the retry delay to lengthen the longer it remains unavailable, up
    /// until the maximum delay.
    /// </summary>
    class BackOff
    {
        int maximum;
        int currentDelay = 50;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="maximum">The maximum number of milliseconds for which the thread is blocked.</param>
        public BackOff(int maximum)
        {
            this.maximum = maximum;
        }

        /// <summary>
        /// It executes the Thread sleep if condition is <c>true</c>, otherwise it resets.
        /// </summary>
        /// <param name="condition">If the condition is <c>true</c> then the wait is performed.</param>
        public void Wait(Func<bool> condition)
        {
            if (!condition())
            {
                currentDelay = 50;
                return;
            }

            Thread.Sleep(currentDelay);

            if (currentDelay < maximum)
            {
                currentDelay *= 2;
            }

            if (currentDelay > maximum)
            {
                currentDelay = maximum;
            }
        }
    }
}
