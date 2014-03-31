using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NServiceBus.Rfc1149
{
    public static class Utils
    {
        /// <summary>
        /// Find the first attached removable storage drive that contains an NServiceBus.Rfc1149 directory at the root level,
        /// and use that as the working directory for all our queueing infrastructure.
        /// </summary>
        /// <returns>A DirectoryInfo</returns>
        public static DirectoryInfo GetWorkingDirectory()
        {
            return DriveInfo.GetDrives()
                .Where(d => d.DriveType == DriveType.Removable)
                .Select(
                    d => d.RootDirectory.GetDirectories("NServiceBus.Rfc1149", SearchOption.TopDirectoryOnly))
                .Where(dirs => dirs.Any())
                .Select(dirs => dirs.FirstOrDefault())
                .FirstOrDefault();
        }

        /// <summary>
        /// Map an NServiceBus Address to a queue directory.
        /// </summary>
        /// <param name="address">The address of the queue/param>
        /// <returns>The directory corresponding to the queue.</returns>
        public static DirectoryInfo GetQueueDirectory(Address address)
        {
            var workingDir = Utils.GetWorkingDirectory();

            if (workingDir == null)
                return null;

            string machineName = address.Machine;
            if (String.IsNullOrWhiteSpace(machineName))
                machineName = Environment.MachineName;

            string subdir = String.Format("{0}\\{1}", machineName, address.Queue);

            return workingDir.CreateSubdirectory(subdir);
        }
    }
}
