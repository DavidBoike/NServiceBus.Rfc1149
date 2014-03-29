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
        public static DirectoryInfo GetWorkingDirectory()
        {
            return new DirectoryInfo(@"C:\Rfc1149\NServiceBus.Rfc1149");
            return DriveInfo.GetDrives()
                .Where(d => d.DriveType == DriveType.Removable)
                .Select(
                    d => d.RootDirectory.GetDirectories("NServiceBus.Rfc1149", SearchOption.TopDirectoryOnly))
                .Where(dirs => dirs.Any())
                .Select(dirs => dirs.FirstOrDefault())
                .FirstOrDefault();
        }

        public static DirectoryInfo GetQueueDirectory(Address address)
        {
            var workingDir = Utils.GetWorkingDirectory();

            string machineName = address.Machine;
            if (String.IsNullOrWhiteSpace(machineName))
                machineName = Environment.MachineName;

            string subdir = String.Format("{0}\\{1}", machineName, address.Queue);

            return workingDir.CreateSubdirectory(subdir);
        }


    }

    public class Rfc1149Init : INeedInitialization
    {

        public void Init()
        {
            Configure.Instance.UseTransport<Rfc1149>();
        }
    }
}
