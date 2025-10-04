using System;
using System.Collections.Generic;

namespace F12020TelemetryLogger.Utilities
{
    internal static class NetworkHelpers
    {
        public static List<(int pid, string name)> FindUdpOwners(int port)
        {
            var result = new List<(int pid, string name)>();
            try
            {
                var psi = new System.Diagnostics.ProcessStartInfo
                {
                    FileName = "cmd.exe",
                    Arguments = "/c netstat -ano -p udp",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };
                
                using var proc = System.Diagnostics.Process.Start(psi)!;
                string output = proc.StandardOutput.ReadToEnd();
                proc.WaitForExit();

                var owners = new HashSet<int>();
                foreach (var raw in output.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    if (!raw.Contains("UDP", StringComparison.OrdinalIgnoreCase)) continue;
                    var needle = $":{port}";
                    if (!raw.Contains(needle)) continue;

                    var parts = raw.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length < 4) continue;

                    if (int.TryParse(parts[^1], out int pid) && pid > 0) 
                        owners.Add(pid);
                }

                foreach (var pid in owners)
                {
                    string name;
                    try { name = System.Diagnostics.Process.GetProcessById(pid).ProcessName; }
                    catch { name = "Unknown"; }
                    result.Add((pid, name));
                }
            }
            catch { }
            return result;
        }
    }
}