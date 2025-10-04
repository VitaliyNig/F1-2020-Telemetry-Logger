using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using F12020TelemetryLogger.Models;
using F12020TelemetryLogger.Services;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Menu
{
    internal static class MenuHandler
    {
        public static void PrintMenu()
        {
            Console.WriteLine("\n    [1] Manual Save (CSV + Excel)\n    [2] Auto-Save (ON/OFF)\n    [3] Open Folder\n    [4] View Buffer\n    [5] Manual Start New Weekend\n    [6] Debug Mode (ON/OFF)\n    [M] Show Menu Again\n    [0] Exit Program\n");
        }

        public static async Task RunMenuLoop(AppState state, Func<bool> getRunning, Action<bool> setRunning, Func<bool, Task> saveCallback)
        {
            while (getRunning())
            {
                if (!Console.KeyAvailable)
                {
                    await Task.Delay(50);
                    continue;
                }

                var key = Console.ReadKey(true).Key;

                switch (key)
                {
                    case ConsoleKey.M:
                        PrintMenu();
                        break;

                    case ConsoleKey.D1:
                    case ConsoleKey.NumPad1:
                        await saveCallback(true);
                        Log.Success("[✓] Manual Saved");
                        break;

                    case ConsoleKey.D2:
                    case ConsoleKey.NumPad2:
                        state.AutoSaveEnabled = !state.AutoSaveEnabled;
                        Console.WriteLine(state.AutoSaveEnabled ? "[i] Auto-Save: ON" : "[i] Auto-Save: OFF");
                        break;

                    case ConsoleKey.D3:
                    case ConsoleKey.NumPad3:
                        TryOpenFolder(state);
                        break;

                    case ConsoleKey.D4:
                    case ConsoleKey.NumPad4:
                        PrintBuffer(state);
                        break;

                    case ConsoleKey.D5:
                    case ConsoleKey.NumPad5:
                        var fakeHdr = new PacketHeader(0, 0, 0, 0, 0, state.AnchorSessionUID + 1, 0f, 0, 0, 0);
                        StateService.ReAnchorForNewWeekend(state, fakeHdr, state.TrackId);
                        Log.Success("[✓] Manual Started New Weekend");
                        break;

                    case ConsoleKey.D6:
                    case ConsoleKey.NumPad6:
                        state.DebugPackets = !state.DebugPackets;
                        Console.WriteLine(state.DebugPackets ? "[i] Debug Mode: ON" : "[i] Debug Mode: OFF");
                        for (int i = 0; i < state.PidCounters.Length; i++)
                            if (state.PidCounters[i] > 0)
                                Console.WriteLine($"  • PID {i}: {state.PidCounters[i]}");
                        break;

                    case ConsoleKey.D0:
                    case ConsoleKey.NumPad0:
                        setRunning(false);
                        Log.Success("[✓] Exiting…");
                        return;
                }
            }
        }

        private static void PrintBuffer(AppState state)
        {
            int total = state.SessionBuffers.Sum(kv => kv.Value.Count);
            Console.WriteLine($"[i] Buffered Rows: {total}");

            foreach (var kv in state.SessionBuffers.OrderBy(k => k.Key))
            {
                var uid = kv.Key;
                var snapshot = kv.Value.ToArray();
                var byDrv = snapshot.GroupBy(r => r.DriverColumn, StringComparer.OrdinalIgnoreCase)
                                    .OrderBy(g => g.Key, StringComparer.OrdinalIgnoreCase);

                Console.WriteLine($"    Session {uid}: Rows={snapshot.Length}, Drivers={byDrv.Count()}");
                foreach (var g in byDrv)
                {
                    int laps = g.Select(x => x.Lap).Distinct().Count();
                    var avg = g.Average(x => x.LapTimeMs);
                    Console.WriteLine($"    {g.Key}  Laps={laps,-3} Avg={Helpers.MsToStr((int)Math.Round(avg))}");
                }
            }
        }

        private static void TryOpenFolder(AppState state)
        {
            try
            {
                var folder = SaveService.GetOutputDir(state);
                var psi = new System.Diagnostics.ProcessStartInfo
                {
                    FileName = folder,
                    UseShellExecute = true
                };
                System.Diagnostics.Process.Start(psi);
                Log.Success("[✓] Opening Folder…");
            }
            catch (Exception ex)
            {
                Log.Warn("[warning] Error Opening Folder: " + ex.Message);
            }
        }
    }
}