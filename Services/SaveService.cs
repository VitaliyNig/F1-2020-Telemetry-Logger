using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using F12020TelemetryLogger.Excel;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Services
{
    internal static class SaveService
    {
        private static readonly object SaveLock = new();

        public static async Task SaveAllAsync(AppState state, bool makeExcel, ref bool saving)
        {
            if (!Monitor.TryEnter(SaveLock, 0))
            {
                Console.WriteLine("[i] Save already in progress, skipping...");
                return;
            }

            try
            {
                saving = true;
                var (csvPath, xlsxPath) = GetStagePaths(state);
                Directory.CreateDirectory(Path.GetDirectoryName(csvPath)!);

                int appended = 0;
                bool newFile = !File.Exists(csvPath);
                
                using (var fs = new FileStream(csvPath, FileMode.Append, FileAccess.Write, FileShare.Read))
                using (var sw = new StreamWriter(fs, Encoding.UTF8))
                {
                    if (newFile)
                    {
                        await sw.WriteLineAsync("session_uid,session_type,car_idx,driver_column,driver_raw,lap,tyre,lap_time_ms,wear_avg,wear_rl,wear_rr,wear_fl,wear_fr,sess_time_sec,sc_status,tyre_age,s1_ms,s2_ms,s3_ms,fuel_mix,fuel_mix_history");
                    }

                    foreach (var kv in state.SessionBuffers.ToArray())
                    {
                        var uid = kv.Key;
                        var q = kv.Value;
                        if (q.IsEmpty) continue;

                        while (q.TryDequeue(out var r))
                        {
                            string sc = !string.IsNullOrEmpty(r.ScStatus) 
                                ? r.ScStatus! 
                                : ScWindowService.GetScAt(state, uid, r.SessTimeSec);

                            await sw.WriteLineAsync(string.Join(",",
                                r.SessionUID,
                                r.SessionType,
                                r.CarIdx,
                                Helpers.Csv(r.DriverColumn),
                                Helpers.Csv(r.DriverRaw),
                                r.Lap,
                                Helpers.Csv(r.Tyre),
                                r.LapTimeMs,
                                (r.WearAvg?.ToString(CultureInfo.InvariantCulture) ?? ""),
                                (r.RL?.ToString(CultureInfo.InvariantCulture) ?? ""),
                                (r.RR?.ToString(CultureInfo.InvariantCulture) ?? ""),
                                (r.FL?.ToString(CultureInfo.InvariantCulture) ?? ""),
                                (r.FR?.ToString(CultureInfo.InvariantCulture) ?? ""),
                                r.SessTimeSec.ToString(CultureInfo.InvariantCulture),
                                sc,
                                r.TyreAge.ToString(CultureInfo.InvariantCulture),
                                r.S1Ms, r.S2Ms, r.S3Ms,
                                r.FuelMix?.ToString() ?? "",
                                Helpers.Csv(r.FuelMixHistory ?? "")
                            ));
                            appended++;
                        }
                    }
                }
                
                if (appended > 0)
                    Console.WriteLine($"[+] {Path.GetFileName(csvPath)} +{appended}");

                if (makeExcel)
                {
                    CsvService.FixDriverNames(state, csvPath);
                    CsvService.FixBarePlayerAi(state, csvPath);

                    try
                    {
                        ExcelBuilder.BuildExcel(state, csvPath, xlsxPath);
                        Console.WriteLine($"[i] File Rebuilt: {Path.GetFileName(xlsxPath)}");
                    }
                    catch (Exception ex)
                    {
                        Log.Warn("[warning] Excel Build Failed: " + ex.Message);
                    }

                    state.LastFinalSaveUtc = DateTime.UtcNow;
                }
            }
            finally
            {
                saving = false;
                Monitor.Exit(SaveLock);
            }
        }

        public static (string csvPath, string xlsxPath) GetStagePaths(AppState state)
        {
            string outDir = GetOutputDir(state);
            string track = string.IsNullOrWhiteSpace(state.AnchorTrackName) 
                ? "UnknownTrack" 
                : Helpers.SafeName(state.AnchorTrackName);
            string uid = (state.AnchorSessionUID != 0) 
                ? state.AnchorSessionUID.ToString() 
                : "session";

            string baseName = $"log_f1_2020_{track}_{uid}";
            return (Path.Combine(outDir, baseName + ".csv"),
                    Path.Combine(outDir, baseName + ".xlsx"));
        }

        public static string GetOutputDir(AppState state)
        {
            string outDir = Path.Combine(AppContext.BaseDirectory, "Logs", state.AnchorLocalDate);
            Directory.CreateDirectory(outDir);
            return outDir;
        }
    }
}