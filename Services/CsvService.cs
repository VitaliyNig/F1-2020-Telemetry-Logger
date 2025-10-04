using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using F12020TelemetryLogger.Models;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Services
{
    internal static class CsvService
    {
        public static void FixDriverNames(AppState state, string csvPath)
        {
            try
            {
                if (!File.Exists(csvPath)) return;

                var map = state.DriverDisplay.ToDictionary(kv => kv.Key, kv => kv.Value);
                if (map.Count == 0) return;

                var tmp = csvPath + ".tmp";
                using var sr = new StreamReader(csvPath, Encoding.UTF8);
                using var sw = new StreamWriter(tmp, false, Encoding.UTF8);

                string? header = sr.ReadLine();
                if (header is null) return;
                sw.WriteLine(header);

                string? line;
                while ((line = sr.ReadLine()) != null)
                {
                    var parts = Helpers.SplitCsv(line);
                    if (parts.Length < 19) { sw.WriteLine(line); continue; }

                    if (int.TryParse(parts[2], out int carIdx) && map.TryGetValue(carIdx, out var display))
                    {
                        var drvCol = parts[3].Trim('"');
                        if (drvCol.Equals($"Unknown [{carIdx}]", StringComparison.OrdinalIgnoreCase))
                        {
                            parts[3] = Helpers.Csv(display);

                            var raw = parts[4].Trim('"');
                            if (string.IsNullOrWhiteSpace(raw) || raw.Equals("Unknown", StringComparison.OrdinalIgnoreCase))
                            {
                                var p2 = display.LastIndexOf('[');
                                var rawName = p2 > 0 ? display.Substring(0, p2).Trim() : display;
                                parts[4] = Helpers.Csv(rawName);
                            }

                            sw.WriteLine(string.Join(",", parts));
                            continue;
                        }
                    }
                    sw.WriteLine(line);
                }

                sr.Close(); sw.Flush(); sw.Close();

                File.Copy(tmp, csvPath, overwrite: true);
                File.Delete(tmp);
            }
            catch (Exception ex)
            {
                Log.Warn("[warning] FixCsvDriverNames: " + ex.Message);
            }
        }

        public static void FixBarePlayerAi(AppState state, string csvPath)
        {
            try
            {
                if (!File.Exists(csvPath)) return;

                var tmp = csvPath + ".tmp2";
                using var sr = new StreamReader(csvPath, Encoding.UTF8);
                using var sw = new StreamWriter(tmp, false, Encoding.UTF8);

                string? header = sr.ReadLine();
                if (header is null) return;
                sw.WriteLine(header);

                string? line;
                while ((line = sr.ReadLine()) != null)
                {
                    var parts = Helpers.SplitCsv(line);

                    if (parts.Length < 19) { sw.WriteLine(line); continue; }
                    if (!int.TryParse(parts[2], out int carIdx)) { sw.WriteLine(line); continue; }

                    string drvCol = parts[3].Trim('"');
                    string raw = parts[4].Trim('"');

                    bool isBarePlayerOrAi =
                        drvCol.Equals("Player", StringComparison.OrdinalIgnoreCase) ||
                        drvCol.Equals("AI", StringComparison.OrdinalIgnoreCase);

                    bool hasIndex = drvCol.EndsWith("]") && drvCol.Contains('[');

                    if (isBarePlayerOrAi && !hasIndex)
                    {
                        string display = state.DriverDisplay.TryGetValue(carIdx, out var disp)
                                         ? disp
                                         : $"{drvCol} [{carIdx}]";

                        parts[3] = Helpers.Csv(display);

                        if (string.IsNullOrWhiteSpace(raw))
                        {
                            var p2 = display.LastIndexOf('[');
                            var rawName = p2 > 0 ? display.Substring(0, p2).Trim() : display;
                            parts[4] = Helpers.Csv(rawName);
                        }

                        sw.WriteLine(string.Join(",", parts));
                    }
                    else
                    {
                        sw.WriteLine(line);
                    }
                }

                sr.Close(); sw.Flush(); sw.Close();
                File.Copy(tmp, csvPath, overwrite: true);
                File.Delete(tmp);
            }
            catch (Exception ex)
            {
                Log.Warn("[warning] FixCsvBarePlayerAi: " + ex.Message);
            }
        }

        public static ParsedRow? ParseCsvRow(string line)
        {
            var p = Helpers.SplitCsv(line);
            if (p == null || p.Length != 21) return null;

            return new ParsedRow(
                SessionUID: ulong.Parse(p[0]),
                SessionType: p[1],
                CarIdx: int.Parse(p[2]),
                DriverColumn: p[3],
                DriverRaw: p[4],
                Lap: int.Parse(p[5]),
                Tyre: p[6],
                LapTimeMs: int.Parse(p[7]),
                WearAvg: string.IsNullOrWhiteSpace(p[8]) ? (double?)null : double.Parse(p[8], CultureInfo.InvariantCulture),
                RL: string.IsNullOrWhiteSpace(p[9]) ? (int?)null : int.Parse(p[9], CultureInfo.InvariantCulture),
                RR: string.IsNullOrWhiteSpace(p[10]) ? (int?)null : int.Parse(p[10], CultureInfo.InvariantCulture),
                FL: string.IsNullOrWhiteSpace(p[11]) ? (int?)null : int.Parse(p[11], CultureInfo.InvariantCulture),
                FR: string.IsNullOrWhiteSpace(p[12]) ? (int?)null : int.Parse(p[12], CultureInfo.InvariantCulture),
                SessTimeSec: double.TryParse(p[13], NumberStyles.Any, CultureInfo.InvariantCulture, out var st) ? st : 0.0,
                ScStatus: p[14],
                TyreAge: int.TryParse(p[15], out var age) ? age : -1,
                S1Ms: int.TryParse(p[16], out var s1) ? s1 : 0,
                S2Ms: int.TryParse(p[17], out var s2) ? s2 : 0,
                S3Ms: int.TryParse(p[18], out var s3) ? s3 : 0,
                FuelMix: string.IsNullOrWhiteSpace(p[19]) ? (byte?)null : byte.Parse(p[19]),
                FuelMixHistory: p.Length > 20 ? p[20] : null
            );
        }

        public static void NormalizeDriverNames(AppState state, List<ParsedRow> rows)
        {
            if (rows == null || rows.Count == 0) return;

            for (int i = 0; i < rows.Count; i++)
            {
                var r = rows[i];
                string col = r.DriverColumn;

                if (col.Equals($"Unknown [{r.CarIdx}]", StringComparison.OrdinalIgnoreCase)
                    && state.DriverDisplay.TryGetValue(r.CarIdx, out var displayName))
                {
                    var p = displayName.LastIndexOf('[');
                    var raw = p > 0 ? displayName.Substring(0, p).Trim() : displayName;
                    rows[i] = r with
                    {
                        DriverColumn = displayName,
                        DriverRaw = string.IsNullOrWhiteSpace(r.DriverRaw) ? raw : r.DriverRaw
                    };
                    continue;
                }

                bool isBare = col.Equals("Player", StringComparison.OrdinalIgnoreCase) ||
                              col.Equals("AI", StringComparison.OrdinalIgnoreCase);
                bool hasIdx = col.EndsWith("]") && col.Contains('[');

                if (isBare && !hasIdx)
                {
                    string baseName = col;
                    string dispName = $"{baseName} [{r.CarIdx}]";

                    if (state.DriverDisplay.TryGetValue(r.CarIdx, out var betterName))
                        dispName = betterName;

                    string raw = string.IsNullOrWhiteSpace(r.DriverRaw) ? baseName : r.DriverRaw;

                    rows[i] = r with { DriverColumn = dispName, DriverRaw = raw };
                }
            }
        }
    }
}