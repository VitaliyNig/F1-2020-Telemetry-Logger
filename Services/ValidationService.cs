using System;
using System.Collections.Generic;
using System.Linq;
using F12020TelemetryLogger.Models;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Services
{
    internal static class ValidationService
    {
        public static bool IsQualiSession(ParsedRow r)
            => r.SessionType.Equals("ShortQ", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("Q1", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("Q2", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("Q3", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("OSQ", StringComparison.OrdinalIgnoreCase);

        public static bool IsRaceSession(ParsedRow r)
            => r.SessionType.Equals("Race", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("R2", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("R", StringComparison.OrdinalIgnoreCase);

        public static bool IsQualiNow(AppState state)
            => state.SessionType.Equals("ShortQ", StringComparison.OrdinalIgnoreCase)
            || state.SessionType.Equals("Q1", StringComparison.OrdinalIgnoreCase)
            || state.SessionType.Equals("Q2", StringComparison.OrdinalIgnoreCase)
            || state.SessionType.Equals("Q3", StringComparison.OrdinalIgnoreCase)
            || state.SessionType.Equals("OSQ", StringComparison.OrdinalIgnoreCase);

        public static bool IsRaceNow(AppState state)
            => state.SessionType.Equals("Race", StringComparison.OrdinalIgnoreCase)
            || state.SessionType.Equals("R2", StringComparison.OrdinalIgnoreCase)
            || state.SessionType.Equals("R", StringComparison.OrdinalIgnoreCase);

        public static bool IsSectorValid(AppState state, ParsedRow r, int sector)
        {
            if (state.LapSectors.TryGetValue(r.SessionUID, out var byCar) &&
                byCar.TryGetValue(r.CarIdx, out var laps) &&
                laps.TryGetValue(r.Lap, out var rec))
            {
                return rec.LapValid;
            }

            if (state.InvalidLaps.Contains((r.SessionUID, r.CarIdx, r.Lap)))
            {
                if (state.DebugPackets)
                    Console.WriteLine($"[DBG] Lap {r.Lap} invalid for car {r.CarIdx} (penalty)");
                return false;
            }

            if (state.LiveInvalidLaps.Contains((r.SessionUID, r.CarIdx, r.Lap)))
            {
                if (state.DebugPackets)
                    Console.WriteLine($"[DBG] Lap {r.Lap} invalid for car {r.CarIdx} (live flag)");
                return false;
            }

            return true;
        }

        public static bool IsValidLap(AppState state, ParsedRow r)
        {
            if (r == null) return false;
            if (r.LapTimeMs <= 0) return false;

            if (r.LapTimeMs > Constants.MAX_REASONABLE_LAP_MS)
            {
                if (state.DebugPackets)
                    Console.WriteLine($"[DBG] Lap {r.Lap} for {r.DriverColumn} too slow ({Helpers.MsToStr(r.LapTimeMs)}), excluding");
                return false;
            }

            const double EPS = 0.001;
            double start = r.SessTimeSec - (r.LapTimeMs / 1000.0);
            if (start < 0) start = 0;
            double end = r.SessTimeSec;

            if (state.ScWindows.TryGetValue(r.SessionUID, out var winList) && winList.Count > 0)
            {
                foreach (var w in winList)
                {
                    var wEnd = w.EndSec ?? double.MaxValue;
                    bool overlaps = (start - EPS) <= (wEnd + EPS) && (end + EPS) >= (w.StartSec - EPS);

                    if (overlaps)
                    {
                        if (state.DebugPackets)
                            Console.WriteLine($"[DBG] Lap {r.Lap} for {r.DriverColumn} overlaps with {w.Kind}, excluding");
                        return false;
                    }
                }
            }

            if (!IsSectorValid(state, r, 1) || !IsSectorValid(state, r, 2) || !IsSectorValid(state, r, 3))
            {
                if (state.DebugPackets)
                    Console.WriteLine($"[DBG] Lap {r.Lap} for {r.DriverColumn} has invalid sector(s), excluding");
                return false;
            }

            return true;
        }

        public static List<ParsedRow> GetCleanLaps(AppState state, IEnumerable<ParsedRow> rows)
        {
            var baseRows = rows.Where(r => IsValidLap(state, r))
                               .OrderBy(r => r.DriverColumn, StringComparer.OrdinalIgnoreCase)
                               .ThenBy(r => r.Lap)
                               .ThenBy(r => r.SessTimeSec)
                               .ToList();

            var cleaned = new List<ParsedRow>(baseRows.Count);

            foreach (var g in baseRows.GroupBy(r => r.DriverColumn, StringComparer.OrdinalIgnoreCase))
            {
                string prevTyre = "";
                int prevAge = -1;
                int prevLap = -1;
                ParsedRow? prev = null;

                foreach (var r in g)
                {
                    bool compoundChanged = !string.Equals(r.Tyre ?? "", prevTyre ?? "", StringComparison.OrdinalIgnoreCase);
                    bool ageKnownNow = r.TyreAge >= 0;
                    bool ageKnownPrev = prevAge >= 0;
                    bool ageReset = ageKnownNow && ageKnownPrev && (r.TyreAge < prevAge);

                    bool wearJump = false;
                    if (r.WearAvg.HasValue && prev?.WearAvg.HasValue == true)
                    {
                        double wearDiff = r.WearAvg.Value - prev.WearAvg.Value;
                        if (wearDiff < Constants.WEAR_JUMP_THRESHOLD)
                        {
                            wearJump = true;
                            if (state.DebugPackets)
                                Console.WriteLine($"[DBG] Pit stop detected for {g.Key} lap {r.Lap} (wear jump: {prev.WearAvg.Value:F1}% -> {r.WearAvg.Value:F1}%)");
                        }
                    }

                    bool pitStop = compoundChanged || ageReset || wearJump;

                    if (pitStop)
                    {
                        if (cleaned.Count > 0 && prev != null &&
                            string.Equals(cleaned[^1].DriverColumn, g.Key, StringComparison.OrdinalIgnoreCase) &&
                            cleaned[^1].Lap == prevLap)
                        {
                            if (state.DebugPackets)
                                Console.WriteLine($"[DBG] Removing in-lap {prevLap} for {g.Key} before pit stop");
                            cleaned.RemoveAt(cleaned.Count - 1);
                        }

                        if (state.DebugPackets)
                            Console.WriteLine($"[DBG] Skipping out-lap {r.Lap} for {g.Key} after pit stop");

                        prevTyre = r.Tyre ?? "";
                        prevAge = r.TyreAge;
                        prevLap = r.Lap;
                        prev = r;
                        continue;
                    }

                    cleaned.Add(r);
                    prevTyre = r.Tyre ?? "";
                    prevAge = r.TyreAge;
                    prevLap = r.Lap;
                    prev = r;
                }
            }

            if (state.DebugPackets)
                Console.WriteLine($"[DBG] GetCleanLaps: {baseRows.Count} base rows -> {cleaned.Count} clean rows");

            return cleaned;
        }

        public static void UpdateProvisionalAgg(AppState state, ulong uid, ParsedRow r)
        {
            var byCar = state.Agg.GetOrAdd(uid, _ => new());
            var agg = byCar.GetOrAdd(r.CarIdx, _ => new AppState.DriverAgg());

            if (r.Lap > agg.Laps) agg.Laps = r.Lap;
            agg.TotalMs += r.LapTimeMs;
            if (r.LapTimeMs > 0 && r.LapTimeMs < agg.BestMs) agg.BestMs = r.LapTimeMs;

            List<int> order;
            string sessionTypeKey;

            if (IsRaceSession(r))
            {
                sessionTypeKey = "Race";
                order = byCar
                    .Select(kv => new { Car = kv.Key, A = kv.Value })
                    .OrderByDescending(x => x.A.Laps)
                    .ThenBy(x => x.A.TotalMs)
                    .Select(x => x.Car)
                    .ToList();
            }
            else if (IsQualiSession(r))
            {
                sessionTypeKey = "Quali";
                order = byCar
                    .Select(kv => new { Car = kv.Key, A = kv.Value })
                    .OrderBy(x => x.A.BestMs == int.MaxValue ? int.MaxValue : x.A.BestMs)
                    .Select(x => x.Car)
                    .ToList();
            }
            else return;

            state.FinalOrder[(uid, sessionTypeKey)] = order;
        }
    }
}