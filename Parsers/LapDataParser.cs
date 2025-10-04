using System;
using System.Buffers.Binary;
using System.Linq;
using F12020TelemetryLogger.Models;
using F12020TelemetryLogger.Services;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Parsers
{
    internal static class LapDataParser
    {
        public static void Parse(ReadOnlySpan<byte> buf, PacketHeader hdr, AppState state)
        {
            int baseOff = Constants.HEADER_SIZE;
            if (baseOff + Constants.LAPDATA_STRIDE * Constants.MAX_CARS > buf.Length)
            {
                if (state.DebugPackets)
                    Log.Warn($"[warning] LapData packet too small: {buf.Length} bytes");
                return;
            }

            for (int carIdx = 0; carIdx < Constants.MAX_CARS; carIdx++)
            {
                try
                {
                    int start = baseOff + carIdx * Constants.LAPDATA_STRIDE;

                    float lastLapSec = BitConverter.ToSingle(buf.Slice(start + Constants.LAPDATA_LASTLAP_OFFSET, 4));
                    float currentLapTimeSec = BitConverter.ToSingle(buf.Slice(start + Constants.LAPDATA_CURRENTLAP_OFFSET, 4));
                    ushort s1Live = BinaryPrimitives.ReadUInt16LittleEndian(buf.Slice(start + Constants.LAPDATA_SECTOR1_OFFSET, 2));
                    ushort s2Live = BinaryPrimitives.ReadUInt16LittleEndian(buf.Slice(start + Constants.LAPDATA_SECTOR2_OFFSET, 2));
                    int currentLapNum = buf[start + Constants.LAPDATA_CURRENTLAPNUM_OFFSET];
                    int sectorIdx = buf[start + Constants.LAPDATA_SECTOR_OFFSET];
                    int currentLapInvalid = buf[start + Constants.LAPDATA_CURRENTLAPINVALID_OFFSET];

                    if (state.DebugPackets && carIdx == 0)
                    {
                        Console.WriteLine($"[DBG] car=0: lastLap={lastLapSec:F3}s, curLap={currentLapTimeSec:F3}s, lapNum={currentLapNum}, sector={sectorIdx}, s1={s1Live}ms, s2={s2Live}ms, invalid={currentLapInvalid}");
                    }

                    double curLapStartSec = Math.Max(0, hdr.SessionTime - currentLapTimeSec);
                    state.LapStartSec[carIdx] = curLapStartSec;
                    state.LiveLapNo[carIdx] = currentLapNum;
                    state.LiveSectorIdx[carIdx] = sectorIdx;
                    state.LiveS1[carIdx] = s1Live;
                    state.LiveS2[carIdx] = s2Live;

                    if (s1Live > 0 && s2Live > 0 && currentLapNum > 0)
                    {
                        state.BufferedSectors[carIdx] = new AppState.SectorBuffer(
                            S1: s1Live,
                            S2: s2Live,
                            LapNum: currentLapNum,
                            SessionUID: hdr.SessionUID
                        );
                    }

                    float prevLast = state.LastLapTimeSeen.TryGetValue(carIdx, out var p) ? p : -1f;
                    int prevLap = state.LastLapNum.TryGetValue(carIdx, out var pl) ? pl : -1;

                    state.LastLapTimeSeen[carIdx] = lastLapSec;
                    state.LastLapNum[carIdx] = currentLapNum;

                    bool lapClosed = currentLapNum > prevLap
                                     && prevLap > 0
                                     && lastLapSec >= Constants.MIN_VALID_LAP_TIME_SEC;

                    if (!lapClosed) continue;

                    int completedLapNum = prevLap;
                    state.WrittenLapCount[carIdx] = completedLapNum;

                    int lapTimeMs = (int)Math.Round(lastLapSec * 1000.0);

                    if (lapTimeMs > Constants.MAX_REASONABLE_LAP_MS)
                    {
                        if (state.DebugPackets)
                            Console.WriteLine($"[DBG] Lap {completedLapNum} for car {carIdx} too slow ({Helpers.MsToStr(lapTimeMs)}), skipping");
                        continue;
                    }

                    int finalS1 = 0, finalS2 = 0, finalS3 = 0;
                    bool sectorsFound = false;

                    if (state.BufferedSectors.TryGetValue(carIdx, out var buffer))
                    {
                        if (buffer.LapNum == completedLapNum && buffer.SessionUID == hdr.SessionUID)
                        {
                            finalS1 = buffer.S1;
                            finalS2 = buffer.S2;
                            finalS3 = lapTimeMs - finalS1 - finalS2;

                            if (finalS3 < 0)
                            {
                                if (state.DebugPackets)
                                    Console.WriteLine($"[DBG] Negative S3 for car {carIdx} lap {completedLapNum}: S1={finalS1}, S2={finalS2}, LapTime={lapTimeMs}");
                                finalS3 = 0;
                            }

                            sectorsFound = true;

                            if (state.DebugPackets)
                                Console.WriteLine($"[DBG] Using buffered sectors for car {carIdx} lap {completedLapNum}: S1={finalS1}, S2={finalS2}, S3={finalS3}");
                        }
                        else if (state.DebugPackets)
                        {
                            Console.WriteLine($"[DBG] Buffer mismatch for car {carIdx}: buffer lap={buffer.LapNum}, completed lap={completedLapNum}");
                        }
                    }

                    if (!sectorsFound)
                    {
                        if (state.DebugPackets)
                            Console.WriteLine($"[DBG] No valid sectors found for car {carIdx} lap {completedLapNum}, skipping");
                        continue;
                    }

                    bool isLapInvalid = state.InvalidLaps.Contains((hdr.SessionUID, carIdx, completedLapNum)) ||
                                        state.LiveInvalidLaps.Contains((hdr.SessionUID, carIdx, completedLapNum));

                    bool v1 = !isLapInvalid;
                    bool v2 = !isLapInvalid;
                    bool v3 = !isLapInvalid;
                    bool vL = !isLapInvalid;

                    var dictByCar = state.LapSectors.GetOrAdd(hdr.SessionUID, _ => new());
                    var lapMap = dictByCar.GetOrAdd(carIdx, _ => new());

                    lapMap[completedLapNum] = new AppState.SectorRec(
                        finalS1, finalS2, finalS3,
                        v1, v2, v3, vL
                    );

                    if (state.DebugPackets)
                    {
                        string validity = vL ? "VALID" : "INVALID";
                        Console.WriteLine($"[DBG] LAP CLOSED car={carIdx} lap={completedLapNum} lapMs={lapTimeMs} S1={finalS1} S2={finalS2} S3={finalS3} [{validity}]");
                    }

                    double lapEndSec = hdr.SessionTime;
                    double lapStartSec = state.LapStartSec.TryGetValue(carIdx, out var ls) ? ls : Math.Max(0, lapEndSec - lastLapSec);
                    string scTag = ScWindowService.GetScOverlap(state, hdr.SessionUID, lapStartSec, lapEndSec);
                    string display = state.DriverDisplay.TryGetValue(carIdx, out var disp) ? disp : $"Unknown [{carIdx}]";
                    string rawName = state.DriverRawName.TryGetValue(carIdx, out var nm) ? nm : "Unknown";
                    state.LastWear.TryGetValue(carIdx, out var wear);
                    string tyre = state.CurrentTyre.TryGetValue(carIdx, out var t) ? t : "Unknown";
                    int tyreAge = state.LastTyreAge.TryGetValue(carIdx, out var age) ? age : -1;
                    byte? fuelMix = null;
                    string? mixHistory = null;

                    if (state.FuelMixHistory.TryGetValue(carIdx, out var history) && history.Count > 0)
                    {
                        var lapHistory = history
                            .Where(h => h.timestamp >= lapStartSec && h.timestamp <= lapEndSec)
                            .OrderBy(h => h.timestamp)
                            .ToList();

                        if (lapHistory.Count > 0)
                        {
                            var mixDurations = new System.Collections.Generic.Dictionary<byte, double>();

                            for (int i = 0; i < lapHistory.Count; i++)
                            {
                                byte currentMix = lapHistory[i].mix;
                                double startTime = lapHistory[i].timestamp;
                                double endTime = (i + 1 < lapHistory.Count)
                                    ? lapHistory[i + 1].timestamp
                                    : lapEndSec;

                                double duration = endTime - startTime;

                                if (!mixDurations.ContainsKey(currentMix))
                                    mixDurations[currentMix] = 0;

                                mixDurations[currentMix] += duration;
                            }

                            fuelMix = mixDurations.OrderByDescending(kv => kv.Value).First().Key;

                            var switches = new System.Collections.Generic.List<string>();

                            foreach (var (mix, ts) in lapHistory)
                            {
                                double timeInLap = ts - lapStartSec;
                                switches.Add($"{Helpers.GetFuelMixName(mix)} ( {timeInLap:F1}s )");
                            }

                            mixHistory = string.Join(" → ", switches);

                            if (state.DebugPackets)
                            {
                                Console.WriteLine($"[DBG] Car {carIdx} lap {completedLapNum}: mix={Helpers.GetFuelMixName(fuelMix.Value)}, history={mixHistory}");
                            }
                        }
                    }

                    if (!fuelMix.HasValue)
                    {
                        fuelMix = state.LastFuelMix.TryGetValue(carIdx, out var fm) ? fm : (byte?)null;
                    }

                    var row = new ParsedRow(
                        SessionUID: hdr.SessionUID,
                        SessionType: state.SessionType,
                        CarIdx: carIdx,
                        DriverColumn: display,
                        DriverRaw: rawName,
                        Lap: completedLapNum,
                        Tyre: tyre,
                        LapTimeMs: lapTimeMs,
                        WearAvg: wear?.Avg,
                        RL: wear?.RL,
                        RR: wear?.RR,
                        FL: wear?.FL,
                        FR: wear?.FR,
                        SessTimeSec: hdr.SessionTime,
                        ScStatus: scTag,
                        TyreAge: tyreAge,
                        S1Ms: finalS1,
                        S2Ms: finalS2,
                        S3Ms: finalS3,
                        FuelMix: fuelMix,
                        FuelMixHistory: mixHistory
                    );

                    var q = state.SessionBuffers.GetOrAdd(hdr.SessionUID, _ => new());
                    q.Enqueue(row);
                    ValidationService.UpdateProvisionalAgg(state, hdr.SessionUID, row);

                    if (state.FuelMixHistory.TryGetValue(carIdx, out var hist))
                    {
                        hist.Clear();
                        if (state.DebugPackets)
                            Console.WriteLine($"[DBG] Cleared fuel mix history for car {carIdx} after lap {completedLapNum}");
                    }
                }
                catch (Exception ex)
                {
                    if (state.DebugPackets)
                        Log.Warn($"[warning] Error parsing lap data for car {carIdx}: {ex.Message}");
                }
            }
        }
    }
}