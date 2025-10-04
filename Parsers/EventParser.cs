using System;
using System.Text;
using System.Threading.Tasks;
using F12020TelemetryLogger.Models;
using F12020TelemetryLogger.Services;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Parsers
{
    internal static class EventParser
    {
        public static void Parse(ReadOnlySpan<byte> buf, PacketHeader hdr, AppState state, Func<bool, Task> saveCallback)
        {
            if (buf.Length < Constants.HEADER_SIZE + 4)
            {
                if (state.DebugPackets)
                    Log.Warn($"[warning] Event packet too small: {buf.Length} bytes");
                return;
            }

            string code = Encoding.UTF8.GetString(buf.Slice(Constants.HEADER_SIZE, 4));

            if (state.DebugPackets)
                Console.WriteLine($"[DBG] Event received: {code}");

            switch (code)
            {
                case "SSTA":
                    Console.WriteLine("[EVT] Session Started");
                    break;

                case "SEND":
                    Console.WriteLine("[EVT] Session Ended - Triggering save...");
                    ScWindowService.CloseOpenWindows(state);
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await saveCallback(true);
                            Console.WriteLine("[EVT] Session Ended — Data Saved");
                        }
                        catch (Exception ex)
                        {
                            Log.Warn("[warning] Save on SEND failed: " + ex.Message);
                        }
                    });
                    break;

                case "SCDP":
                    Console.WriteLine("[EVT] Safety Car Deployed");
                    state.CurrentSC = 1;
                    ScWindowService.StartWindow(state, hdr.SessionUID, "SC", hdr.SessionTime, "[Event]");
                    break;

                case "SCDE":
                    Console.WriteLine("[EVT] Safety Car Ending");
                    state.CurrentSC = 0;
                    ScWindowService.EndWindow(state, hdr.SessionUID, "SC", hdr.SessionTime, "[Event]");
                    break;

                case "VSCN":
                    Console.WriteLine("[EVT] Virtual Safety Car");
                    state.CurrentSC = 2;
                    ScWindowService.StartWindow(state, hdr.SessionUID, "VSC", hdr.SessionTime, "[Event]");
                    break;

                case "VSCF":
                    Console.WriteLine("[EVT] Virtual Safety Car Ending");
                    state.CurrentSC = 0;
                    ScWindowService.EndWindow(state, hdr.SessionUID, "VSC", hdr.SessionTime, "[Event]");
                    break;

                case "PENA":
                    ParsePenaltyEvent(buf, hdr, state);
                    break;

                case "FTLP":
                    if (state.DebugPackets) Console.WriteLine("[EVT] Fastest Lap");
                    break;

                case "RTMT":
                    if (state.DebugPackets) Console.WriteLine("[EVT] Retirement");
                    break;

                case "DRSE":
                    if (state.DebugPackets) Console.WriteLine("[EVT] DRS Enabled");
                    break;

                case "DRSD":
                    if (state.DebugPackets) Console.WriteLine("[EVT] DRS Disabled");
                    break;

                case "TMPT":
                    if (state.DebugPackets) Console.WriteLine("[EVT] Team Mate in Pits");
                    break;

                case "CHQF":
                    if (state.DebugPackets) Console.WriteLine("[EVT] Chequered Flag");
                    break;

                case "RCWN":
                    if (state.DebugPackets) Console.WriteLine("[EVT] Race Winner");
                    break;

                default:
                    if (state.DebugPackets) Console.WriteLine($"[EVT] Unknown event code: {code}");
                    break;
            }
        }

        private static void ParsePenaltyEvent(ReadOnlySpan<byte> buf, PacketHeader hdr, AppState state)
        {
            int o = Constants.HEADER_SIZE + 4;
            if (o + 7 > buf.Length) return;

            byte penaltyType = buf[o + 0];
            byte infringementType = buf[o + 1];
            byte vehicleIdx = buf[o + 2];
            byte otherVehicleIdx = buf[o + 3];
            byte timeSec = buf[o + 4];
            byte lapNum = buf[o + 5];
            byte placesGained = buf[o + 6];

            string penName = Helpers.PenaltyName(penaltyType);
            string infrName = Helpers.InfringementName(infringementType);

            string penaltyText = penName;

            if (penaltyType == 1 || penaltyType == 4)
                penaltyText = $"{penName} ({timeSec}s)";
            else if (penaltyType == 2 && placesGained > 0)
                penaltyText = $"{penName} (+{placesGained})";

            if (lapNum > 0)
            {
                switch (penaltyType)
                {
                    case Constants.PENALTY_THIS_LAP_INVALID:
                    case Constants.PENALTY_THIS_LAP_INVALID_NO_REASON:
                        state.InvalidLaps.Add((hdr.SessionUID, vehicleIdx, lapNum));
                        if (state.DebugPackets)
                            Console.WriteLine($"[DBG] Lap {lapNum} invalidated for car {vehicleIdx} (this lap only)");
                        break;

                    case Constants.PENALTY_THIS_NEXT_LAP_INVALID:
                    case Constants.PENALTY_THIS_NEXT_LAP_INVALID_NO_REASON:
                        state.InvalidLaps.Add((hdr.SessionUID, vehicleIdx, lapNum));
                        state.InvalidLaps.Add((hdr.SessionUID, vehicleIdx, lapNum + 1));
                        if (state.DebugPackets)
                            Console.WriteLine($"[DBG] Laps {lapNum} and {lapNum + 1} invalidated for car {vehicleIdx}");
                        break;

                    case Constants.PENALTY_THIS_PREV_LAP_INVALID:
                    case Constants.PENALTY_THIS_PREV_LAP_INVALID_NO_REASON:
                        state.InvalidLaps.Add((hdr.SessionUID, vehicleIdx, lapNum));
                        if (lapNum > 1)
                        {
                            state.InvalidLaps.Add((hdr.SessionUID, vehicleIdx, lapNum - 1));
                            if (state.DebugPackets)
                                Console.WriteLine($"[DBG] Laps {lapNum - 1} and {lapNum} invalidated for car {vehicleIdx}");
                        }
                        else
                        {
                            if (state.DebugPackets)
                                Console.WriteLine($"[DBG] Lap {lapNum} invalidated for car {vehicleIdx} (previous lap N/A)");
                        }
                        break;
                }
            }

            if (infringementType >= Constants.INFRINGEMENT_CORNER_CUTTING_LAP_INVALID &&
                infringementType <= Constants.INFRINGEMENT_LAP_INVALID_RESET)
            {
                if (lapNum > 0)
                {
                    state.InvalidLaps.Add((hdr.SessionUID, vehicleIdx, lapNum));
                    if (state.DebugPackets)
                        Console.WriteLine($"[DBG] Lap {lapNum} invalidated for car {vehicleIdx} (infringement: {infrName})");
                }
            }

            var inc = new IncidentRow(
                Lap: lapNum,
                CarIdx: vehicleIdx,
                Accident: infrName,
                Penalty: penaltyText,
                Seconds: timeSec,
                PlacesGained: placesGained,
                OtherCar: otherVehicleIdx,
                SessTimeSec: hdr.SessionTime
            );

            var list = state.Incidents.GetOrAdd(hdr.SessionUID, _ => new());
            list.Add(inc);
        }
    }
}