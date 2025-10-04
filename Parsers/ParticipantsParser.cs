using System;
using F12020TelemetryLogger.Models;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Parsers
{
    internal static class ParticipantsParser
    {
        public static void Parse(ReadOnlySpan<byte> buf, PacketHeader hdr, AppState state)
        {
            int off = Constants.HEADER_SIZE;
            if (buf.Length < off + 1)
            {
                if (state.DebugPackets)
                    Log.Warn($"[warning] Participants packet too small: {buf.Length} bytes");
                return;
            }

            int numActive = buf[off]; 
            off += 1;

            if (state.DebugPackets)
                Console.WriteLine($"[DBG] Parsing participants: {numActive} active cars");

            for (int car = 0; car < Math.Min(numActive, Constants.MAX_CARS); car++)
            {
                try
                {
                    int start = off + car * Constants.PARTICIPANT_STRIDE;
                    if (start + Constants.PARTICIPANT_STRIDE > buf.Length)
                    {
                        if (state.DebugPackets)
                            Log.Warn($"[warning] Participant data incomplete for car {car}");
                        break;
                    }

                    byte aiControlled = buf[start + 0];
                    byte teamId = buf[start + 2];

                    var nameBytes = buf.Slice(start + 5, 48);

                    string name = Helpers.DecodeName(nameBytes);
                    if (string.IsNullOrWhiteSpace(name))
                    {
                        name = (aiControlled == 1) ? "AI" : "Player";
                    }
                    string display = $"{name} [{car}]";

                    state.DriverDisplay[car] = display;
                    state.DriverRawName[car] = name;
                    state.TeamByCar[car] = teamId;

                    if (state.DebugPackets)
                        Console.WriteLine($"[DBG] Car {car}: {display}, Team={teamId}, AI={aiControlled}");
                }
                catch (Exception ex)
                {
                    if (state.DebugPackets)
                        Log.Warn($"[warning] Error parsing participant {car}: {ex.Message}");
                }
            }
        }
    }
}