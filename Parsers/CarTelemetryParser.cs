using System;
using System.Buffers.Binary;
using F12020TelemetryLogger.Models;
using F12020TelemetryLogger.Services;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Parsers
{
    internal static class CarTelemetryParser
    {
        public static void Parse(ReadOnlySpan<byte> buf, PacketHeader hdr, AppState state)
        {
            int off = Constants.HEADER_SIZE;
            if (off + Constants.CARTELEM_STRIDE * Constants.MAX_CARS > buf.Length)
            {
                if (state.DebugPackets)
                    Log.Warn($"[warning] CarTelemetry packet too small: {buf.Length} bytes");
                return;
            }

            bool isQuali = ValidationService.IsQualiNow(state);
            bool isRace = ValidationService.IsRaceNow(state);

            if (!isQuali && !isRace) return;

            for (int car = 0; car < Constants.MAX_CARS; car++)
            {
                try
                {
                    int start = off + car * Constants.CARTELEM_STRIDE;
                    if (start + 2 > buf.Length) break;

                    ushort speed = BinaryPrimitives.ReadUInt16LittleEndian(buf.Slice(start, 2));
                    if (speed == 0) continue;

                    if (isQuali)
                    {
                        if (!state.MaxSpeedQuali.TryGetValue(car, out int cur) || speed > cur)
                        {
                            state.MaxSpeedQuali[car] = speed;
                            if (state.DebugPackets)
                                Console.WriteLine($"[DBG] New max speed (quali) for car {car}: {speed} km/h");
                        }
                    }
                    else if (isRace)
                    {
                        if (!state.MaxSpeedRace.TryGetValue(car, out int cur) || speed > cur)
                        {
                            state.MaxSpeedRace[car] = speed;
                            if (state.DebugPackets)
                                Console.WriteLine($"[DBG] New max speed (race) for car {car}: {speed} km/h");
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (state.DebugPackets)
                        Log.Warn($"[warning] Error parsing telemetry for car {car}: {ex.Message}");
                }
            }
        }
    }
}