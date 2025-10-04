using System;
using F12020TelemetryLogger.Models;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Parsers
{
    internal static class CarStatusParser
    {
        public static void Parse(ReadOnlySpan<byte> buf, PacketHeader hdr, AppState state)
        {
            int off = Constants.HEADER_SIZE;
            if (off + Constants.CARSTATUS_STRIDE * Constants.MAX_CARS > buf.Length)
            {
                if (state.DebugPackets)
                    Log.Warn($"[warning] CarStatus packet too small: {buf.Length} bytes");
                return;
            }

            for (int car = 0; car < Constants.MAX_CARS; car++)
            {
                try
                {
                    int start = off + car * Constants.CARSTATUS_STRIDE;
                    var block = buf.Slice(start, Constants.CARSTATUS_STRIDE);

                    int rl = block[Constants.CARSTATUS_WEAR_RL_OFFSET];
                    int rr = block[Constants.CARSTATUS_WEAR_RR_OFFSET];
                    int fl = block[Constants.CARSTATUS_WEAR_FL_OFFSET];
                    int fr = block[Constants.CARSTATUS_WEAR_FR_OFFSET];

                    if (Helpers.IsPct(rl) && Helpers.IsPct(rr) && Helpers.IsPct(fl) && Helpers.IsPct(fr))
                        state.LastWear[car] = new WearInfo(rl, rr, fl, fr);

                    string tyre = Helpers.TyreName(
                        block[Constants.CARSTATUS_ACTUAL_COMPOUND_OFFSET],
                        block[Constants.CARSTATUS_VISUAL_COMPOUND_OFFSET]);
                    state.CurrentTyre[car] = tyre;

                    int age = block.Length > Constants.CARSTATUS_TYRE_AGE_OFFSET 
                        ? block[Constants.CARSTATUS_TYRE_AGE_OFFSET] 
                        : -1;
                    if (age >= 0 && age <= 200)
                        state.LastTyreAge[car] = age;
                    else
                        state.LastTyreAge[car] = -1;

                    byte fuelMix = block.Length > Constants.CARSTATUS_FUEL_MIX_OFFSET 
                        ? block[Constants.CARSTATUS_FUEL_MIX_OFFSET] 
                        : (byte)0;
                    
                    if (fuelMix >= 0 && fuelMix <= 3)
                    {
                        bool mixChanged = !state.LastFuelMix.TryGetValue(car, out var prevMix) || prevMix != fuelMix;

                        state.LastFuelMix[car] = fuelMix;

                        if (mixChanged)
                        {
                            if (!state.FuelMixHistory.ContainsKey(car))
                                state.FuelMixHistory[car] = new();

                            state.FuelMixHistory[car].Add((fuelMix, hdr.SessionTime));

                            if (state.DebugPackets)
                                Console.WriteLine($"[DBG] Car {car}: Mix {Helpers.GetFuelMixName(fuelMix)} at {hdr.SessionTime:F1}s");
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (state.DebugPackets)
                        Log.Warn($"[warning] Error parsing car status for car {car}: {ex.Message}");
                }
            }
        }
    }
}