using System;
using F12020TelemetryLogger.Models;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Services
{
    internal static class StateService
    {
        public static void ReAnchorForNewWeekend(AppState state, PacketHeader hdr, byte? trackIdHint = null)
        {
            state.ScWindows.Clear();
            state.SessionBuffers.Clear();
            state.AnchorInitialized = true;
            state.AnchorSessionUID = hdr.SessionUID;
            state.AnchorLocalDate = DateTime.Now.ToString("yyyy-MM-dd");
            
            if (trackIdHint.HasValue) 
                state.AnchorTrackId = trackIdHint;
            
            state.AnchorTrackName = trackIdHint.HasValue 
                ? Helpers.TrackName(trackIdHint.Value) 
                : state.AnchorTrackName;
            
            state.LastLapNum.Clear();
            state.LastLapTimeSeen.Clear();
            state.WrittenLapCount.Clear();
            state.LapStartSec.Clear();
            state.DriverDisplay.Clear();
            state.DriverRawName.Clear();
            state.LastWear.Clear();
            state.CurrentTyre.Clear();
            state.MaxSpeedQuali.Clear();
            state.MaxSpeedRace.Clear();
            state.LastTyreAge.Clear();
            state.Incidents.Clear();
            state.Agg.Clear();
            state.FinalOrder.Clear();
            state.LapSectors.Clear();
            state.LiveS1.Clear();
            state.LiveS2.Clear();
            state.LiveLapNo.Clear();
            state.LiveSectorIdx.Clear();
            state.LiveInvalidLaps.Clear();
            state.BufferedSectors.Clear();
            state.LastFuelMix.Clear();

            Console.WriteLine($"[i] New Weekend Anchor: Date={state.AnchorLocalDate}, Track={state.AnchorTrackName}, SessionUID={state.AnchorSessionUID}");
        }
    }
}