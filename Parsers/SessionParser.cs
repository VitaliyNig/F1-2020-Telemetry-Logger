using System;
using F12020TelemetryLogger.Models;
using F12020TelemetryLogger.Services;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Parsers
{
    internal static class SessionParser
    {
        public static void Parse(ReadOnlySpan<byte> buf, PacketHeader hdr, AppState state)
        {
            int off = Constants.HEADER_SIZE;

            try
            {
                int tmp = Constants.HEADER_SIZE;
                tmp += 1 + 1 + 1 + 1 + 2;
                if (tmp < buf.Length) tmp += 1;
                byte? trackIdNow = null;
                if (tmp < buf.Length)
                {
                    sbyte tSigned = unchecked((sbyte)buf[tmp]);
                    if (tSigned >= 0) trackIdNow = (byte)tSigned;
                }

                bool trackChanged = trackIdNow.HasValue
                                    && state.AnchorTrackId.HasValue
                                    && trackIdNow.Value != state.AnchorTrackId.Value
                                    && trackIdNow.Value >= 0
                                    && trackIdNow.Value <= 26;

                if (trackChanged)
                {
                    Console.WriteLine($"[i] Track changed: {Helpers.TrackName(state.AnchorTrackId!.Value)} ({state.AnchorTrackId!.Value}) -> {Helpers.TrackName(trackIdNow!.Value)} ({trackIdNow!.Value})");
                    StateService.ReAnchorForNewWeekend(state, hdr, trackIdNow);
                }
            }
            catch (Exception ex)
            {
                if (state.DebugPackets)
                    Log.Warn($"[warning] Error parsing track change: {ex.Message}");
            }

            off += 1 + 1 + 1 + 1 + 2;
            if (off < buf.Length)
            {
                byte stype = buf[off++];
                state.SessionType = Helpers.SessionTypeName(stype);
                state.SessionTypeByUid[hdr.SessionUID] = state.SessionType;
            }

            if (off < buf.Length)
            {
                sbyte tSigned = unchecked((sbyte)buf[off++]);
                if (tSigned >= 0)
                {
                    byte t = (byte)tSigned;
                    state.TrackId = t;
                    if (!state.AnchorTrackId.HasValue) state.AnchorTrackId = t;
                    state.AnchorTrackName = Helpers.TrackName(t);
                }
                else
                {
                    state.TrackId = null;
                    if (!state.AnchorTrackId.HasValue) state.AnchorTrackName = "UnknownTrack";
                }
            }
            off += 1 + 2 + 2 + 1 + 1 + 1 + 1 + 1;

            if (off < buf.Length)
            {
                byte numZones = buf[off++];
                int zones = Math.Min(numZones, (byte)21);
                int bytesZones = zones * 5;
                if (off + bytesZones > buf.Length) return;
                off += bytesZones;
            }

            if (off < buf.Length)
            {
                byte sc = buf[off++];

                if (sc <= 3) state.CurrentSC = sc;
                if (state.LastSCFromSession is null) state.LastSCFromSession = sc;
                else if (state.LastSCFromSession != sc)
                {
                    var prev = state.LastSCFromSession.Value;

                    if (prev == 1 && sc != 1) 
                        ScWindowService.EndWindow(state, hdr.SessionUID, "SC", hdr.SessionTime, "[Session]");
                    if (prev != 1 && sc == 1) 
                        ScWindowService.StartWindow(state, hdr.SessionUID, "SC", hdr.SessionTime, "[Session]");

                    if (prev == 2 && sc != 2) 
                        ScWindowService.EndWindow(state, hdr.SessionUID, "VSC", hdr.SessionTime, "[Session]");
                    if (prev != 2 && sc == 2) 
                        ScWindowService.StartWindow(state, hdr.SessionUID, "VSC", hdr.SessionTime, "[Session]");

                    state.LastSCFromSession = sc;
                }
            }

            state.OnPotentialNewSession(hdr.SessionUID);
        }
    }
}