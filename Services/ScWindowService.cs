using System;
using F12020TelemetryLogger.Models;
using F12020TelemetryLogger.State;

namespace F12020TelemetryLogger.Services
{
    internal static class ScWindowService
    {
        public static string GetScAt(AppState state, ulong uid, double tSec)
        {
            if (!state.ScWindows.TryGetValue(uid, out var list)) return "";
            
            for (int i = list.Count - 1; i >= 0; i--)
            {
                var w = list[i];
                var end = w.EndSec ?? double.MaxValue;
                if (tSec >= w.StartSec && tSec <= end) return w.Kind;
            }
            return "";
        }

        public static string GetScOverlap(AppState state, ulong uid, double startSec, double endSec)
        {
            const double EPS = 0.001;
            if (!state.ScWindows.TryGetValue(uid, out var list) || list.Count == 0) return "";
            
            bool hasSC = false, hasVSC = false;
            foreach (var w in list)
            {
                var wEnd = w.EndSec ?? double.MaxValue;
                if ((startSec - EPS) <= (wEnd + EPS) && (endSec + EPS) >= (w.StartSec - EPS))
                {
                    if (w.Kind == "SC") hasSC = true;
                    if (w.Kind == "VSC") hasVSC = true;
                }
            }
            if (hasSC) return "SC";
            if (hasVSC) return "VSC";
            return "";
        }

        public static void StartWindow(AppState state, ulong uid, string kind, double startSec, string srcTag)
        {
            var list = state.ScWindows.GetOrAdd(uid, _ => new());

            if (list.Count > 0)
            {
                var last = list[^1];
                if (last.Kind == kind && last.EndSec is null)
                {
                    if (state.DebugPackets)
                        Console.WriteLine($"[DBG] {srcTag} Ignoring duplicate {kind} start (already open since {last.StartSec:F3}s)");
                    return;
                }
            }

            list.Add(new ScWindow(kind, startSec, null));
            if (state.DebugPackets)
                Console.WriteLine($"[DBG] {srcTag} Started {kind} window at {startSec:F3}s");
        }

        public static void EndWindow(AppState state, ulong uid, string kind, double endSec, string srcTag)
        {
            if (!state.ScWindows.TryGetValue(uid, out var list)) return;

            for (int i = list.Count - 1; i >= 0; i--)
            {
                if (list[i].Kind == kind && list[i].EndSec is null)
                {
                    double duration = endSec - list[i].StartSec;
                    list[i] = list[i] with { EndSec = endSec };

                    if (state.DebugPackets)
                        Console.WriteLine($"[DBG] {srcTag} Ended {kind} window at {endSec:F3}s (duration: {duration:F1}s)");
                    return;
                }
            }

            if (state.DebugPackets)
                Console.WriteLine($"[DBG] {srcTag} Warning: Tried to end {kind} but no open window found");
        }

        public static void CloseOpenWindows(AppState state)
        {
            foreach (var kv in state.ScWindows)
            {
                var list = kv.Value;
                for (int i = 0; i < list.Count; i++)
                {
                    if (list[i].EndSec is null)
                        list[i] = list[i] with { EndSec = double.MaxValue };
                }
            }
        }
    }
}