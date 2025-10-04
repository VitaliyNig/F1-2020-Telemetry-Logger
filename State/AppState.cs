using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using F12020TelemetryLogger.Models;

namespace F12020TelemetryLogger.State
{
    public sealed class AppState
    {
        // Fuel Mix
        public Dictionary<int, byte> LastFuelMix { get; } = new();
        public Dictionary<int, List<(byte mix, float timestamp)>> FuelMixHistory { get; } = new();
        
        // Sectors
        public Dictionary<int, SectorBuffer> BufferedSectors { get; } = new();
        public sealed record SectorBuffer(int S1, int S2, int LapNum, ulong SessionUID);
        
        public Dictionary<int, int> LiveS1 { get; } = new();
        public Dictionary<int, int> LiveS2 { get; } = new();
        public Dictionary<int, int> LiveLapNo { get; } = new();
        public Dictionary<int, int> LiveSectorIdx { get; } = new();
        
        // Invalid laps
        public HashSet<(ulong uid, int car, int lap)> InvalidLaps { get; } = new();
        public HashSet<(ulong uid, int car, int lap)> LiveInvalidLaps { get; } = new();
        
        // Anchor info
        public bool AnchorInitialized { get; set; }
        public ulong AnchorSessionUID { get; set; }
        public string AnchorLocalDate { get; set; } = DateTime.Now.ToString("yyyy-MM-dd");
        public string AnchorTrackName { get; set; } = "UnknownTrack";
        public DateTime LastFinalSaveUtc { get; set; } = DateTime.MinValue;
        public byte? AnchorTrackId { get; set; }
        
        // Session info
        public string SessionType { get; set; } = "Unknown";
        public byte CurrentSC { get; set; }
        public byte? LastSCFromSession { get; set; }
        public byte? TrackId { get; set; }
        
        // Data buffers
        public ConcurrentDictionary<ulong, ConcurrentQueue<ParsedRow>> SessionBuffers { get; } = new();
        public ConcurrentDictionary<ulong, List<ScWindow>> ScWindows { get; } = new();
        
        // Driver info
        public Dictionary<int, string> DriverDisplay { get; } = new();
        public Dictionary<int, string> DriverRawName { get; } = new();
        public Dictionary<int, byte> TeamByCar { get; } = new();
        
        // Lap tracking
        public Dictionary<int, int> LastLapNum { get; } = new();
        public Dictionary<int, float> LastLapTimeSeen { get; } = new();
        public Dictionary<int, int> WrittenLapCount { get; } = new();
        public Dictionary<int, WearInfo> LastWear { get; } = new();
        public Dictionary<int, string> CurrentTyre { get; } = new();
        public Dictionary<int, int> LastTyreAge { get; } = new();
        public Dictionary<int, double> LapStartSec { get; } = new();
        
        // Session metadata
        public ConcurrentDictionary<ulong, string> SessionTypeByUid { get; } = new();
        
        // Telemetry
        public Dictionary<int, int> MaxSpeedQuali { get; } = new();
        public Dictionary<int, int> MaxSpeedRace { get; } = new();
        
        // Sectors history
        public ConcurrentDictionary<ulong, ConcurrentDictionary<int, Dictionary<int, SectorRec>>> LapSectors { get; } = new();
        public sealed record SectorRec(int S1, int S2, int S3, bool V1, bool V2, bool V3, bool LapValid);
        
        // Final order
        public Dictionary<(ulong uid, string sessionType), List<int>> FinalOrder { get; } = new();
        
        // Incidents
        public ConcurrentDictionary<ulong, List<IncidentRow>> Incidents { get; } = new();
        
        // Aggregation
        public sealed class DriverAgg
        {
            public int Laps;
            public long TotalMs;
            public int BestMs = int.MaxValue;
        }
        public ConcurrentDictionary<ulong, ConcurrentDictionary<int, DriverAgg>> Agg { get; } = new();
        
        // Debug
        public bool DebugPackets { get; set; }
        public readonly int[] PidCounters = new int[64];
        
        // Settings
        public bool AutoSaveEnabled { get; set; } = true;
        
        private ulong? _lastUid;
        
        public void OnPotentialNewSession(ulong uid)
        {
            if (_lastUid != uid)
            {
                _lastUid = uid;
                LastLapNum.Clear();
                LastLapTimeSeen.Clear();
                WrittenLapCount.Clear();
                LapStartSec.Clear();
                LastTyreAge.Clear();
                CurrentSC = 0;
                LiveS1.Clear();
                LiveS2.Clear();
                LiveLapNo.Clear();
                LiveSectorIdx.Clear();
                LiveInvalidLaps.Clear();
                BufferedSectors.Clear();
                FuelMixHistory.Clear();
            }
        }
    }
}