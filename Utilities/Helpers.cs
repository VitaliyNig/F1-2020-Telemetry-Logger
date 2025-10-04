using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace F12020TelemetryLogger.Utilities
{
    internal static class Helpers
    {
        public static string SessionTypeName(byte s) => s switch
        {
            0 => "Unknown",
            1 => "P1",
            2 => "P2",
            3 => "P3",
            4 => "ShortP",
            5 => "Q1",
            6 => "Q2",
            7 => "Q3",
            8 => "ShortQ",
            9 => "OSQ",
            10 => "Race",
            11 => "R2",
            12 => "TT",
            _ => "Unknown"
        };

        public static string PacketName(byte id) => id switch
        {
            0 => "Motion",
            1 => "Session",
            2 => "LapData",
            3 => "Event",
            4 => "Participants",
            5 => "CarSetups",
            6 => "CarTelemetry",
            7 => "CarStatus",
            8 => "FinalClassification",
            9 => "LobbyInfo",
            10 => "CarDamage",
            11 => "SessionHistory",
            _ => $"Packet({id})"
        };

        public static string TrackName(byte trackId) => 
            Constants.TrackNames.TryGetValue(trackId, out var n) ? n : $"Track_{trackId}";

        public static string DecodeName(ReadOnlySpan<byte> nameBytes)
        {
            int len = nameBytes.IndexOf((byte)0);
            if (len < 0) len = nameBytes.Length;
            if (len <= 0) return "Unknown";

            string s = Encoding.UTF8.GetString(nameBytes[..len]);

            var sb = new StringBuilder(s.Length);
            foreach (var ch in s) 
                if (!char.IsControl(ch)) 
                    sb.Append(ch);
            s = sb.ToString().Trim();

            if (string.IsNullOrWhiteSpace(s))
            {
                s = Encoding.GetEncoding("ISO-8859-1").GetString(nameBytes[..len]).Trim();
                if (string.IsNullOrWhiteSpace(s)) s = "Unknown";
            }
            if (s.Length > 64) s = s[..64];
            return s;
        }

        public static bool IsPct(int v) => v >= 0 && v <= 100;

        public static string TyreName(byte actual, byte visual)
        {
            return visual switch
            {
                16 => "Soft",
                17 => "Medium",
                18 => "Hard",
                7 => "Inter",
                8 => "Wet",
                _ => actual switch
                {
                    16 => "C5",
                    17 => "C4",
                    18 => "C3",
                    19 => "C2",
                    20 => "C1",
                    7 => "Inter",
                    8 => "Wet",
                    _ => "Unknown"
                }
            };
        }

        public static string Csv(string s)
        {
            if (string.IsNullOrEmpty(s)) return "";
            if (s.Contains(',') || s.Contains('"')) 
                return "\"" + s.Replace("\"", "\"\"") + "\"";
            return s;
        }

        public static string MsToStr(int ms)
        {
            if (ms <= 0) return "";
            double s = ms / 1000.0;
            int m = (int)(s / 60);
            s -= m * 60;
            return $"{m}:{s:00.000}";
        }

        public static int TyreOrder(string tyre) => tyre switch
        {
            "Soft" => 0,
            "Medium" => 1,
            "Hard" => 2,
            "Inter" => 3,
            "Wet" => 4,
            _ => 99
        };

        public static string[] SplitCsv(string line)
        {
            var res = new List<string>();
            var sb = new StringBuilder();
            bool quoted = false;
            for (int i = 0; i < line.Length; i++)
            {
                char ch = line[i];
                if (quoted)
                {
                    if (ch == '"')
                    {
                        if (i + 1 < line.Length && line[i + 1] == '"') 
                        { 
                            sb.Append('"'); 
                            i++; 
                        }
                        else 
                        { 
                            quoted = false; 
                        }
                    }
                    else sb.Append(ch);
                }
                else
                {
                    if (ch == ',') 
                    { 
                        res.Add(sb.ToString()); 
                        sb.Clear(); 
                    }
                    else if (ch == '"') 
                    { 
                        quoted = true; 
                    }
                    else sb.Append(ch);
                }
            }
            res.Add(sb.ToString());
            return res.ToArray();
        }

        public static string SafeName(string s)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var sb = new StringBuilder(s.Length);
            foreach (var ch in s)
                sb.Append(invalid.Contains(ch) ? '_' : ch);
            return sb.ToString();
        }

        public static int SessionOrderKey(string s) => s.ToUpperInvariant() switch
        {
            "P1" => 10,
            "P2" => 11,
            "P3" => 12,
            "SHORTP" => 13,
            "Q1" => 20,
            "Q2" => 21,
            "Q3" => 22,
            "SHORTQ" => 23,
            "OSQ" => 24,
            "RACE" => 30,
            "R2" => 31,
            "R" => 32,
            _ => 99
        };

        public static string GetFuelMixName(byte mix) => mix switch
        {
            0 => "Lean",
            1 => "Std",
            2 => "Rich",
            3 => "Max",
            _ => ""
        };

        public static string PenaltyName(byte id) => id switch
        {
            0 => "Drive through",
            1 => "Stop Go",
            2 => "Grid penalty",
            3 => "Penalty reminder",
            4 => "Time penalty",
            5 => "Warning",
            6 => "Disqualified",
            7 => "Removed from formation lap",
            8 => "Parked too long timer",
            9 => "Tyre regulations",
            10 => "This lap invalidated",
            11 => "This and next lap invalidated",
            12 => "This lap invalidated without reason",
            13 => "This and next lap invalidated without reason",
            14 => "This and previous lap invalidated",
            15 => "This and previous lap invalidated without reason",
            16 => "Retired",
            17 => "Black flag timer",
            _ => $"Unknown({id})"
        };

        public static string InfringementName(byte id) => id switch
        {
            0 => "Blocking by slow driving",
            1 => "Blocking by wrong way driving",
            2 => "Reversing off the start line",
            3 => "Big Collision",
            4 => "Small Collision",
            5 => "Collision failed to hand back position single",
            6 => "Collision failed to hand back position multiple",
            7 => "Corner cutting gained time",
            8 => "Corner cutting overtake single",
            9 => "Corner cutting overtake multiple",
            10 => "Crossed pit exit lane",
            11 => "Ignoring blue flags",
            12 => "Ignoring yellow flags",
            13 => "Ignoring drive through",
            14 => "Too many drive throughs",
            15 => "Drive through reminder serve within n laps",
            16 => "Drive through reminder serve this lap",
            17 => "Pit lane speeding",
            18 => "Parked for too long",
            19 => "Ignoring tyre regulations",
            20 => "Too many penalties",
            21 => "Multiple warnings",
            22 => "Approaching disqualification",
            23 => "Tyre regulations select single",
            24 => "Tyre regulations select multiple",
            25 => "Lap invalidated corner cutting",
            26 => "Lap invalidated running wide",
            27 => "Corner cutting ran wide gained time minor",
            28 => "Corner cutting ran wide gained time significant",
            29 => "Corner cutting ran wide gained time extreme",
            30 => "Lap invalidated wall riding",
            31 => "Lap invalidated flashback used",
            32 => "Lap invalidated reset to track",
            33 => "Blocking the pitlane",
            34 => "Jump start",
            35 => "Safety car to car collision",
            36 => "Safety car illegal overtake",
            37 => "Safety car exceeding allowed pace",
            38 => "Virtual safety car exceeding allowed pace",
            39 => "Formation lap below allowed speed",
            40 => "Retired mechanical failure",
            41 => "Retired terminally damaged",
            42 => "Safety car falling too far back",
            43 => "Black flag timer",
            44 => "Unserved stop go penalty",
            45 => "Unserved drive through penalty",
            46 => "Engine component change",
            47 => "Gearbox change",
            48 => "League grid penalty",
            49 => "Retry penalty",
            50 => "Illegal time gain",
            51 => "Mandatory pitstop",
            _ => $"Unknown({id})"
        };

        public static int? TryExtractCarIndexFromDisplay(string display)
        {
            int lb = display.LastIndexOf('[');
            int rb = display.LastIndexOf(']');
            if (lb >= 0 && rb > lb)
            {
                var inner = display.Substring(lb + 1, rb - lb - 1);
                if (int.TryParse(inner, out int idx)) return idx;
            }
            return null;
        }
    }
}