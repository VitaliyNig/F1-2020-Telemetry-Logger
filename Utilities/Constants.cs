using System.Collections.Generic;

namespace F12020TelemetryLogger.Utilities
{
    internal static class Constants
    {
        // Protocol
        public const int HEADER_SIZE = 24;
        public const int MAX_CARS = 22;
        public const int UDP_PORT_DEFAULT = 20777;
        public const ushort EXPECTED_PACKET_FORMAT = 2020;

        // Packet strides
        public const int LAPDATA_STRIDE = 53;
        public const int PARTICIPANT_STRIDE = 54;
        public const int CARSTATUS_STRIDE = 60;
        public const int CARTELEM_STRIDE = 58;

        // LapData offsets
        public const int LAPDATA_LASTLAP_OFFSET = 0;
        public const int LAPDATA_CURRENTLAP_OFFSET = 4;
        public const int LAPDATA_SECTOR1_OFFSET = 8;
        public const int LAPDATA_SECTOR2_OFFSET = 10;
        public const int LAPDATA_CURRENTLAPNUM_OFFSET = 45;
        public const int LAPDATA_PITSTATUS_OFFSET = 46;
        public const int LAPDATA_SECTOR_OFFSET = 47;
        public const int LAPDATA_CURRENTLAPINVALID_OFFSET = 48;
        public const int LAPDATA_PENALTIES_OFFSET = 49;

        // CarStatus offsets
        public const int CARSTATUS_WEAR_RL_OFFSET = 25;
        public const int CARSTATUS_WEAR_RR_OFFSET = 26;
        public const int CARSTATUS_WEAR_FL_OFFSET = 27;
        public const int CARSTATUS_WEAR_FR_OFFSET = 28;
        public const int CARSTATUS_ACTUAL_COMPOUND_OFFSET = 29;
        public const int CARSTATUS_VISUAL_COMPOUND_OFFSET = 30;
        public const int CARSTATUS_TYRE_AGE_OFFSET = 31;
        public const int CARSTATUS_FUEL_MIX_OFFSET = 32;

        // Penalty types
        public const byte PENALTY_THIS_LAP_INVALID = 10;
        public const byte PENALTY_THIS_NEXT_LAP_INVALID = 11;
        public const byte PENALTY_THIS_LAP_INVALID_NO_REASON = 12;
        public const byte PENALTY_THIS_NEXT_LAP_INVALID_NO_REASON = 13;
        public const byte PENALTY_THIS_PREV_LAP_INVALID = 14;
        public const byte PENALTY_THIS_PREV_LAP_INVALID_NO_REASON = 15;

        // Infringement types
        public const byte INFRINGEMENT_CORNER_CUTTING_LAP_INVALID = 25;
        public const byte INFRINGEMENT_LAP_INVALID_RESET = 32;

        // Validation
        public const float MIN_VALID_LAP_TIME_SEC = 1.0f;
        public const double WEAR_JUMP_THRESHOLD = -5.0;
        public const double LAP_TIME_EPSILON = 1e-3;
        public const int MAX_REASONABLE_LAP_MS = 300000; // 5 minutes

        // Track names
        public static readonly Dictionary<byte, string> TrackNames = new()
        {
            { 0,  "Melbourne" },
            { 1,  "PaulRicard" },
            { 2,  "Shanghai" },
            { 3,  "Sakhir" },
            { 4,  "Catalunya" },
            { 5,  "Monaco" },
            { 6,  "Montreal" },
            { 7,  "Silverstone" },
            { 8,  "Hockenheim" },
            { 9,  "Hungaroring" },
            { 10, "Spa" },
            { 11, "Monza" },
            { 12, "Singapore" },
            { 13, "Suzuka" },
            { 14, "AbuDhabi" },
            { 15, "Texas" },
            { 16, "Brazil" },
            { 17, "Austria" },
            { 18, "Sochi" },
            { 19, "Mexico" },
            { 20, "Baku" },
            { 21, "SakhirShort" },
            { 22, "SilverstoneShort" },
            { 23, "TexasShort" },
            { 24, "SuzukaShort" },
            { 25, "Hanoi" },
            { 26, "Zandvoort" }
        };
    }
}