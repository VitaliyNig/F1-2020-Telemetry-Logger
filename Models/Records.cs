using System;
using System.Buffers.Binary;

namespace F12020TelemetryLogger.Models
{
    public sealed record PacketHeader(
        ushort PacketFormat, 
        byte GameMajor, 
        byte GameMinor, 
        byte PacketVersion, 
        byte PacketId,
        ulong SessionUID, 
        float SessionTime, 
        uint FrameIdentifier, 
        byte PlayerCarIndex, 
        byte SecondaryPlayerCarIndex)
    {
        public static PacketHeader Parse(ReadOnlySpan<byte> buf)
        {
            int o = 0;
            ushort pf = BinaryPrimitives.ReadUInt16LittleEndian(buf[o..]); o += 2;
            byte gmj = buf[o++], gmn = buf[o++], pv = buf[o++], pid = buf[o++];
            ulong uid = BinaryPrimitives.ReadUInt64LittleEndian(buf[o..]); o += 8;
            float st = BitConverter.ToSingle(buf[o..(o + 4)]); o += 4;
            uint frame = BinaryPrimitives.ReadUInt32LittleEndian(buf[o..]); o += 4;
            byte pci = buf[o++], spci = buf[o++];
            return new PacketHeader(pf, gmj, gmn, pv, pid, uid, st, frame, pci, spci);
        }
    }

    public record ParsedRow(
        ulong SessionUID,
        string SessionType,
        int CarIdx,
        string DriverColumn,
        string DriverRaw,
        int Lap,
        string Tyre,
        int LapTimeMs,
        double? WearAvg,
        int? RL, int? RR, int? FL, int? FR,
        double SessTimeSec,
        string? ScStatus,
        int TyreAge,
        int S1Ms,
        int S2Ms,
        int S3Ms,
        byte? FuelMix,
        string? FuelMixHistory
    );

    public sealed record IncidentRow(
        int Lap,
        int CarIdx,
        string Accident,
        string Penalty,
        int Seconds,
        int PlacesGained,
        int OtherCar,
        double SessTimeSec
    );

    public sealed record ScWindow(string Kind, double StartSec, double? EndSec);

    public sealed record WearInfo(int RL, int RR, int FL, int FR)
    {
        public double Avg => Math.Round((RL + RR + FL + FR) / 4.0, 1);
    }
}