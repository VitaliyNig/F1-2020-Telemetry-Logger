using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClosedXML.Excel;

namespace F12020TelemetryLogger
{
    internal static class Program
    {
        // ======= Console Helpers ========
        private static class Log
        {
            public static void Info(string message)
            {
                Console.WriteLine(message);
            }

            public static void Success(string message)
            {
                var prev = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(message);
                Console.ForegroundColor = prev;
            }

            public static void Warn(string message)
            {
                var prev = Console.BackgroundColor;
                Console.BackgroundColor = ConsoleColor.Red;
                Console.WriteLine(message);
                Console.BackgroundColor = prev;
            }
        }

        // ========= Protocol / Runtime =========
        private const int HEADER_SIZE = 24;
        private const int MAX_CARS = 22;
        private const int UDP_PORT_DEFAULT = 20777;

        private const int LAPDATA_STRIDE = 53;
        private const int PARTICIPANT_STRIDE = 54;
        private const int CARSTATUS_STRIDE = 60;
        private const int CARTELEM_STRIDE = 58;

        // ======= NEW: LapData offsets =======
        private const int LAPDATA_LASTLAP_OFFSET = 0;
        private const int LAPDATA_CURRENTLAP_OFFSET = 4;
        private const int LAPDATA_SECTOR1_OFFSET = 8;
        private const int LAPDATA_SECTOR2_OFFSET = 10;
        private const int LAPDATA_CURRENTLAPNUM_OFFSET = 45;
        private const int LAPDATA_PITSTATUS_OFFSET = 46;
        private const int LAPDATA_SECTOR_OFFSET = 47;
        private const int LAPDATA_CURRENTLAPINVALID_OFFSET = 48;
        private const int LAPDATA_PENALTIES_OFFSET = 49;

        // ======= NEW: CarStatus offsets =======
        private const int CARSTATUS_WEAR_RL_OFFSET = 25;
        private const int CARSTATUS_WEAR_RR_OFFSET = 26;
        private const int CARSTATUS_WEAR_FL_OFFSET = 27;
        private const int CARSTATUS_WEAR_FR_OFFSET = 28;
        private const int CARSTATUS_ACTUAL_COMPOUND_OFFSET = 29;
        private const int CARSTATUS_VISUAL_COMPOUND_OFFSET = 30;
        private const int CARSTATUS_TYRE_AGE_OFFSET = 31;
        private const int CARSTATUS_FUEL_MIX_OFFSET = 32;

        // ======= NEW: Penalty Types =======
        private const byte PENALTY_THIS_LAP_INVALID = 10;
        private const byte PENALTY_THIS_NEXT_LAP_INVALID = 11;
        private const byte PENALTY_THIS_LAP_INVALID_NO_REASON = 12;
        private const byte PENALTY_THIS_NEXT_LAP_INVALID_NO_REASON = 13;
        private const byte PENALTY_THIS_PREV_LAP_INVALID = 14;
        private const byte PENALTY_THIS_PREV_LAP_INVALID_NO_REASON = 15;

        // ======= NEW: Infringement Types =======
        private const byte INFRINGEMENT_CORNER_CUTTING_LAP_INVALID = 25;
        private const byte INFRINGEMENT_LAP_INVALID_RESET = 32;

        // ======= NEW: Constants =======
        private const ushort EXPECTED_PACKET_FORMAT = 2020;
        private const float MIN_VALID_LAP_TIME_SEC = 1.0f;
        private const double WEAR_JUMP_THRESHOLD = -5.0;
        private const double LAP_TIME_EPSILON = 1e-3;
        private const int MAX_REASONABLE_LAP_MS = 300000; // 5 minutes

        private static readonly TimeSpan AutoSaveEvery = TimeSpan.FromSeconds(10);

        private static readonly State ST = new();
        private static volatile bool _running = true;
        private static volatile bool _saving = false;
        private static DateTime _lastAutoSaveUtc = DateTime.UtcNow;

        private static UdpClient? _udp;
        private static CancellationTokenSource? _cts;

        private static readonly object SaveLock = new();

        public static async Task<int> Main(string[]? args = null)
        {
            Console.OutputEncoding = Encoding.UTF8;
            try { Console.InputEncoding = Encoding.UTF8; } catch { }

            int port = UDP_PORT_DEFAULT;
            if (args is { Length: > 0 } && int.TryParse(args[0], out var p) && p is > 0 and < 65536)
                port = p;

            PrintBanner();
            PrintMenu();

            try
            {
                _udp = new UdpClient(AddressFamily.InterNetwork);
                _udp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                _udp.Client.Bind(new IPEndPoint(IPAddress.Any, port));
                _udp.Client.ReceiveTimeout = 50;
                _udp.Client.ReceiveBufferSize = 1 << 20;
                Log.Info($"[i] UDP Listening Enabled: {port} (ReuseAddress=ON)");
            }
            catch (SocketException se) when (se.SocketErrorCode == SocketError.AddressAlreadyInUse || se.SocketErrorCode == SocketError.AccessDenied)
            {
                Log.Warn($"[warning] Порт {port} недоступен: {se.Message}");

                var owners = FindUdpOwners(port);
                if (owners.Count > 0)
                {
                    Log.Warn("[warning] Порт занят процессом: ");
                    foreach (var (pid, name) in owners)
                    {
                        Console.WriteLine($"  • {name} (PID {pid})");
                    }
                }
                else
                {
                    Log.Warn("[warning] Не удалось определить процесс, занявший порт");
                }

                Console.WriteLine($"\n[i] Закройте приложение выше или запустите логгер на другом порту, например:");
                Console.WriteLine($"    dotnet run -- {port + 1}");
                Console.WriteLine($"    telemetry.exe {port + 1}");
                return 2;
            }

            _cts = new CancellationTokenSource();
            var menuTask = Task.Run(MenuLoop, _cts.Token);

            try
            {
                while (_running)
                {
                    try
                    {
                        var res = await _udp.ReceiveAsync();
                        var buf = res.Buffer;
                        if (buf.Length < HEADER_SIZE) continue;

                        var hdr = PacketHeader.Parse(buf);
                        if (hdr.PacketId < ST.PidCounters.Length) ST.PidCounters[hdr.PacketId]++;

                        AnchorsInitialization(hdr);

                        switch (hdr.PacketId)
                        {
                            case 1: ParseSession(buf, hdr); break;
                            case 2: if (ST.DebugPackets) Console.WriteLine("[DBG] Packet ID=2 (LapData) received"); ParseLapData(buf, hdr); break;
                            case 3: ParseEvent(buf, hdr); break;
                            case 4: ParseParticipants(buf, hdr); break;
                            case 6: ParseCarTelemetry(buf, hdr); break;
                            case 7: ParseCarStatus(buf, hdr); break;
                            default: break;
                        }

                        if (ST.AutoSaveEnabled && DateTime.UtcNow - _lastAutoSaveUtc >= AutoSaveEvery && !_saving)
                        {
                            await SaveAllAsync(makeExcel: false);
                            _lastAutoSaveUtc = DateTime.UtcNow;
                            Console.WriteLine("[i] Auto-Saved (CSV)");
                        }
                    }
                    catch (SocketException) { }
                    catch (ObjectDisposedException) { break; }
                    catch (Exception ex)
                    {
                        Log.Warn("[warning] " + ex.Message);
                    }
                }
            }
            finally
            {
                await FinalizeAndExitAsync();
            }

            return 0;
        }

        // ========== Menu / UI ==========

        private static async Task MenuLoop()
        {
            while (_running)
            {
                if (!Console.KeyAvailable)
                {
                    await Task.Delay(50);
                    continue;
                }
                var key = Console.ReadKey(true).Key;
                switch (key)
                {
                    case ConsoleKey.M:
                        PrintMenu();
                        break;

                    case ConsoleKey.D1:
                    case ConsoleKey.NumPad1:
                        await SaveAllAsync(makeExcel: true);
                        Log.Success("[✓] Manual Saved");
                        break;

                    case ConsoleKey.D2:
                    case ConsoleKey.NumPad2:
                        ST.AutoSaveEnabled = !ST.AutoSaveEnabled;
                        Console.WriteLine(ST.AutoSaveEnabled ? "[i] Auto-Save: ON" : "[i] Auto-Save: OFF");
                        break;

                    case ConsoleKey.D3:
                    case ConsoleKey.NumPad3:
                        TryOpenFolder();
                        break;

                    case ConsoleKey.D4:
                    case ConsoleKey.NumPad4:
                        PrintBuffer();
                        break;

                    case ConsoleKey.D5:
                    case ConsoleKey.NumPad5:
                        var fakeHdr = new PacketHeader(0, 0, 0, 0, 0, ST.AnchorSessionUID + 1, 0f, 0, 0, 0);
                        ReAnchorForNewWeekend(fakeHdr, ST.TrackId);
                        Log.Success("[✓] Manual Started New Weekend");
                        break;

                    case ConsoleKey.D6:
                    case ConsoleKey.NumPad6:
                        ST.DebugPackets = !ST.DebugPackets;
                        Console.WriteLine(ST.DebugPackets ? "[i] Debug Mode: ON" : "[i] Debug Mode: OFF");
                        for (int i = 0; i < ST.PidCounters.Length; i++)
                            if (ST.PidCounters[i] > 0)
                                Console.WriteLine($"  • PID {i}: {ST.PidCounters[i]}");
                        break;

                    case ConsoleKey.D0:
                    case ConsoleKey.NumPad0:
                        TriggerExit();
                        return;
                }
            }
        }

        private static void PrintBanner()
        {
            Console.BackgroundColor = ConsoleColor.DarkMagenta;
            Console.WriteLine("    Telemetry Logger | F1 2020 | v7.1    ");
            Console.ResetColor();
        }

        private static void PrintMenu()
        {
            Console.WriteLine("\n    [1] Manual Save (CSV + Excel)\n    [2] Auto-Save (ON/OFF)\n    [3] Open Folder\n    [4] View Buffer\n    [5] Manual Start New Weekend\n    [6] Debug Mode (ON/OFF)\n    [M] Show Menu Again\n    [0] Exit Program\n");
        }

        private static void PrintBuffer()
        {
            int total = ST.SessionBuffers.Sum(kv => kv.Value.Count);
            Console.WriteLine($"[i] Buffered Rows: {total}");

            foreach (var kv in ST.SessionBuffers.OrderBy(k => k.Key))
            {
                var uid = kv.Key;
                var snapshot = kv.Value.ToArray();
                var byDrv = snapshot.GroupBy(r => r.DriverColumn, StringComparer.OrdinalIgnoreCase)
                                    .OrderBy(g => g.Key, StringComparer.OrdinalIgnoreCase);

                Console.WriteLine($"    Session {uid}: Rows={snapshot.Length}, Drivers={byDrv.Count()}");
                foreach (var g in byDrv)
                {
                    int laps = g.Select(x => x.Lap).Distinct().Count();
                    var avg = g.Average(x => x.LapTimeMs);
                    Console.WriteLine($"    {g.Key}  Laps={laps,-3} Avg={MsToStr((int)Math.Round(avg))}");
                }
            }
        }

        private static void TryOpenFolder()
        {
            try
            {
                var folder = GetOutputDir();
                var psi = new System.Diagnostics.ProcessStartInfo { FileName = folder, UseShellExecute = true };
                System.Diagnostics.Process.Start(psi);
                Log.Success("[✓] Opening Folder…");
            }
            catch (Exception ex)
            {
                Log.Warn("[warning] Error Opening Folder: " + ex.Message);
            }
        }

        private static void TriggerExit()
        {
            if (!_running) return;
            _running = false;
            Log.Success("[✓] Exiting…");
            _ = Task.Run(async () => await FinalizeAndExitAsync());
        }

        private static async Task FinalizeAndExitAsync()
        {
            try
            {
                CloseOpenScWindows();
                await SaveAllAsync(makeExcel: true);
                Log.Success("[✓] Final Save Complete");
            }
            catch (Exception ex)
            {
                Log.Warn("[warning] Final Save Failed: " + ex.Message);
            }

            try { _udp?.Close(); _udp?.Dispose(); } catch { }
            try { _cts?.Cancel(); _cts?.Dispose(); } catch { }

            Log.Info("[i] Bye");
            Environment.Exit(0);
        }

        // ========== Packet Parsing ==========

        private static void AnchorsInitialization(PacketHeader hdr)
        {
            if (ST.AnchorInitialized) return;

            // NEW: Check protocol version
            if (hdr.PacketFormat != EXPECTED_PACKET_FORMAT)
            {
                Log.Warn($"[warning] Unexpected packet format: {hdr.PacketFormat}, expected {EXPECTED_PACKET_FORMAT}");
            }

            ST.AnchorInitialized = true;
            ST.AnchorSessionUID = hdr.SessionUID;
            ST.AnchorLocalDate = DateTime.Now.ToString("yyyy-MM-dd");
            if (ST.TrackId.HasValue) ST.AnchorTrackId = ST.TrackId;
            if (string.IsNullOrEmpty(ST.AnchorTrackName)) ST.AnchorTrackName = "UnknownTrack";
            Console.WriteLine($"[i] Anchor: Date={ST.AnchorLocalDate}, SessionUID={ST.AnchorSessionUID}, Format={hdr.PacketFormat}");
        }

        private static void ParseCarTelemetry(ReadOnlySpan<byte> buf, PacketHeader hdr)
        {
            int off = HEADER_SIZE;
            if (off + CARTELEM_STRIDE * MAX_CARS > buf.Length)
            {
                if (ST.DebugPackets)
                    Log.Warn($"[warning] CarTelemetry packet too small: {buf.Length} bytes");
                return;
            }

            bool isQuali = IsQualiNow();
            bool isRace = IsRaceNow();

            if (!isQuali && !isRace) return;

            for (int car = 0; car < MAX_CARS; car++)
            {
                try
                {
                    int start = off + car * CARTELEM_STRIDE;
                    if (start + 2 > buf.Length) break;

                    ushort speed = BinaryPrimitives.ReadUInt16LittleEndian(buf.Slice(start, 2));
                    if (speed == 0) continue;

                    if (isQuali)
                    {
                        if (!ST.MaxSpeedQuali.TryGetValue(car, out int cur) || speed > cur)
                        {
                            ST.MaxSpeedQuali[car] = speed;
                            if (ST.DebugPackets)
                                Console.WriteLine($"[DBG] New max speed (quali) for car {car}: {speed} km/h");
                        }
                    }
                    else if (isRace)
                    {
                        if (!ST.MaxSpeedRace.TryGetValue(car, out int cur) || speed > cur)
                        {
                            ST.MaxSpeedRace[car] = speed;
                            if (ST.DebugPackets)
                                Console.WriteLine($"[DBG] New max speed (race) for car {car}: {speed} km/h");
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (ST.DebugPackets)
                        Log.Warn($"[warning] Error parsing telemetry for car {car}: {ex.Message}");
                }
            }
        }

        private static void ParseSession(ReadOnlySpan<byte> buf, PacketHeader hdr)
        {
            int off = HEADER_SIZE;

            try
            {
                int tmp = HEADER_SIZE;
                tmp += 1 + 1 + 1 + 1 + 2;
                if (tmp < buf.Length) tmp += 1;
                byte? trackIdNow = null;
                if (tmp < buf.Length)
                {
                    sbyte tSigned = unchecked((sbyte)buf[tmp]);
                    if (tSigned >= 0) trackIdNow = (byte)tSigned;
                }

                // FIXED: Improved track change detection
                bool trackChanged = trackIdNow.HasValue
                                    && ST.AnchorTrackId.HasValue
                                    && trackIdNow.Value != ST.AnchorTrackId.Value
                                    && trackIdNow.Value >= 0
                                    && trackIdNow.Value <= 26;

                if (trackChanged)
                {
                    Console.WriteLine($"[i] Track changed: {TrackName(ST.AnchorTrackId!.Value)} ({ST.AnchorTrackId!.Value}) -> {TrackName(trackIdNow!.Value)} ({trackIdNow!.Value})");
                    ReAnchorForNewWeekend(hdr, trackIdNow);
                }
            }
            catch (Exception ex)
            {
                if (ST.DebugPackets)
                    Log.Warn($"[warning] Error parsing track change: {ex.Message}");
            }

            off += 1 + 1 + 1 + 1 + 2;
            if (off < buf.Length)
            {
                byte stype = buf[off++];
                ST.SessionType = SessionTypeName(stype);
                ST.SessionTypeByUid[hdr.SessionUID] = ST.SessionType;
            }

            if (off < buf.Length)
            {
                sbyte tSigned = unchecked((sbyte)buf[off++]);
                if (tSigned >= 0)
                {
                    byte t = (byte)tSigned;
                    ST.TrackId = t;
                    if (!ST.AnchorTrackId.HasValue) ST.AnchorTrackId = t;
                    ST.AnchorTrackName = TrackName(t);
                }
                else
                {
                    ST.TrackId = null;
                    if (!ST.AnchorTrackId.HasValue) ST.AnchorTrackName = "UnknownTrack";
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

                if (sc <= 3) ST.CurrentSC = sc;
                if (ST.LastSCFromSession is null) ST.LastSCFromSession = sc;
                else if (ST.LastSCFromSession != sc)
                {
                    var prev = ST.LastSCFromSession.Value;

                    if (prev == 1 && sc != 1) EndScWindow(hdr.SessionUID, "SC", hdr.SessionTime, "[Session]");
                    if (prev != 1 && sc == 1) StartScWindow(hdr.SessionUID, "SC", hdr.SessionTime, "[Session]");

                    if (prev == 2 && sc != 2) EndScWindow(hdr.SessionUID, "VSC", hdr.SessionTime, "[Session]");
                    if (prev != 2 && sc == 2) StartScWindow(hdr.SessionUID, "VSC", hdr.SessionTime, "[Session]");

                    ST.LastSCFromSession = sc;
                }
            }

            ST.OnPotentialNewSession(hdr.SessionUID);
        }

        private static void ParseEvent(ReadOnlySpan<byte> buf, PacketHeader hdr)
        {
            if (buf.Length < HEADER_SIZE + 4)
            {
                if (ST.DebugPackets)
                    Log.Warn($"[warning] Event packet too small: {buf.Length} bytes");
                return;
            }

            string code = Encoding.UTF8.GetString(buf.Slice(HEADER_SIZE, 4));

            if (ST.DebugPackets)
                Console.WriteLine($"[DBG] Event received: {code}");

            switch (code)
            {
                case "SSTA":
                    Console.WriteLine("[EVT] Session Started");
                    break;

                case "SEND":
                    Console.WriteLine("[EVT] Session Ended - Triggering save...");
                    CloseOpenScWindows();
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await SaveAllAsync(makeExcel: true);
                            Console.WriteLine("[EVT] Session Ended — Data Saved");
                        }
                        catch (Exception ex)
                        {
                            Log.Warn("[warning] Save on SEND failed: " + ex.Message);
                        }
                    });
                    break;

                case "SCDP":
                    Console.WriteLine("[EVT] Safety Car Deployed");
                    ST.CurrentSC = 1;
                    StartScWindow(hdr.SessionUID, "SC", hdr.SessionTime, "[Event]");
                    break;

                case "SCDE":
                    Console.WriteLine("[EVT] Safety Car Ending");
                    ST.CurrentSC = 0;
                    EndScWindow(hdr.SessionUID, "SC", hdr.SessionTime, "[Event]");
                    break;

                case "VSCN":
                    Console.WriteLine("[EVT] Virtual Safety Car");
                    ST.CurrentSC = 2;
                    StartScWindow(hdr.SessionUID, "VSC", hdr.SessionTime, "[Event]");
                    break;

                case "VSCF":
                    Console.WriteLine("[EVT] Virtual Safety Car Ending");
                    ST.CurrentSC = 0;
                    EndScWindow(hdr.SessionUID, "VSC", hdr.SessionTime, "[Event]");
                    break;

                case "PENA":
                    ParsePenaltyEvent(buf, hdr);
                    break;

                case "FTLP":
                    if (ST.DebugPackets) Console.WriteLine("[EVT] Fastest Lap");
                    break;

                case "RTMT":
                    if (ST.DebugPackets) Console.WriteLine("[EVT] Retirement");
                    break;

                case "DRSE":
                    if (ST.DebugPackets) Console.WriteLine("[EVT] DRS Enabled");
                    break;

                case "DRSD":
                    if (ST.DebugPackets) Console.WriteLine("[EVT] DRS Disabled");
                    break;

                case "TMPT":
                    if (ST.DebugPackets) Console.WriteLine("[EVT] Team Mate in Pits");
                    break;

                case "CHQF":
                    if (ST.DebugPackets) Console.WriteLine("[EVT] Chequered Flag");
                    break;

                case "RCWN":
                    if (ST.DebugPackets) Console.WriteLine("[EVT] Race Winner");
                    break;

                default:
                    if (ST.DebugPackets) Console.WriteLine($"[EVT] Unknown event code: {code}");
                    break;
            }
        }

        private static void ParsePenaltyEvent(ReadOnlySpan<byte> buf, PacketHeader hdr)
        {
            int o = HEADER_SIZE + 4;
            if (o + 7 > buf.Length) return;

            byte penaltyType = buf[o + 0];
            byte infringementType = buf[o + 1];
            byte vehicleIdx = buf[o + 2];
            byte otherVehicleIdx = buf[o + 3];
            byte timeSec = buf[o + 4];
            byte lapNum = buf[o + 5];
            byte placesGained = buf[o + 6];

            string penName = PenaltyName(penaltyType);
            string infrName = InfringementName(infringementType);

            string penaltyText = penName;

            if (penaltyType == 1 || penaltyType == 4)
                penaltyText = $"{penName} ({timeSec}s)";
            else if (penaltyType == 2 && placesGained > 0)
                penaltyText = $"{penName} (+{placesGained})";

            // FIXED: Correct handling of all penalty types
            if (lapNum > 0)
            {
                switch (penaltyType)
                {
                    case PENALTY_THIS_LAP_INVALID:
                    case PENALTY_THIS_LAP_INVALID_NO_REASON:
                        ST.InvalidLaps.Add((hdr.SessionUID, vehicleIdx, lapNum));
                        if (ST.DebugPackets)
                            Console.WriteLine($"[DBG] Lap {lapNum} invalidated for car {vehicleIdx} (this lap only)");
                        break;

                    case PENALTY_THIS_NEXT_LAP_INVALID:
                    case PENALTY_THIS_NEXT_LAP_INVALID_NO_REASON:
                        ST.InvalidLaps.Add((hdr.SessionUID, vehicleIdx, lapNum));
                        ST.InvalidLaps.Add((hdr.SessionUID, vehicleIdx, lapNum + 1));
                        if (ST.DebugPackets)
                            Console.WriteLine($"[DBG] Laps {lapNum} and {lapNum + 1} invalidated for car {vehicleIdx}");
                        break;

                    case PENALTY_THIS_PREV_LAP_INVALID:
                    case PENALTY_THIS_PREV_LAP_INVALID_NO_REASON:
                        ST.InvalidLaps.Add((hdr.SessionUID, vehicleIdx, lapNum));
                        if (lapNum > 1)
                        {
                            ST.InvalidLaps.Add((hdr.SessionUID, vehicleIdx, lapNum - 1));
                            if (ST.DebugPackets)
                                Console.WriteLine($"[DBG] Laps {lapNum - 1} and {lapNum} invalidated for car {vehicleIdx}");
                        }
                        else
                        {
                            if (ST.DebugPackets)
                                Console.WriteLine($"[DBG] Lap {lapNum} invalidated for car {vehicleIdx} (previous lap N/A)");
                        }
                        break;
                }
            }

            // FIXED: Handle infringement corner cutting
            if (infringementType >= INFRINGEMENT_CORNER_CUTTING_LAP_INVALID &&
                infringementType <= INFRINGEMENT_LAP_INVALID_RESET)
            {
                if (lapNum > 0)
                {
                    ST.InvalidLaps.Add((hdr.SessionUID, vehicleIdx, lapNum));
                    if (ST.DebugPackets)
                        Console.WriteLine($"[DBG] Lap {lapNum} invalidated for car {vehicleIdx} (infringement: {infrName})");
                }
            }

            var inc = new IncidentRow(
                Lap: lapNum,
                CarIdx: vehicleIdx,
                Accident: infrName,
                Penalty: penaltyText,
                Seconds: timeSec,
                PlacesGained: placesGained,
                OtherCar: otherVehicleIdx,
                SessTimeSec: hdr.SessionTime
            );

            var list = ST.Incidents.GetOrAdd(hdr.SessionUID, _ => new List<IncidentRow>());
            list.Add(inc);
        }

        private static void ParseParticipants(ReadOnlySpan<byte> buf, PacketHeader hdr)
        {
            int off = HEADER_SIZE;
            if (buf.Length < off + 1)
            {
                if (ST.DebugPackets)
                    Log.Warn($"[warning] Participants packet too small: {buf.Length} bytes");
                return;
            }

            int numActive = buf[off]; off += 1;

            if (ST.DebugPackets)
                Console.WriteLine($"[DBG] Parsing participants: {numActive} active cars");

            for (int car = 0; car < Math.Min(numActive, MAX_CARS); car++)
            {
                try
                {
                    int start = off + car * PARTICIPANT_STRIDE;
                    if (start + PARTICIPANT_STRIDE > buf.Length)
                    {
                        if (ST.DebugPackets)
                            Log.Warn($"[warning] Participant data incomplete for car {car}");
                        break;
                    }

                    byte aiControlled = buf[start + 0];
                    byte teamId = buf[start + 2];

                    var nameBytes = buf.Slice(start + 5, 48);

                    string name = DecodeName(nameBytes);
                    if (string.IsNullOrWhiteSpace(name))
                    {
                        name = (aiControlled == 1) ? "AI" : "Player";
                    }
                    string display = $"{name} [{car}]";

                    ST.DriverDisplay[car] = display;
                    ST.DriverRawName[car] = name;
                    ST.TeamByCar[car] = teamId;

                    if (ST.DebugPackets)
                        Console.WriteLine($"[DBG] Car {car}: {display}, Team={teamId}, AI={aiControlled}");
                }
                catch (Exception ex)
                {
                    if (ST.DebugPackets)
                        Log.Warn($"[warning] Error parsing participant {car}: {ex.Message}");
                }
            }
        }

        private static void ParseCarStatus(ReadOnlySpan<byte> buf, PacketHeader hdr)
        {
            int off = HEADER_SIZE;
            if (off + CARSTATUS_STRIDE * MAX_CARS > buf.Length)
            {
                if (ST.DebugPackets)
                    Log.Warn($"[warning] CarStatus packet too small: {buf.Length} bytes");
                return;
            }

            for (int car = 0; car < MAX_CARS; car++)
            {
                try
                {
                    int start = off + car * CARSTATUS_STRIDE;
                    var block = buf.Slice(start, CARSTATUS_STRIDE);

                    // FIXED: Use named constants instead of magic numbers
                    int rl = block[CARSTATUS_WEAR_RL_OFFSET];
                    int rr = block[CARSTATUS_WEAR_RR_OFFSET];
                    int fl = block[CARSTATUS_WEAR_FL_OFFSET];
                    int fr = block[CARSTATUS_WEAR_FR_OFFSET];

                    if (IsPct(rl) && IsPct(rr) && IsPct(fl) && IsPct(fr))
                        ST.LastWear[car] = new WearInfo(rl, rr, fl, fr);

                    string tyre = TyreName(block[CARSTATUS_ACTUAL_COMPOUND_OFFSET],
                                           block[CARSTATUS_VISUAL_COMPOUND_OFFSET]);
                    ST.CurrentTyre[car] = tyre;

                    int age = block.Length > CARSTATUS_TYRE_AGE_OFFSET ? block[CARSTATUS_TYRE_AGE_OFFSET] : -1;
                    if (age >= 0 && age <= 200)
                        ST.LastTyreAge[car] = age;
                    else
                        ST.LastTyreAge[car] = -1;

                    byte fuelMix = block.Length > CARSTATUS_FUEL_MIX_OFFSET ? block[CARSTATUS_FUEL_MIX_OFFSET] : (byte)0;
                    if (fuelMix >= 0 && fuelMix <= 3)
                    {
                        // Проверяем изменение режима
                        bool mixChanged = !ST.LastFuelMix.TryGetValue(car, out var prevMix) || prevMix != fuelMix;

                        ST.LastFuelMix[car] = fuelMix;

                        // Записываем в историю ТОЛЬКО при изменении
                        if (mixChanged)
                        {
                            if (!ST.FuelMixHistory.ContainsKey(car))
                                ST.FuelMixHistory[car] = new List<(byte, float)>();

                            ST.FuelMixHistory[car].Add((fuelMix, hdr.SessionTime));

                            if (ST.DebugPackets)
                                Console.WriteLine($"[DBG] Car {car}: Mix {GetFuelMixName(fuelMix)} at {hdr.SessionTime:F1}s");
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (ST.DebugPackets)
                        Log.Warn($"[warning] Error parsing car status for car {car}: {ex.Message}");
                }
            }
        }

        private static void ParseLapData(ReadOnlySpan<byte> buf, PacketHeader hdr)
        {
            int baseOff = HEADER_SIZE;
            if (baseOff + LAPDATA_STRIDE * MAX_CARS > buf.Length)
            {
                if (ST.DebugPackets)
                    Log.Warn($"[warning] LapData packet too small: {buf.Length} bytes");
                return;
            }

            for (int carIdx = 0; carIdx < MAX_CARS; carIdx++)
            {
                try
                {
                    int start = baseOff + carIdx * LAPDATA_STRIDE;

                    float lastLapSec = BitConverter.ToSingle(buf.Slice(start + LAPDATA_LASTLAP_OFFSET, 4));
                    float currentLapTimeSec = BitConverter.ToSingle(buf.Slice(start + LAPDATA_CURRENTLAP_OFFSET, 4));
                    ushort s1Live = BinaryPrimitives.ReadUInt16LittleEndian(buf.Slice(start + LAPDATA_SECTOR1_OFFSET, 2));
                    ushort s2Live = BinaryPrimitives.ReadUInt16LittleEndian(buf.Slice(start + LAPDATA_SECTOR2_OFFSET, 2));
                    int currentLapNum = buf[start + LAPDATA_CURRENTLAPNUM_OFFSET];
                    int sectorIdx = buf[start + LAPDATA_SECTOR_OFFSET];
                    int currentLapInvalid = buf[start + LAPDATA_CURRENTLAPINVALID_OFFSET];

                    if (ST.DebugPackets && carIdx == 0)
                    {
                        Console.WriteLine($"[DBG] car=0: lastLap={lastLapSec:F3}s, curLap={currentLapTimeSec:F3}s, lapNum={currentLapNum}, sector={sectorIdx}, s1={s1Live}ms, s2={s2Live}ms, invalid={currentLapInvalid}");
                    }

                    // Обновляем живые данные
                    double curLapStartSec = Math.Max(0, hdr.SessionTime - currentLapTimeSec);
                    ST.LapStartSec[carIdx] = curLapStartSec;
                    ST.LiveLapNo[carIdx] = currentLapNum;
                    ST.LiveSectorIdx[carIdx] = sectorIdx;
                    ST.LiveS1[carIdx] = s1Live;
                    ST.LiveS2[carIdx] = s2Live;

                    // Буферизуем сектора ПЕРЕД переходом на следующий круг
                    if (s1Live > 0 && s2Live > 0 && currentLapNum > 0)
                    {
                        ST.BufferedSectors[carIdx] = new State.SectorBuffer(
                            S1: s1Live,
                            S2: s2Live,
                            LapNum: currentLapNum,
                            SessionUID: hdr.SessionUID
                        );
                    }

                    // Проверяем, закрылся ли круг
                    float prevLast = ST.LastLapTimeSeen.TryGetValue(carIdx, out var p) ? p : -1f;
                    int prevLap = ST.LastLapNum.TryGetValue(carIdx, out var pl) ? pl : -1;

                    ST.LastLapTimeSeen[carIdx] = lastLapSec;
                    ST.LastLapNum[carIdx] = currentLapNum;

                    // Круг считается закрытым когда:
                    // 1. Номер круга увеличился (currentLapNum > prevLap)
                    // 2. И предыдущий круг был валидный (prevLap > 0)
                    // 3. И lastLapSec содержит валидное время
                    bool lapClosed = currentLapNum > prevLap
                                     && prevLap > 0
                                     && lastLapSec >= MIN_VALID_LAP_TIME_SEC;

                    if (!lapClosed) continue;

                    // Круг закрылся - сохраняем данные
                    int completedLapNum = prevLap;
                    ST.WrittenLapCount[carIdx] = completedLapNum;

                    int lapTimeMs = (int)Math.Round(lastLapSec * 1000.0);

                    // Валидация: круг не может быть слишком медленным
                    if (lapTimeMs > MAX_REASONABLE_LAP_MS)
                    {
                        if (ST.DebugPackets)
                            Console.WriteLine($"[DBG] Lap {completedLapNum} for car {carIdx} too slow ({MsToStr(lapTimeMs)}), skipping");
                        continue;
                    }

                    // Получаем сектора из буфера
                    int finalS1 = 0, finalS2 = 0, finalS3 = 0;
                    bool sectorsFound = false;

                    if (ST.BufferedSectors.TryGetValue(carIdx, out var buffer))
                    {
                        // Проверяем что буфер относится к правильному кругу и сессии
                        if (buffer.LapNum == completedLapNum && buffer.SessionUID == hdr.SessionUID)
                        {
                            finalS1 = buffer.S1;
                            finalS2 = buffer.S2;
                            finalS3 = lapTimeMs - finalS1 - finalS2;

                            // Валидация: S3 не может быть отрицательным
                            if (finalS3 < 0)
                            {
                                if (ST.DebugPackets)
                                    Console.WriteLine($"[DBG] Negative S3 for car {carIdx} lap {completedLapNum}: S1={finalS1}, S2={finalS2}, LapTime={lapTimeMs}");
                                finalS3 = 0;
                            }

                            sectorsFound = true;

                            if (ST.DebugPackets)
                                Console.WriteLine($"[DBG] Using buffered sectors for car {carIdx} lap {completedLapNum}: S1={finalS1}, S2={finalS2}, S3={finalS3}");
                        }
                        else if (ST.DebugPackets)
                        {
                            Console.WriteLine($"[DBG] Buffer mismatch for car {carIdx}: buffer lap={buffer.LapNum}, completed lap={completedLapNum}");
                        }
                    }

                    // Если сектора не найдены - пропускаем этот круг
                    if (!sectorsFound)
                    {
                        if (ST.DebugPackets)
                            Console.WriteLine($"[DBG] No valid sectors found for car {carIdx} lap {completedLapNum}, skipping");
                        continue;
                    }

                    // Определяем валидность каждого сектора и круга
                    bool isLapInvalid = ST.InvalidLaps.Contains((hdr.SessionUID, carIdx, completedLapNum)) ||
                                        ST.LiveInvalidLaps.Contains((hdr.SessionUID, carIdx, completedLapNum));

                    bool v1 = !isLapInvalid;
                    bool v2 = !isLapInvalid;
                    bool v3 = !isLapInvalid;
                    bool vL = !isLapInvalid;

                    // Сохраняем сектора в историю
                    var dictByCar = ST.LapSectors.GetOrAdd(hdr.SessionUID, _ => new());
                    var lapMap = dictByCar.GetOrAdd(carIdx, _ => new Dictionary<int, State.SectorRec>());

                    lapMap[completedLapNum] = new State.SectorRec(
                        finalS1, finalS2, finalS3,
                        v1, v2, v3, vL
                    );

                    if (ST.DebugPackets)
                    {
                        string validity = vL ? "VALID" : "INVALID";
                        Console.WriteLine($"[DBG] LAP CLOSED car={carIdx} lap={completedLapNum} lapMs={lapTimeMs} S1={finalS1} S2={finalS2} S3={finalS3} [{validity}]");
                    }

                    // Формируем данные для сохранения
                    double lapEndSec = hdr.SessionTime;
                    double lapStartSec = ST.LapStartSec.TryGetValue(carIdx, out var ls) ? ls : Math.Max(0, lapEndSec - lastLapSec);
                    string scTag = GetScOverlap(hdr.SessionUID, lapStartSec, lapEndSec);
                    string display = ST.DriverDisplay.TryGetValue(carIdx, out var disp) ? disp : $"Unknown [{carIdx}]";
                    string rawName = ST.DriverRawName.TryGetValue(carIdx, out var nm) ? nm : "Unknown";
                    ST.LastWear.TryGetValue(carIdx, out var wear);
                    string tyre = ST.CurrentTyre.TryGetValue(carIdx, out var t) ? t : "Unknown";
                    int tyreAge = ST.LastTyreAge.TryGetValue(carIdx, out var age) ? age : -1;
                    byte? fuelMix = null;
                    string? mixHistory = null;

                    if (ST.FuelMixHistory.TryGetValue(carIdx, out var history) && history.Count > 0)
                    {
                        // Фильтруем записи для текущего круга
                        var lapHistory = history
                            .Where(h => h.timestamp >= lapStartSec && h.timestamp <= lapEndSec)
                            .OrderBy(h => h.timestamp)
                            .ToList();

                        if (lapHistory.Count > 0)
                        {
                            // Определяем доминирующий режим по длительности использования
                            var mixDurations = new Dictionary<byte, double>();

                            for (int i = 0; i < lapHistory.Count; i++)
                            {
                                byte currentMix = lapHistory[i].mix;
                                double startTime = lapHistory[i].timestamp;
                                double endTime = (i + 1 < lapHistory.Count)
                                    ? lapHistory[i + 1].timestamp
                                    : lapEndSec;

                                double duration = endTime - startTime;

                                if (!mixDurations.ContainsKey(currentMix))
                                    mixDurations[currentMix] = 0;

                                mixDurations[currentMix] += duration;
                            }

                            // Выбираем режим с наибольшей длительностью
                            fuelMix = mixDurations.OrderByDescending(kv => kv.Value).First().Key;

                            var switches = new List<string>();

                            foreach (var (mix, ts) in lapHistory)
                            {
                                double timeInLap = ts - lapStartSec;
                                switches.Add($"{GetFuelMixName(mix)} ( {timeInLap:F1}s )");
                            }

                            mixHistory = string.Join(" → ", switches);

                            if (ST.DebugPackets)
                            {
                                Console.WriteLine($"[DBG] Car {carIdx} lap {completedLapNum}: mix={GetFuelMixName(fuelMix.Value)}, history={mixHistory}");
                            }
                        }
                    }

                    // Fallback
                    if (!fuelMix.HasValue)
                    {
                        fuelMix = ST.LastFuelMix.TryGetValue(carIdx, out var fm) ? fm : (byte?)null;
                    }

                    var row = new ParsedRow(
                        SessionUID: hdr.SessionUID,
                        SessionType: ST.SessionType,
                        CarIdx: carIdx,
                        DriverColumn: display,
                        DriverRaw: rawName,
                        Lap: completedLapNum,
                        Tyre: tyre,
                        LapTimeMs: lapTimeMs,
                        WearAvg: wear?.Avg,
                        RL: wear?.RL,
                        RR: wear?.RR,
                        FL: wear?.FL,
                        FR: wear?.FR,
                        SessTimeSec: hdr.SessionTime,
                        ScStatus: scTag,
                        TyreAge: tyreAge,
                        S1Ms: finalS1,
                        S2Ms: finalS2,
                        S3Ms: finalS3,
                        FuelMix: fuelMix,
                        FuelMixHistory: mixHistory
                    );

                    var q = ST.SessionBuffers.GetOrAdd(hdr.SessionUID, _ => new ConcurrentQueue<ParsedRow>());
                    q.Enqueue(row);
                    UpdateProvisionalAgg(hdr.SessionUID, row);

                    if (ST.FuelMixHistory.TryGetValue(carIdx, out var hist))
                    {
                        hist.Clear();
                        if (ST.DebugPackets)
                            Console.WriteLine($"[DBG] Cleared fuel mix history for car {carIdx} after lap {completedLapNum}");
                    }
                }
                catch (Exception ex)
                {
                    if (ST.DebugPackets)
                        Log.Warn($"[warning] Error parsing lap data for car {carIdx}: {ex.Message}");
                }
            }
        }

        // ========== Save / Excel ==========

        private static async Task SaveAllAsync(bool makeExcel)
        {
            // FIXED: Atomic check and lock
            if (!Monitor.TryEnter(SaveLock, 0))
            {
                Console.WriteLine("[i] Save already in progress, skipping...");
                return;
            }

            try
            {
                _saving = true;
                var (csvPath, xlsxPath) = GetStagePaths();
                Directory.CreateDirectory(Path.GetDirectoryName(csvPath)!);

                int appended = 0;
                bool newFile = !File.Exists(csvPath);
                using (var fs = new FileStream(csvPath, FileMode.Append, FileAccess.Write, FileShare.Read))
                using (var sw = new StreamWriter(fs, Encoding.UTF8))
                {
                    if (newFile)
                    {
                        await sw.WriteLineAsync("session_uid,session_type,car_idx,driver_column,driver_raw,lap,tyre,lap_time_ms,wear_avg,wear_rl,wear_rr,wear_fl,wear_fr,sess_time_sec,sc_status,tyre_age,s1_ms,s2_ms,s3_ms,fuel_mix,fuel_mix_history");
                    }

                    foreach (var kv in ST.SessionBuffers.ToArray())
                    {
                        var uid = kv.Key;
                        var q = kv.Value;
                        if (q.IsEmpty) continue;

                        while (q.TryDequeue(out var r))
                        {
                            string sc = !string.IsNullOrEmpty(r.ScStatus) ? r.ScStatus! : GetScAt(uid, r.SessTimeSec);

                            await sw.WriteLineAsync(string.Join(",",
                                r.SessionUID,
                                r.SessionType,
                                r.CarIdx,
                                Csv(r.DriverColumn),
                                Csv(r.DriverRaw),
                                r.Lap,
                                Csv(r.Tyre),
                                r.LapTimeMs,
                                (r.WearAvg?.ToString(CultureInfo.InvariantCulture) ?? ""),
                                (r.RL?.ToString(CultureInfo.InvariantCulture) ?? ""),
                                (r.RR?.ToString(CultureInfo.InvariantCulture) ?? ""),
                                (r.FL?.ToString(CultureInfo.InvariantCulture) ?? ""),
                                (r.FR?.ToString(CultureInfo.InvariantCulture) ?? ""),
                                r.SessTimeSec.ToString(CultureInfo.InvariantCulture),
                                sc,
                                r.TyreAge.ToString(CultureInfo.InvariantCulture),
                                r.S1Ms, r.S2Ms, r.S3Ms,
                                r.FuelMix?.ToString() ?? "",
                                Csv(r.FuelMixHistory ?? "")
                            ));
                            appended++;
                        }
                    }
                }
                if (appended > 0)
                    Console.WriteLine($"[+] {Path.GetFileName(csvPath)} +{appended}");

                if (makeExcel)
                {
                    FixCsvDriverNames(csvPath);
                    FixCsvBarePlayerAi(csvPath);

                    try
                    {
                        BuildExcel(csvPath, xlsxPath);
                        Console.WriteLine($"[i] File Rebuilt: {Path.GetFileName(xlsxPath)}");
                    }
                    catch (Exception ex)
                    {
                        Log.Warn("[warning] Excel Build Failed: " + ex.Message);
                    }

                    ST.LastFinalSaveUtc = DateTime.UtcNow;
                }
            }
            finally
            {
                _saving = false;
                Monitor.Exit(SaveLock);
            }
        }

        private static (string csvPath, string xlsxPath) GetStagePaths()
        {
            string outDir = GetOutputDir();

            string track = string.IsNullOrWhiteSpace(ST.AnchorTrackName) ? "UnknownTrack" : SafeName(ST.AnchorTrackName);
            string uid = (ST.AnchorSessionUID != 0) ? ST.AnchorSessionUID.ToString() : "session";

            string baseName = $"log_f1_2020_{track}_{uid}";
            return (Path.Combine(outDir, baseName + ".csv"),
                    Path.Combine(outDir, baseName + ".xlsx"));
        }

        private static string SafeName(string s)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var sb = new StringBuilder(s.Length);
            foreach (var ch in s)
                sb.Append(invalid.Contains(ch) ? '_' : ch);
            return sb.ToString();
        }


        // ========== ClosedXML Excel Builder ==========

        private static void BuildExcel(string csvPath, string xlsxPath)
        {
            var lines = File.ReadAllLines(csvPath);
            if (lines.Length <= 1)
            {
                using var wb0 = new XLWorkbook();
                var ws0 = wb0.AddWorksheet("Info");
                ws0.Cell(1, 1).Value = "Нет данных";
                wb0.SaveAs(xlsxPath);
                return;
            }

            var rows = lines.Skip(1)
                            .Select(ParseCsvRow)
                            .Where(r => r != null)
                            .Select(r => r!)
                            .ToList();

            try { NormalizeDriverNames(rows); } catch { }

            using var wb = new XLWorkbook();

            var sessionTypes = rows.Select(r => r.SessionType).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            if (sessionTypes.Contains("ShortQ", StringComparer.OrdinalIgnoreCase))
            {
                AddQualiSheetXL(wb, "ShortQ",
                    rows.Where(r => r.SessionType.Equals("ShortQ", StringComparison.OrdinalIgnoreCase)));
            }
            else if (sessionTypes.Contains("OSQ", StringComparer.OrdinalIgnoreCase))
            {
                AddQualiSheetXL(wb, "OSQ",
                    rows.Where(r => r.SessionType.Equals("OSQ", StringComparison.OrdinalIgnoreCase)));
            }
            else
            {
                if (sessionTypes.Contains("Q1", StringComparer.OrdinalIgnoreCase))
                    AddQualiSheetXL(wb, "Q1",
                        rows.Where(r => r.SessionType.Equals("Q1", StringComparison.OrdinalIgnoreCase)));

                if (sessionTypes.Contains("Q2", StringComparer.OrdinalIgnoreCase))
                    AddQualiSheetXL(wb, "Q2",
                        rows.Where(r => r.SessionType.Equals("Q2", StringComparison.OrdinalIgnoreCase)));

                if (sessionTypes.Contains("Q3", StringComparer.OrdinalIgnoreCase))
                    AddQualiSheetXL(wb, "Q3",
                        rows.Where(r => r.SessionType.Equals("Q3", StringComparison.OrdinalIgnoreCase)));
            }

            AddLapSheetXL(wb, "Race", rows.Where(IsRaceSession));
            AddTyreDegradationXL(wb, rows);
            AddAveragePaceXL(wb, rows);
            AddMaxSpeedXL(wb);
            AddIncidentsXL(wb);

            var info = wb.AddWorksheet("Info");
            var (csvFinal, xlsxFinal) = GetStagePaths();
            info.Cell(1, 1).Value = "CSV"; info.Cell(1, 2).Value = Path.GetFileName(csvFinal);
            info.Cell(2, 1).Value = "XLSX"; info.Cell(2, 2).Value = Path.GetFileName(xlsxFinal);
            info.Cell(3, 1).Value = "Track"; info.Cell(3, 2).Value = ST.AnchorTrackName;
            info.Cell(4, 1).Value = "Date"; info.Cell(4, 2).Value = ST.AnchorLocalDate;
            info.Cell(5, 1).Value = "SessionUID(anchor)"; info.Cell(5, 2).Value = ST.AnchorSessionUID.ToString();

            info.Columns().AdjustToContents();
            wb.SaveAs(xlsxPath);
        }

        private static void AddLapSheetXL(XLWorkbook wb, string sheetName, IEnumerable<ParsedRow> data)
        {
            var list = data.OrderBy(r => r.DriverColumn, StringComparer.OrdinalIgnoreCase)
                           .ThenBy(r => r.Lap)
                           .ToList();

            var ws = wb.AddWorksheet(sheetName);

            if (list.Count == 0)
            {
                ws.Cell(1, 1).Value = $"В {sheetName} нет данных";
                ws.Columns().AdjustToContents();
                return;
            }

            ulong sessionUid = list[0].SessionUID;
            var drivers = list.Select(r => r.CarIdx).Distinct().ToList();

            if (ST.FinalOrder.TryGetValue((sessionUid, "Race"), out var order) && order.Count > 0)
            {
                var orderedDrivers = order.Where(car => drivers.Contains(car)).ToList();
                var missingDrivers = drivers.Except(orderedDrivers).OrderBy(car =>
                    ST.DriverDisplay.TryGetValue(car, out var d) ? d : $"Car {car}",
                    StringComparer.OrdinalIgnoreCase).ToList();
                drivers = orderedDrivers.Concat(missingDrivers).ToList();
            }
            else
            {
                drivers = drivers.OrderBy(car => ST.DriverDisplay.TryGetValue(car, out var d) ? d : $"Car {car}",
                                          StringComparer.OrdinalIgnoreCase).ToList();
            }

            var driverColumns = drivers.Select(car =>
                ST.DriverDisplay.TryGetValue(car, out var d) ? d : $"Car {car}"
            ).ToList();

            // Заголовок "Lap"
            ws.Cell(1, 1).Value = "Lap";
            ws.Cell(1, 1).Style.Font.SetBold();
            ws.Cell(1, 1).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            ws.Cell(1, 1).Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

            // Заголовки пилотов
            for (int c = 0; c < driverColumns.Count; c++)
            {
                var headerCell = ws.Cell(1, 2 + c);
                headerCell.Value = driverColumns[c];
                headerCell.Style.Font.SetBold();
                headerCell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

                XLColor bg = XLColor.NoColor;
                int? carIdx = TryExtractCarIndexFromDisplay(driverColumns[c]);
                if (carIdx.HasValue && ST.TeamByCar.TryGetValue(carIdx.Value, out var teamId))
                    bg = TeamColor(teamId);

                if (bg != XLColor.NoColor)
                {
                    headerCell.Style.Fill.BackgroundColor = bg;
                }
            }

            var byDriver = list.GroupBy(r => r.DriverColumn, StringComparer.OrdinalIgnoreCase)
                               .ToDictionary(g => g.Key, g => g.OrderBy(x => x.Lap).ToList(), StringComparer.OrdinalIgnoreCase);

            int maxRows = byDriver.Values.Select(v => v.Count).DefaultIfEmpty(0).Max();

            // Заполнение данных
            for (int i = 0; i < maxRows; i++)
            {
                // Номер круга
                var lapCell = ws.Cell(2 + i, 1);
                lapCell.Value = i + 1;
                lapCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
                lapCell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

                for (int c = 0; c < driverColumns.Count; c++)
                {
                    var d = driverColumns[c];
                    if (byDriver.TryGetValue(d, out var laps) && i < laps.Count)
                    {
                        var r = laps[i];
                        string cellText = $"{MsToStr(r.LapTimeMs)} ({r.Tyre})"
                                          + (r.WearAvg.HasValue ? $" — {Math.Round(r.WearAvg.Value)}%" : "")
                                          + (string.IsNullOrEmpty(r.ScStatus) ? "" : $" [{r.ScStatus}]");

                        var cell = ws.Cell(2 + i, 2 + c);
                        cell.Value = cellText;
                        cell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

                        var color = GetTyreColor(r.Tyre);
                        if (color != XLColor.NoColor)
                            cell.Style.Fill.BackgroundColor = color;

                        bool hasAnyWheel = r.FL.HasValue || r.FR.HasValue || r.RL.HasValue || r.RR.HasValue;
                        if (hasAnyWheel)
                        {
                            string fl = r.FL.HasValue ? $"{r.FL.Value}%" : "?";
                            string fr = r.FR.HasValue ? $"{r.FR.Value}%" : "?";
                            string rl = r.RL.HasValue ? $"{r.RL.Value}%" : "?";
                            string rr = r.RR.HasValue ? $"{r.RR.Value}%" : "?";

                            var comment = cell.CreateComment();
                            comment.AddText($"{fl} - FL FR - {fr}\n{rl} - RL RR - {rr}");
                        }
                    }
                }
            }

            ws.SheetView.Freeze(1, 1);

            var used = ws.Range(2, 2, Math.Max(2, maxRows + 1), Math.Max(2, driverColumns.Count + 1));
            var scRule = used.AddConditionalFormat().WhenContains("[SC]");
            scRule.Font.SetItalic();
            scRule.Font.SetFontColor(GetTyreColor("SC"));
            var vscRule = used.AddConditionalFormat().WhenContains("[VSC]");
            vscRule.Font.SetItalic();
            vscRule.Font.SetFontColor(GetTyreColor("VSC"));

            // Установка ширины колонок в самом конце
            ws.Column(1).Width = 5;
            for (int c = 2; c <= driverColumns.Count + 1; c++)
            {
                ws.Column(c).Width = 27;
            }
        }

        private static void AddQualiSheetXL(XLWorkbook wb, string sheetName, IEnumerable<ParsedRow> data)
        {
            var list = data.Where(r => r.LapTimeMs > 0)
                           .OrderBy(r => r.DriverColumn, StringComparer.OrdinalIgnoreCase)
                           .ThenBy(r => r.Lap)
                           .ToList();

            var ws = wb.AddWorksheet(sheetName);
            if (list.Count == 0)
            {
                ws.Cell(1, 1).Value = $"В {sheetName} нет данных";
                ws.Columns().AdjustToContents();
                return;
            }

            ulong sessUid = list[0].SessionUID;
            var drivers = list.Select(r => r.CarIdx).Distinct().ToList();

            if (ST.FinalOrder.TryGetValue((sessUid, "Quali"), out var order) && order.Count > 0)
            {
                var orderedDrivers = order.Where(car => drivers.Contains(car)).ToList();
                var missingDrivers = drivers.Except(orderedDrivers).OrderBy(car =>
                    ST.DriverDisplay.TryGetValue(car, out var d) ? d : $"Car {car}",
                    StringComparer.OrdinalIgnoreCase).ToList();
                drivers = orderedDrivers.Concat(missingDrivers).ToList();
            }
            else
            {
                drivers = drivers.OrderBy(car => ST.DriverDisplay.TryGetValue(car, out var d) ? d : $"Car {car}",
                                          StringComparer.OrdinalIgnoreCase).ToList();
            }

            int MinValidSectorFromHistory(int car, int sector)
            {
                if (!ST.LapSectors.TryGetValue(sessUid, out var byCar) ||
                    !byCar.TryGetValue(car, out var laps))
                    return int.MaxValue;

                int best = int.MaxValue;
                foreach (var kv in laps)
                {
                    var rec = kv.Value;
                    bool valid = sector switch
                    {
                        1 => rec.V1,
                        2 => rec.V2,
                        3 => rec.V3,
                        _ => true
                    };
                    int ms = sector switch { 1 => rec.S1, 2 => rec.S2, 3 => rec.S3, _ => 0 };
                    if (valid && ms > 0 && ms < best) best = ms;
                }
                return best;
            }

            // Заголовок "Lap"
            ws.Cell(1, 1).Value = "Lap";
            ws.Cell(1, 1).Style.Font.SetBold();
            ws.Cell(1, 1).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            ws.Cell(1, 1).Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

            int col = 2;
            foreach (var car in drivers)
            {
                string name = ST.DriverDisplay.TryGetValue(car, out var d) ? d : $"Car {car}";

                ws.Range(1, col, 1, col + 1).Merge();
                var headerCell = ws.Cell(1, col);
                headerCell.Value = name;
                headerCell.Style.Font.SetBold();
                headerCell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

                if (ST.TeamByCar.TryGetValue(car, out var teamId))
                    headerCell.Style.Fill.BackgroundColor = TeamColor(teamId);

                col += 2;
            }

            var perDriverBest = drivers.ToDictionary(
                car => car,
                car => (
                    s1: MinValidSectorFromHistory(car, 1),
                    s2: MinValidSectorFromHistory(car, 2),
                    s3: MinValidSectorFromHistory(car, 3)
                )
            );

            int bestAllS1 = drivers.Select(car => perDriverBest[car].s1).DefaultIfEmpty(int.MaxValue).Min();
            int bestAllS2 = drivers.Select(car => perDriverBest[car].s2).DefaultIfEmpty(int.MaxValue).Min();
            int bestAllS3 = drivers.Select(car => perDriverBest[car].s3).DefaultIfEmpty(int.MaxValue).Min();

            var bestRealList = list
                .GroupBy(r => r.CarIdx)
                .Select(g => (Car: g.Key, Best: g.Min(x => x.LapTimeMs)))
                .OrderBy(x => x.Best)
                .ToList();

            var realPosByCar = bestRealList.Select((x, i) => (x.Car, Pos: i + 1))
                                           .ToDictionary(x => x.Car, x => x.Pos);
            int totalReal = bestRealList.Count;

            var vblList = new List<(int Car, int Time)>();
            foreach (var car in drivers)
            {
                int bS1 = perDriverBest[car].s1;
                int bS2 = perDriverBest[car].s2;
                int bS3 = perDriverBest[car].s3;
                if (bS1 != int.MaxValue && bS2 != int.MaxValue && bS3 != int.MaxValue)
                    vblList.Add((car, bS1 + bS2 + bS3));
            }
            var rankedVbl = vblList.OrderBy(x => x.Time).ToList();
            var vblPosByCar = rankedVbl.Select((x, i) => (x.Car, Pos: i + 1))
                                       .ToDictionary(x => x.Car, x => x.Pos);
            int totalVbl = rankedVbl.Count;

            int maxLap = list.Max(r => r.Lap);

            for (int lap = 1; lap <= maxLap; lap++)
            {
                int baseRow = 2 + (lap - 1) * 3;

                // Номер круга (объединенная ячейка на 3 строки)
                ws.Range(baseRow, 1, baseRow + 2, 1).Merge();
                var lapCell = ws.Cell(baseRow, 1);
                lapCell.Value = lap;
                lapCell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;
                lapCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;

                col = 2;
                foreach (var car in drivers)
                {
                    var r = list.FirstOrDefault(x => x.CarIdx == car && x.Lap == lap);

                    int colTime = col;
                    int colSecs = col + 1;
                    col += 2;

                    // Время круга (объединенная ячейка на 3 строки)
                    ws.Range(baseRow, colTime, baseRow + 2, colTime).Merge();
                    var lapTimeCell = ws.Cell(baseRow, colTime);
                    if (r != null)
                    {
                        lapTimeCell.Value = $"{MsToStr(r.LapTimeMs)} ({r.Tyre})"
                                        + (r.WearAvg.HasValue ? $" — {Math.Round(r.WearAvg.Value)}%" : "")
                                        + (string.IsNullOrEmpty(r.ScStatus) ? "" : $" [{r.ScStatus}]");

                        var color = GetTyreColor(r.Tyre);
                        if (color != XLColor.NoColor)
                            lapTimeCell.Style.Fill.BackgroundColor = color;

                        bool hasAnyWheel = r.FL.HasValue || r.FR.HasValue || r.RL.HasValue || r.RR.HasValue;
                        if (hasAnyWheel)
                        {
                            string fl = r.FL.HasValue ? $"{r.FL.Value}%" : "?";
                            string fr = r.FR.HasValue ? $"{r.FR.Value}%" : "?";
                            string rl = r.RL.HasValue ? $"{r.RL.Value}%" : "?";
                            string rr = r.RR.HasValue ? $"{r.RR.Value}%" : "?";

                            var comment = lapTimeCell.CreateComment();
                            comment.AddText($"{fl} - FL FR - {fr}\n{rl} - RL RR - {rr}");
                        }
                    }
                    lapTimeCell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

                    WriteSector(ws.Cell(baseRow + 0, colSecs), r, r?.S1Ms ?? 0, perDriverBest[car].s1, bestAllS1, 1);
                    WriteSector(ws.Cell(baseRow + 1, colSecs), r, r?.S2Ms ?? 0, perDriverBest[car].s2, bestAllS2, 2);
                    WriteSector(ws.Cell(baseRow + 2, colSecs), r, r?.S3Ms ?? 0, perDriverBest[car].s3, bestAllS3, 3);
                }

                if (lap < maxLap)
                {
                    var bottomRow = ws.Range(baseRow + 2, 1, baseRow + 2, drivers.Count * 2 + 1);
                    bottomRow.Style.Border.BottomBorder = XLBorderStyleValues.Thin;
                    bottomRow.Style.Border.BottomBorderColor = XLColor.Black;
                }
            }

            int vblRow = 3 * maxLap + 2;
            var vblLapCell = ws.Cell(vblRow, 1);
            vblLapCell.Value = "VBL";
            vblLapCell.Style.Font.SetBold();
            vblLapCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            vblLapCell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

            col = 2;
            foreach (var car in drivers)
            {
                int bS1 = perDriverBest[car].s1;
                int bS2 = perDriverBest[car].s2;
                int bS3 = perDriverBest[car].s3;
                bool hasAll = (bS1 != int.MaxValue && bS2 != int.MaxValue && bS3 != int.MaxValue);

                var cellRange = ws.Range(vblRow, col, vblRow, col + 1);
                cellRange.Merge();

                var vblCell = ws.Cell(vblRow, col);
                if (hasAll)
                {
                    int vbl = bS1 + bS2 + bS3;
                    vblCell.Value = MsToStr(vbl);

                    vblCell.Style.Fill.BackgroundColor = XLColor.FromHtml("#9B3FF5");
                    vblCell.Style.Font.SetBold();
                    vblCell.Style.Font.SetFontColor(XLColor.White);
                    vblCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
                    vblCell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

                    var cmt = vblCell.CreateComment();
                    if (realPosByCar.TryGetValue(car, out int realPos))
                        cmt.AddText($"Real Place: P{realPos} out {totalReal}\n");
                    if (vblPosByCar.TryGetValue(car, out int vblPos))
                        cmt.AddText($"Virtual Place: P{vblPos} out {totalVbl}");
                }
                else
                {
                    vblCell.Value = "-";
                    vblCell.Style.Fill.BackgroundColor = XLColor.FromHtml("#F56440");
                    vblCell.Style.Font.SetFontColor(XLColor.White);
                    vblCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
                    vblCell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;
                }

                col += 2;
            }

            ws.SheetView.Freeze(1, 1);
            ws.Column(1).Width = 5;
            int tempCol = 2;
            foreach (var car in drivers)
            {
                ws.Column(tempCol).Width = 27;
                ws.Column(tempCol + 1).Width = 9;
                tempCol += 2;
            }
            ws.Style.Alignment.WrapText = false;

            void WriteSector(IXLCell cell, ParsedRow? row, int ms, int bestDriver, int bestAll, int sectorNo)
            {
                if (ms <= 0)
                {
                    cell.Value = "";
                    return;
                }

                bool valid = (row == null) ? true : IsSectorValid(row, sectorNo);

                cell.Value = MsToStr(ms);
                cell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
                cell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

                if (!valid)
                {
                    cell.Style.Fill.BackgroundColor = XLColor.FromHtml("#F56440");
                    cell.Style.Font.SetFontColor(XLColor.White);
                    return;
                }

                if (ms == bestAll)
                {
                    cell.Style.Fill.BackgroundColor = XLColor.FromHtml("#9B3FF5");
                    cell.Style.Font.SetFontColor(XLColor.White);
                }
                else if (ms == bestDriver)
                    cell.Style.Fill.BackgroundColor = XLColor.FromHtml("#40F57B");
                else
                    cell.Style.Fill.BackgroundColor = XLColor.FromHtml("#E1E1E1");
            }
        }

        private static void AddMaxSpeedXL(XLWorkbook wb)
        {
            if (ST.MaxSpeedQuali.Count == 0 && ST.MaxSpeedRace.Count == 0) return;

            var ws = wb.AddWorksheet("Max Speed");

            ws.Cell(1, 1).Value = "Qualifying";
            ws.Range(1, 1, 1, 2).Merge().Style.Font.SetBold();
            ws.Cell(2, 1).Value = "Driver";
            ws.Cell(2, 2).Value = "Max Speed (km/h)";
            ws.Range(2, 1, 2, 2).Style.Font.SetBold();

            ws.Cell(1, 4).Value = "Race";
            ws.Range(1, 4, 1, 5).Merge().Style.Font.SetBold();
            ws.Cell(2, 4).Value = "Driver";
            ws.Cell(2, 5).Value = "Max Speed (km/h)";
            ws.Range(2, 4, 2, 5).Style.Font.SetBold();

            var quali = ST.MaxSpeedQuali
                .Select(kv =>
                {
                    int car = kv.Key; int vmax = kv.Value;
                    string name = ST.DriverDisplay.TryGetValue(car, out var disp) ? disp : $"Unknown [{car}]";
                    return (name, vmax);
                })
                .OrderByDescending(x => x.vmax)
                .ThenBy(x => x.name, StringComparer.OrdinalIgnoreCase)
                .ToList();

            int rQ = 3;
            foreach (var x in quali)
            {
                ws.Cell(rQ, 1).Value = x.name;
                ws.Cell(rQ, 2).Value = x.vmax;
                rQ++;
            }

            var race = ST.MaxSpeedRace
                .Select(kv =>
                {
                    int car = kv.Key; int vmax = kv.Value;
                    string name = ST.DriverDisplay.TryGetValue(car, out var disp) ? disp : $"Unknown [{car}]";
                    return (name, vmax);
                })
                .OrderByDescending(x => x.vmax)
                .ThenBy(x => x.name, StringComparer.OrdinalIgnoreCase)
                .ToList();

            int rR = 3;
            foreach (var x in race)
            {
                ws.Cell(rR, 4).Value = x.name;
                ws.Cell(rR, 5).Value = x.vmax;
                rR++;
            }

            ws.Columns().AdjustToContents();
            ws.Rows().AdjustToContents();
        }

        private static void AddAveragePaceXL(XLWorkbook wb, List<ParsedRow> allRows)
        {
            var ws = wb.AddWorksheet("Average Pace");

            var qualiBase = allRows.Where(IsQualiSession).Where(r => r.LapTimeMs > 0).ToList();
            var raceBase = allRows.Where(IsRaceSession).Where(r => r.LapTimeMs > 0).ToList();

            var qualiClean = GetCleanLaps(qualiBase);
            var raceClean = GetCleanLaps(raceBase);

            if (qualiClean.Count == 0) qualiClean = qualiBase;
            if (raceClean.Count == 0) raceClean = raceBase;

            int r = 1;

            ws.Cell(r, 1).Value = "Qualifying";
            ws.Range(r, 1, r, 3).Merge().Style.Font.SetBold();
            r++;

            ws.Cell(r, 1).Value = "Driver";
            ws.Cell(r, 2).Value = "Tyre";
            ws.Cell(r, 3).Value = "Avg Time";
            ws.Range(r, 1, r, 3).Style.Font.SetBold();
            r++;

            var qualiAvg = qualiClean
                .GroupBy(x => new { x.DriverColumn, x.Tyre })
                .Select(g => new { g.Key.DriverColumn, g.Key.Tyre, AvgMs = g.Average(x => x.LapTimeMs), N = g.Count() })
                .OrderBy(x => x.DriverColumn, StringComparer.OrdinalIgnoreCase)
                .ThenBy(x => TyreOrder(x.Tyre))
                .ToList();

            foreach (var x in qualiAvg)
            {
                ws.Cell(r, 1).Value = x.DriverColumn;
                ws.Cell(r, 2).Value = x.Tyre;
                ws.Cell(r, 3).Value = MsToStr((int)Math.Round(x.AvgMs));
                var col = GetTyreColor(x.Tyre);
                if (col != XLColor.NoColor) ws.Cell(r, 3).Style.Fill.BackgroundColor = col;
                ws.Cell(r, 3).CreateComment().AddText($"laps: {x.N}");
                r++;
            }

            r = 1;

            ws.Cell(r, 5).Value = "Race";
            ws.Range(r, 5, r, 7).Merge().Style.Font.SetBold();
            r++;

            ws.Cell(r, 5).Value = "Driver";
            ws.Cell(r, 6).Value = "Tyre";
            ws.Cell(r, 7).Value = "Avg Time";
            ws.Range(r, 5, r, 7).Style.Font.SetBold();
            r++;

            var raceAvg = raceClean
                .GroupBy(x => new { x.DriverColumn, x.Tyre })
                .Select(g => new { g.Key.DriverColumn, g.Key.Tyre, AvgMs = g.Average(x => x.LapTimeMs), N = g.Count() })
                .OrderBy(x => x.DriverColumn, StringComparer.OrdinalIgnoreCase)
                .ThenBy(x => TyreOrder(x.Tyre))
                .ToList();

            foreach (var x in raceAvg)
            {
                ws.Cell(r, 5).Value = x.DriverColumn;
                ws.Cell(r, 6).Value = x.Tyre;
                ws.Cell(r, 7).Value = MsToStr((int)Math.Round(x.AvgMs));
                var col = GetTyreColor(x.Tyre);
                if (col != XLColor.NoColor) ws.Cell(r, 7).Style.Fill.BackgroundColor = col;
                ws.Cell(r, 7).CreateComment().AddText($"laps: {x.N}");
                r++;
            }

            ws.Columns().AdjustToContents();
            ws.Rows().AdjustToContents();
        }

        private static void AddIncidentsXL(XLWorkbook wb)
        {
            if (ST.Incidents == null || ST.Incidents.Count == 0) return;

            var ws = wb.AddWorksheet("Incidents");

            ws.Cell(1, 1).Value = "Session";
            ws.Cell(1, 2).Value = "Lap";
            ws.Cell(1, 3).Value = "Driver";
            ws.Cell(1, 4).Value = "Incident";
            ws.Cell(1, 5).Value = "Penalty";
            ws.Range(1, 1, 1, 5).Style.Font.SetBold();

            int r = 2;

            var sessions = ST.Incidents
                .Select(kv =>
                {
                    var uid = kv.Key;
                    var sessName = ST.SessionTypeByUid.TryGetValue(uid, out var nm) ? nm : "Unknown";
                    return new
                    {
                        Uid = uid,
                        Name = sessName,
                        Order = SessionOrderKey(sessName),
                        Items = kv.Value
                    };
                })
                .OrderBy(x => x.Order)
                .ThenBy(x => x.Uid)
                .ToList();

            foreach (var sess in sessions)
            {
                ws.Cell(r, 1).Value = sess.Name;
                ws.Range(r, 1, r, 5).Merge();
                ws.Row(r).Style.Font.SetBold();
                ws.Row(r).Style.Fill.BackgroundColor = XLColor.FromHtml("#EEEEEE");
                r++;

                var items = (sess.Items ?? new List<IncidentRow>())
                    .OrderBy(x => x.Lap)
                    .ThenBy(x => x.SessTimeSec)
                    .ToList();

                foreach (var x in items)
                {
                    string driver = (x.CarIdx >= 0 && ST.DriverDisplay.TryGetValue(x.CarIdx, out var disp))
                                    ? disp
                                    : (x.CarIdx >= 0 ? $"Car {x.CarIdx}" : "—");

                    ws.Cell(r, 1).Value = "";
                    ws.Cell(r, 2).Value = x.Lap > 0 ? x.Lap : 0;
                    ws.Cell(r, 3).Value = driver;
                    ws.Cell(r, 4).Value = string.IsNullOrWhiteSpace(x.Accident) ? "-" : x.Accident;
                    ws.Cell(r, 5).Value = string.IsNullOrWhiteSpace(x.Penalty) ? "-" : x.Penalty;

                    var cat = PenaltyCategory(x.Penalty ?? string.Empty);
                    if (cat == "Penalty")
                        ws.Row(r).Style.Fill.BackgroundColor = XLColor.FromHtml("#FFC0C0");
                    else if (cat == "Warning")
                        ws.Row(r).Style.Fill.BackgroundColor = XLColor.FromHtml("#FFFACD");
                    r++;
                }
                r++;
            }

            ws.SheetView.Freeze(1, 0);
            ws.Columns().AdjustToContents();
            ws.Rows().AdjustToContents();
        }

        private static bool IsQualiSession(ParsedRow r)
            => r.SessionType.Equals("ShortQ", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("Q1", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("Q2", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("Q3", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("OSQ", StringComparison.OrdinalIgnoreCase);

        private static bool IsRaceSession(ParsedRow r)
            => r.SessionType.Equals("Race", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("R2", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("R", StringComparison.OrdinalIgnoreCase);

        private static bool IsQualiNow()
            => ST.SessionType.Equals("ShortQ", StringComparison.OrdinalIgnoreCase)
            || ST.SessionType.Equals("Q1", StringComparison.OrdinalIgnoreCase)
            || ST.SessionType.Equals("Q2", StringComparison.OrdinalIgnoreCase)
            || ST.SessionType.Equals("Q3", StringComparison.OrdinalIgnoreCase)
            || ST.SessionType.Equals("OSQ", StringComparison.OrdinalIgnoreCase);

        private static bool IsRaceNow()
            => ST.SessionType.Equals("Race", StringComparison.OrdinalIgnoreCase)
            || ST.SessionType.Equals("R2", StringComparison.OrdinalIgnoreCase)
            || ST.SessionType.Equals("R", StringComparison.OrdinalIgnoreCase);

        private static void AddTyreDegradationXL(XLWorkbook wb, List<ParsedRow> allRows)
        {
            var raceData = allRows.Where(IsRaceSession).Where(r => r.WearAvg.HasValue).ToList();
            if (raceData.Count == 0) return;

            var ws = wb.AddWorksheet("Tyre Degradation");
            ulong sessionUid = raceData[0].SessionUID;

            // Get and sort drivers
            var drivers = raceData.Select(r => r.CarIdx).Distinct().ToList();

            if (ST.FinalOrder.TryGetValue((sessionUid, "Race"), out var order) && order.Count > 0)
            {
                var orderedDrivers = order.Where(car => drivers.Contains(car)).ToList();
                var missingDrivers = drivers.Except(orderedDrivers).OrderBy(car =>
                    ST.DriverDisplay.TryGetValue(car, out var d) ? d : $"Car {car}",
                    StringComparer.OrdinalIgnoreCase).ToList();
                drivers = orderedDrivers.Concat(missingDrivers).ToList();
            }
            else
            {
                drivers = drivers.OrderBy(car => ST.DriverDisplay.TryGetValue(car, out var d) ? d : $"Car {car}",
                                          StringComparer.OrdinalIgnoreCase).ToList();
            }

            // Group data by driver
            var byDriver = raceData.GroupBy(r => r.CarIdx)
                                   .ToDictionary(g => g.Key, g => g.OrderBy(x => x.Lap).ToList());

            // Detect stints for each driver
            var driverStints = new Dictionary<int, List<List<ParsedRow>>>();
            foreach (var carIdx in drivers)
            {
                if (!byDriver.TryGetValue(carIdx, out var laps)) continue;

                var stints = new List<List<ParsedRow>>();
                var currentStint = new List<ParsedRow> { laps[0] };

                for (int i = 1; i < laps.Count; i++)
                {
                    var prev = laps[i - 1];
                    var curr = laps[i];

                    bool compoundChanged = !string.Equals(curr.Tyre ?? "", prev.Tyre ?? "",
                                                          StringComparison.OrdinalIgnoreCase);
                    bool ageReset = curr.TyreAge >= 0 && prev.TyreAge >= 0 && curr.TyreAge < prev.TyreAge;
                    bool wearJump = curr.WearAvg.HasValue && prev.WearAvg.HasValue &&
                                   (curr.WearAvg.Value - prev.WearAvg.Value) < -5.0;

                    if (compoundChanged || ageReset || wearJump)
                    {
                        stints.Add(currentStint);
                        currentStint = new List<ParsedRow> { curr };
                    }
                    else
                    {
                        currentStint.Add(curr);
                    }
                }
                if (currentStint.Count > 0) stints.Add(currentStint);
                driverStints[carIdx] = stints;
            }

            int maxLap = raceData.Max(r => r.Lap);
            int currentCol = 1;

            ws.Range(1, 1, 2, 1).Merge();
            ws.Cell(1, 1).Value = "Lap";
            ws.Cell(1, 1).Style.Font.SetBold();
            ws.Cell(1, 1).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            ws.Cell(1, 1).Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

            currentCol = 2;
            foreach (var carIdx in drivers)
            {
                string driverName = ST.DriverDisplay.TryGetValue(carIdx, out var d) ? d : $"Car [{carIdx}]";

                ws.Range(1, currentCol, 1, currentCol + 3).Merge();
                var headerCell = ws.Cell(1, currentCol);
                headerCell.Value = driverName;
                headerCell.Style.Font.SetBold();
                headerCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;

                if (ST.TeamByCar.TryGetValue(carIdx, out var teamId))
                {
                    headerCell.Style.Fill.BackgroundColor = TeamColor(teamId);
                }

                currentCol += 4;
            }

            // Header Row 2: Column labels
            currentCol = 2;
            foreach (var carIdx in drivers)
            {
                ws.Cell(2, currentCol).Value = "Lap Time";
                ws.Cell(2, currentCol + 1).Value = "Delta";
                ws.Cell(2, currentCol + 2).Value = "Wear";
                ws.Cell(2, currentCol + 3).Value = "Mix";

                for (int i = 0; i < 4; i++)
                {
                    ws.Cell(2, currentCol + i).Style.Font.SetBold();
                    ws.Cell(2, currentCol + i).Style.Fill.BackgroundColor = XLColor.FromHtml("#E0E0E0");
                    ws.Cell(2, currentCol + i).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
                }

                currentCol += 4;
            }

            // Calculate representative lap for each stint
            var representativeLaps = new Dictionary<int, Dictionary<int, (double time, int index)>>();
            foreach (var carIdx in drivers)
            {
                if (!driverStints.TryGetValue(carIdx, out var stints)) continue;

                representativeLaps[carIdx] = new Dictionary<int, (double, int)>();

                for (int stintNum = 0; stintNum < stints.Count; stintNum++)
                {
                    var stint = stints[stintNum];
                    double? bestTime = null;
                    int bestIndex = -1;

                    for (int i = 0; i < Math.Min(3, stint.Count); i++)
                    {
                        if (string.IsNullOrEmpty(stint[i].ScStatus))
                        {
                            double lapTime = stint[i].LapTimeMs / 1000.0;
                            if (!bestTime.HasValue || lapTime < bestTime.Value)
                            {
                                bestTime = lapTime;
                                bestIndex = i;
                            }
                        }
                    }

                    if (!bestTime.HasValue && stint.Count > 0)
                    {
                        bestTime = stint[0].LapTimeMs / 1000.0;
                        bestIndex = 0;
                    }

                    if (bestTime.HasValue)
                    {
                        representativeLaps[carIdx][stintNum] = (bestTime.Value, bestIndex);
                    }
                }
            }

            // Fill data rows
            for (int lap = 1; lap <= maxLap; lap++)
            {
                int row = 2 + lap;

                // Lap number column
                ws.Cell(row, 1).Value = lap;
                ws.Cell(row, 1).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
                ws.Cell(row, 1).Style.Font.SetBold();

                currentCol = 2;
                foreach (var carIdx in drivers)
                {
                    if (!byDriver.TryGetValue(carIdx, out var laps) ||
                        !driverStints.TryGetValue(carIdx, out var stints))
                    {
                        currentCol += 4;
                        continue;
                    }

                    var lapData = laps.FirstOrDefault(r => r.Lap == lap);

                    if (lapData != null && lapData.WearAvg.HasValue)
                    {
                        // Find which stint this lap belongs to
                        int stintNum = -1;
                        int lapInStint = -1;
                        double? firstWearInStint = null;

                        for (int s = 0; s < stints.Count; s++)
                        {
                            var stint = stints[s];
                            var foundIndex = stint.FindIndex(r => r.Lap == lap);
                            if (foundIndex >= 0)
                            {
                                stintNum = s;
                                lapInStint = foundIndex;
                                firstWearInStint = stint[0].WearAvg;
                                break;
                            }
                        }

                        double lapTime = lapData.LapTimeMs / 1000.0;
                        double wear = lapData.WearAvg.Value;
                        string tyre = lapData.Tyre ?? "Unknown";

                        // Lap Time with tyre color background
                        var lapTimeCell = ws.Cell(row, currentCol);
                        lapTimeCell.Value = $"{MsToStr(lapData.LapTimeMs)} ({tyre})";

                        var tyreColor = GetTyreColor(tyre);
                        if (tyreColor != XLColor.NoColor)
                        {
                            lapTimeCell.Style.Fill.BackgroundColor = tyreColor;
                        }

                        // Delta to representative lap
                        if (stintNum >= 0 && representativeLaps[carIdx].TryGetValue(stintNum, out var repLap))
                        {
                            double delta = lapTime - repLap.time;
                            var deltaCell = ws.Cell(row, currentCol + 1);

                            if (lapInStint == repLap.index)
                            {
                                deltaCell.Value = "REF";
                                deltaCell.Style.Font.SetBold();
                                deltaCell.Style.Fill.BackgroundColor = XLColor.FromHtml("#9B3FF5");
                                deltaCell.Style.Font.SetFontColor(XLColor.White);
                            }
                            else if (Math.Abs(delta) < 0.001)
                            {
                                deltaCell.Value = "0.000s";
                                deltaCell.Style.Fill.BackgroundColor = XLColor.FromHtml("#E1E1E1");
                            }
                            else if (delta < 0)
                            {
                                deltaCell.Value = $"{delta:F3}s";
                                deltaCell.Style.Fill.BackgroundColor = XLColor.FromHtml("#40F57B");
                            }
                            else if (delta > 1.5)
                            {
                                deltaCell.Value = $"+{delta:F3}s";
                                deltaCell.Style.Fill.BackgroundColor = XLColor.FromHtml("#F56440");
                                deltaCell.Style.Font.SetFontColor(XLColor.White);
                            }
                            else if (delta > 0.8)
                            {
                                deltaCell.Value = $"+{delta:F3}s";
                                deltaCell.Style.Fill.BackgroundColor = XLColor.FromHtml("#F5A340");
                            }
                            else
                            {
                                deltaCell.Value = $"+{delta:F3}s";
                                deltaCell.Style.Fill.BackgroundColor = XLColor.FromHtml("#E1E1E1");
                            }

                            deltaCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
                        }

                        // Wear
                        var wearCell = ws.Cell(row, currentCol + 2);
                        double wearPercent = 100.0 - wear;
                        wearCell.Value = $"{wearPercent:F1}%";
                        wearCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;

                        // Цвет текста по уровню ИЗНОСА
                        var wearColors = new (int maxPercent, string hex)[]
                        {
                            (100, "#B6F2B6"),
                            (90, "#CFF7B0"),
                            (80, "#E8FCA8"),
                            (70, "#FFF7A1"),
                            (60, "#FFE89C"),
                            (50, "#FFD3A1"),
                            (40, "#FFBFA1"),
                            (30, "#FFA8A8"),
                            (20, "#FF8A8A"),
                            (10, "#D97A7A")
                        };

                        foreach (var (maxPercent, hex) in wearColors)
                        {
                            if (wearPercent <= maxPercent)
                            {
                                wearCell.Style.Fill.BackgroundColor = XLColor.FromHtml(hex);
                                break;
                            }
                        }

                        if (lapData.FuelMix.HasValue)
                        {
                            var mixCell = ws.Cell(row, currentCol + 3);
                            mixCell.Value = GetFuelMixName(lapData.FuelMix.Value);
                            mixCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;

                            var mixColor = GetFuelMixColor(lapData.FuelMix.Value);
                            if (mixColor != XLColor.NoColor)
                            {
                                mixCell.Style.Fill.BackgroundColor = mixColor;
                            }

                            // Добавляем комментарий с историей переключений
                            if (!string.IsNullOrEmpty(lapData.FuelMixHistory))
                            {
                                var comment = mixCell.CreateComment();
                                comment.AddText($"Mix changes:\n{lapData.FuelMixHistory}");
                            }
                        }

                        // Add wheel wear comment
                        if (lapData.FL.HasValue || lapData.FR.HasValue ||
                            lapData.RL.HasValue || lapData.RR.HasValue)
                        {
                            string fl = lapData.FL.HasValue ? $"{100 - lapData.FL.Value}%" : "?";
                            string fr = lapData.FR.HasValue ? $"{100 - lapData.FR.Value}%" : "?";
                            string rl = lapData.RL.HasValue ? $"{100 - lapData.RL.Value}%" : "?";
                            string rr = lapData.RR.HasValue ? $"{100 - lapData.RR.Value}%" : "?";

                            var comment = wearCell.CreateComment();
                            comment.AddText($"Wear:\nFL: {fl}  FR: {fr}\nRL: {rl}  RR: {rr}\nAge: {lapData.TyreAge} laps");
                        }
                    }

                    currentCol += 4;
                }
            }

            // Freeze panes: first column and first two rows
            ws.SheetView.Freeze(2, 1);

            // Column widths
            ws.Column(1).Width = 5;
            currentCol = 2;
            foreach (var carIdx in drivers)
            {
                ws.Column(currentCol).Width = 18;      // Lap Time
                ws.Column(currentCol + 1).Width = 9;   // Delta
                ws.Column(currentCol + 2).Width = 9;   // Wear
                ws.Column(currentCol + 3).Width = 9;   // Deg
                currentCol += 4;
            }
        }

        // ========== SC/VSC Windows ==========

        private static string GetScAt(ulong uid, double tSec)
        {
            if (!ST.ScWindows.TryGetValue(uid, out var list)) return "";
            for (int i = list.Count - 1; i >= 0; i--)
            {
                var w = list[i];
                var end = w.EndSec ?? double.MaxValue;
                if (tSec >= w.StartSec && tSec <= end) return w.Kind;
            }
            return "";
        }

        private static string GetScOverlap(ulong uid, double startSec, double endSec)
        {
            const double EPS = 0.001;
            if (!ST.ScWindows.TryGetValue(uid, out var list) || list.Count == 0) return "";
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

        private static void StartScWindow(ulong uid, string kind, double startSec, string srcTag)
        {
            var list = ST.ScWindows.GetOrAdd(uid, _ => new());

            // FIXED: Check for duplicate open windows
            if (list.Count > 0)
            {
                var last = list[^1];
                if (last.Kind == kind && last.EndSec is null)
                {
                    if (ST.DebugPackets)
                        Console.WriteLine($"[DBG] {srcTag} Ignoring duplicate {kind} start (already open since {last.StartSec:F3}s)");
                    return;
                }
            }

            list.Add(new ScWindow(kind, startSec, null));
            if (ST.DebugPackets)
                Console.WriteLine($"[DBG] {srcTag} Started {kind} window at {startSec:F3}s");
        }

        private static void EndScWindow(ulong uid, string kind, double endSec, string srcTag)
        {
            if (!ST.ScWindows.TryGetValue(uid, out var list)) return;

            for (int i = list.Count - 1; i >= 0; i--)
            {
                if (list[i].Kind == kind && list[i].EndSec is null)
                {
                    double duration = endSec - list[i].StartSec;
                    list[i] = list[i] with { EndSec = endSec };

                    if (ST.DebugPackets)
                        Console.WriteLine($"[DBG] {srcTag} Ended {kind} window at {endSec:F3}s (duration: {duration:F1}s)");
                    return;
                }
            }

            if (ST.DebugPackets)
                Console.WriteLine($"[DBG] {srcTag} Warning: Tried to end {kind} but no open window found");
        }

        private static void CloseOpenScWindows()
        {
            foreach (var kv in ST.ScWindows)
            {
                var list = kv.Value;
                for (int i = 0; i < list.Count; i++)
                {
                    if (list[i].EndSec is null)
                        list[i] = list[i] with { EndSec = double.MaxValue };
                }
            }
        }

        // ========== Helpers ==========

        private static string GetFuelMixName(byte mix) => mix switch
        {
            0 => "Lean",
            1 => "Std",
            2 => "Rich",
            3 => "Max",
            _ => ""
        };

        private static XLColor GetFuelMixColor(byte mix) => mix switch
        {
            0 => XLColor.FromHtml("#B8E6F5"), // Lean
            1 => XLColor.FromHtml("#E1E1E1"), // Std
            2 => XLColor.FromHtml("#FFE8B8"), // Rich
            3 => XLColor.FromHtml("#FFB8B8"), // Max
            _ => XLColor.NoColor
        };

        private static bool IsSectorValid(ParsedRow r, int sector)
        {
            if (ST.LapSectors.TryGetValue(r.SessionUID, out var byCar) &&
                byCar.TryGetValue(r.CarIdx, out var laps) &&
                laps.TryGetValue(r.Lap, out var rec))
            {
                return rec.LapValid;
            }

            if (ST.InvalidLaps.Contains((r.SessionUID, r.CarIdx, r.Lap)))
            {
                if (ST.DebugPackets)
                    Console.WriteLine($"[DBG] Lap {r.Lap} invalid for car {r.CarIdx} (penalty)");
                return false;
            }

            if (ST.LiveInvalidLaps.Contains((r.SessionUID, r.CarIdx, r.Lap)))
            {
                if (ST.DebugPackets)
                    Console.WriteLine($"[DBG] Lap {r.Lap} invalid for car {r.CarIdx} (live flag)");
                return false;
            }

            return true;
        }

        private static int SessionOrderKey(string s) => s.ToUpperInvariant() switch
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

        private static void UpdateProvisionalAgg(ulong uid, ParsedRow r)
        {
            var byCar = ST.Agg.GetOrAdd(uid, _ => new ConcurrentDictionary<int, State.DriverAgg>());
            var agg = byCar.GetOrAdd(r.CarIdx, _ => new State.DriverAgg());

            if (r.Lap > agg.Laps) agg.Laps = r.Lap;
            agg.TotalMs += r.LapTimeMs;
            if (r.LapTimeMs > 0 && r.LapTimeMs < agg.BestMs) agg.BestMs = r.LapTimeMs;

            List<int> order;
            string sessionTypeKey;

            if (IsRaceSession(r))
            {
                sessionTypeKey = "Race";
                order = byCar
                    .Select(kv => new { Car = kv.Key, A = kv.Value })
                    .OrderByDescending(x => x.A.Laps)
                    .ThenBy(x => x.A.TotalMs)
                    .Select(x => x.Car)
                    .ToList();
            }
            else if (IsQualiSession(r))
            {
                sessionTypeKey = "Quali";
                order = byCar
                    .Select(kv => new { Car = kv.Key, A = kv.Value })
                    .OrderBy(x => x.A.BestMs == int.MaxValue ? int.MaxValue : x.A.BestMs)
                    .Select(x => x.Car)
                    .ToList();
            }
            else return;

            ST.FinalOrder[(uid, sessionTypeKey)] = order;
        }

        private static readonly HashSet<string> _penaltyHeads = new(StringComparer.OrdinalIgnoreCase)
        {
            "Drive-through", "Stop-Go", "Grid penalty", "Time penalty",
            "Disqualified", "Removed from formation lap", "Parked too long timer",
            "Retired", "Black flag timer"
        };

        private static readonly HashSet<string> _warningHeads = new(StringComparer.OrdinalIgnoreCase)
        {
            "Penalty reminder", "Warning", "Tyre regulations",
            "Lap invalidated", "This & next lap invalid",
            "Lap invalid no reason", "This & next invalid no reason",
            "This & prev lap invalid", "This & prev invalid no reason"
        };

        private static string PenaltyCategory(string penalty)
        {
            if (string.IsNullOrWhiteSpace(penalty)) return "Other";

            foreach (var head in _penaltyHeads)
                if (penalty.StartsWith(head, StringComparison.OrdinalIgnoreCase))
                    return "Penalty";

            foreach (var head in _warningHeads)
                if (penalty.StartsWith(head, StringComparison.OrdinalIgnoreCase))
                    return "Warning";

            return "Other";
        }

        private static bool IsValidLap(ParsedRow r)
        {
            if (r == null) return false;
            if (r.LapTimeMs <= 0) return false;

            // FIXED: Check for unreasonably slow lap
            if (r.LapTimeMs > MAX_REASONABLE_LAP_MS)
            {
                if (ST.DebugPackets)
                    Console.WriteLine($"[DBG] Lap {r.Lap} for {r.DriverColumn} too slow ({MsToStr(r.LapTimeMs)}), excluding");
                return false;
            }

            const double EPS = 0.001;
            double start = r.SessTimeSec - (r.LapTimeMs / 1000.0);
            if (start < 0) start = 0;
            double end = r.SessTimeSec;

            // Check overlap with Safety Car / VSC
            if (ST.ScWindows.TryGetValue(r.SessionUID, out var winList) && winList.Count > 0)
            {
                foreach (var w in winList)
                {
                    var wEnd = w.EndSec ?? double.MaxValue;

                    bool overlaps = (start - EPS) <= (wEnd + EPS) && (end + EPS) >= (w.StartSec - EPS);

                    if (overlaps)
                    {
                        if (ST.DebugPackets)
                            Console.WriteLine($"[DBG] Lap {r.Lap} for {r.DriverColumn} overlaps with {w.Kind}, excluding");
                        return false;
                    }
                }
            }

            // FIXED: Check sector validity
            if (!IsSectorValid(r, 1) || !IsSectorValid(r, 2) || !IsSectorValid(r, 3))
            {
                if (ST.DebugPackets)
                    Console.WriteLine($"[DBG] Lap {r.Lap} for {r.DriverColumn} has invalid sector(s), excluding");
                return false;
            }

            return true;
        }

        private static List<ParsedRow> GetCleanLaps(IEnumerable<ParsedRow> rows)
        {
            var baseRows = rows.Where(IsValidLap)
                               .OrderBy(r => r.DriverColumn, StringComparer.OrdinalIgnoreCase)
                               .ThenBy(r => r.Lap)
                               .ThenBy(r => r.SessTimeSec)
                               .ToList();

            var cleaned = new List<ParsedRow>(baseRows.Count);

            foreach (var g in baseRows.GroupBy(r => r.DriverColumn, StringComparer.OrdinalIgnoreCase))
            {
                string prevTyre = "";
                int prevAge = -1;
                int prevLap = -1;
                ParsedRow? prev = null;

                foreach (var r in g)
                {
                    bool compoundChanged = !string.Equals(r.Tyre ?? "", prevTyre ?? "", StringComparison.OrdinalIgnoreCase);

                    bool ageKnownNow = r.TyreAge >= 0;
                    bool ageKnownPrev = prevAge >= 0;
                    bool ageReset = ageKnownNow && ageKnownPrev && (r.TyreAge < prevAge);

                    // FIXED: Additional check by tire wear
                    bool wearJump = false;
                    if (r.WearAvg.HasValue && prev?.WearAvg.HasValue == true)
                    {
                        double wearDiff = r.WearAvg.Value - prev.WearAvg.Value;

                        if (wearDiff < WEAR_JUMP_THRESHOLD) // < -5.0%
                        {
                            wearJump = true;
                            if (ST.DebugPackets)
                                Console.WriteLine($"[DBG] Pit stop detected for {g.Key} lap {r.Lap} (wear jump: {prev.WearAvg.Value:F1}% -> {r.WearAvg.Value:F1}%)");
                        }
                    }

                    bool pitStop = compoundChanged || ageReset || wearJump;

                    if (pitStop)
                    {
                        // Remove in-lap before pit stop
                        if (cleaned.Count > 0 && prev != null &&
                            string.Equals(cleaned[^1].DriverColumn, g.Key, StringComparison.OrdinalIgnoreCase) &&
                            cleaned[^1].Lap == prevLap)
                        {
                            if (ST.DebugPackets)
                                Console.WriteLine($"[DBG] Removing in-lap {prevLap} for {g.Key} before pit stop");
                            cleaned.RemoveAt(cleaned.Count - 1);
                        }

                        // Skip out-lap
                        if (ST.DebugPackets)
                            Console.WriteLine($"[DBG] Skipping out-lap {r.Lap} for {g.Key} after pit stop");

                        prevTyre = r.Tyre ?? "";
                        prevAge = r.TyreAge;
                        prevLap = r.Lap;
                        prev = r;
                        continue;
                    }

                    cleaned.Add(r);
                    prevTyre = r.Tyre ?? "";
                    prevAge = r.TyreAge;
                    prevLap = r.Lap;
                    prev = r;
                }
            }

            if (ST.DebugPackets)
                Console.WriteLine($"[DBG] GetCleanLaps: {baseRows.Count} base rows -> {cleaned.Count} clean rows");

            return cleaned;
        }

        private static void TrimWorksheet(IXLWorksheet ws)
        {
            var used = ws.RangeUsed();
            if (used == null) return;

            int lastRow = used.LastRow().RowNumber();
            int lastCol = used.LastColumn().ColumnNumber();

            ws.Rows(lastRow + 1, ws.Rows().Count()).Delete();
            ws.Columns(lastCol + 1, ws.Columns().Count()).Delete();
        }

        private static string GetOutputDir()
        {
            string outDir = Path.Combine(AppContext.BaseDirectory, "Logs", ST.AnchorLocalDate);
            Directory.CreateDirectory(outDir);
            return outDir;
        }

        private static void ReAnchorForNewWeekend(PacketHeader hdr, byte? trackIdHint = null)
        {
            ST.ScWindows.Clear();
            ST.SessionBuffers.Clear();
            ST.AnchorInitialized = true;
            ST.AnchorSessionUID = hdr.SessionUID;
            ST.AnchorLocalDate = DateTime.Now.ToString("yyyy-MM-dd");
            if (trackIdHint.HasValue) ST.AnchorTrackId = trackIdHint;
            ST.AnchorTrackName = trackIdHint.HasValue ? TrackName(trackIdHint.Value) : ST.AnchorTrackName;
            ST.LastLapNum.Clear();
            ST.LastLapTimeSeen.Clear();
            ST.WrittenLapCount.Clear();
            ST.LapStartSec.Clear();
            ST.DriverDisplay.Clear();
            ST.DriverRawName.Clear();
            ST.LastWear.Clear();
            ST.CurrentTyre.Clear();
            ST.MaxSpeedQuali.Clear();
            ST.MaxSpeedRace.Clear();
            ST.LastTyreAge.Clear();
            ST.Incidents.Clear();
            ST.Agg.Clear();
            ST.FinalOrder.Clear();
            ST.LapSectors.Clear();
            ST.LiveS1.Clear();
            ST.LiveS2.Clear();
            ST.LiveLapNo.Clear();
            ST.LiveSectorIdx.Clear();
            ST.LiveInvalidLaps.Clear();
            ST.BufferedSectors.Clear();
            ST.LastFuelMix.Clear();

            Console.WriteLine($"[i] New Weekend Anchor: Date={ST.AnchorLocalDate}, Track={ST.AnchorTrackName}, SessionUID={ST.AnchorSessionUID}");
        }

        private static XLColor GetTyreColor(string tyreCompound)
        {
            if (string.IsNullOrWhiteSpace(tyreCompound)) return XLColor.NoColor;
            switch (tyreCompound.Trim().ToUpperInvariant())
            {
                case "SOFT": return XLColor.FromHtml("#FFC0C0");
                case "MEDIUM": return XLColor.FromHtml("#FFFACD");
                case "HARD": return XLColor.FromHtml("#F5F5F5");
                case "INTER": return XLColor.FromHtml("#CCFFCC");
                case "WET": return XLColor.FromHtml("#CCE5FF");
                case "VSC":
                case "SC":
                    return XLColor.FromHtml("#1E1E1E");
                default: return XLColor.NoColor;
            }
        }

        private static XLColor TeamColor(byte teamId)
        {
            return teamId switch
            {
                0 => XLColor.FromHtml("#A6EDE6"), // Mercedes
                1 => XLColor.FromHtml("#F5A3A3"), // Ferrari
                2 => XLColor.FromHtml("#A8B6FF"), // Red Bull
                3 => XLColor.FromHtml("#AFD8FA"), // Williams
                4 => XLColor.FromHtml("#F9CFE2"), // Racing Point
                5 => XLColor.FromHtml("#FFE7A8"), // Renault
                6 => XLColor.FromHtml("#9BB8CC"), // AlphaTauri
                7 => XLColor.FromHtml("#CFCFCF"), // Haas
                8 => XLColor.FromHtml("#FFC999"), // McLaren
                9 => XLColor.FromHtml("#E6A3A3"), // Alfa Romeo
                10 => XLColor.FromHtml("#9BB8CC"), // Toro Rosso
                11 => XLColor.FromHtml("#F9CFE2"), // Racing Point
                12 => XLColor.FromHtml("#AFD8FA"), // Williams
                _ => XLColor.FromHtml("#F5F5F5")  // Unknown / F2 / др.
            };
        }

        private static int? TryExtractCarIndexFromDisplay(string display)
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

        private static void FixCsvDriverNames(string csvPath)
        {
            try
            {
                if (!File.Exists(csvPath)) return;

                var map = ST.DriverDisplay.ToDictionary(kv => kv.Key, kv => kv.Value);
                if (map.Count == 0) return;

                var tmp = csvPath + ".tmp";
                using var sr = new StreamReader(csvPath, Encoding.UTF8);
                using var sw = new StreamWriter(tmp, false, Encoding.UTF8);

                string? header = sr.ReadLine();
                if (header is null) return;
                sw.WriteLine(header);

                string? line;
                while ((line = sr.ReadLine()) != null)
                {
                    var parts = SplitCsv(line);
                    if (parts.Length < 19) { sw.WriteLine(line); continue; }

                    if (int.TryParse(parts[2], out int carIdx) && map.TryGetValue(carIdx, out var display))
                    {
                        var drvCol = parts[3].Trim('"');
                        if (drvCol.Equals($"Unknown [{carIdx}]", StringComparison.OrdinalIgnoreCase))
                        {
                            parts[3] = Csv(display);

                            var raw = parts[4].Trim('"');
                            if (string.IsNullOrWhiteSpace(raw) || raw.Equals("Unknown", StringComparison.OrdinalIgnoreCase))
                            {
                                var p2 = display.LastIndexOf('[');
                                var rawName = p2 > 0 ? display.Substring(0, p2).Trim() : display;
                                parts[4] = Csv(rawName);
                            }

                            sw.WriteLine(string.Join(",", parts));
                            continue;
                        }
                    }
                    sw.WriteLine(line);
                }

                sr.Close(); sw.Flush(); sw.Close();

                File.Copy(tmp, csvPath, overwrite: true);
                File.Delete(tmp);
            }
            catch (Exception ex)
            {
                Console.BackgroundColor = ConsoleColor.Red;
                Console.WriteLine("[warning] FixCsvDriverNames: " + ex.Message);
                Console.ResetColor();
            }
        }

        private static void FixCsvBarePlayerAi(string csvPath)
        {
            try
            {
                if (!File.Exists(csvPath)) return;

                var tmp = csvPath + ".tmp2";
                using var sr = new StreamReader(csvPath, Encoding.UTF8);
                using var sw = new StreamWriter(tmp, false, Encoding.UTF8);

                string? header = sr.ReadLine();
                if (header is null) return;
                sw.WriteLine(header);

                string? line;
                while ((line = sr.ReadLine()) != null)
                {
                    var parts = SplitCsv(line);

                    if (parts.Length < 19) { sw.WriteLine(line); continue; }
                    if (!int.TryParse(parts[2], out int carIdx)) { sw.WriteLine(line); continue; }

                    string drvCol = parts[3].Trim('"');
                    string raw = parts[4].Trim('"');

                    bool isBarePlayerOrAi =
                        drvCol.Equals("Player", StringComparison.OrdinalIgnoreCase) ||
                        drvCol.Equals("AI", StringComparison.OrdinalIgnoreCase);

                    bool hasIndex = drvCol.EndsWith("]") && drvCol.Contains('[');

                    if (isBarePlayerOrAi && !hasIndex)
                    {
                        string display = ST.DriverDisplay.TryGetValue(carIdx, out var disp)
                                         ? disp
                                         : $"{drvCol} [{carIdx}]";

                        parts[3] = Csv(display);

                        if (string.IsNullOrWhiteSpace(raw))
                        {
                            var p2 = display.LastIndexOf('[');
                            var rawName = p2 > 0 ? display.Substring(0, p2).Trim() : display;
                            parts[4] = Csv(rawName);
                        }

                        sw.WriteLine(string.Join(",", parts));
                    }
                    else
                    {
                        sw.WriteLine(line);
                    }
                }

                sr.Close(); sw.Flush(); sw.Close();
                File.Copy(tmp, csvPath, overwrite: true);
                File.Delete(tmp);
            }
            catch (Exception ex)
            {
                Console.BackgroundColor = ConsoleColor.Red;
                Console.WriteLine("[warning] FixCsvBarePlayerAi: " + ex.Message);
                Console.ResetColor();
            }
        }

        private static void NormalizeDriverNames(List<ParsedRow> rows)
        {
            if (rows == null || rows.Count == 0) return;

            for (int i = 0; i < rows.Count; i++)
            {
                var r = rows[i];
                string col = r.DriverColumn;

                if (col.Equals($"Unknown [{r.CarIdx}]", StringComparison.OrdinalIgnoreCase)
                    && ST.DriverDisplay.TryGetValue(r.CarIdx, out var displayName))
                {
                    var p = displayName.LastIndexOf('[');
                    var raw = p > 0 ? displayName.Substring(0, p).Trim() : displayName;
                    rows[i] = r with
                    {
                        DriverColumn = displayName,
                        DriverRaw = string.IsNullOrWhiteSpace(r.DriverRaw) ? raw : r.DriverRaw
                    };
                    continue;
                }

                bool isBare = col.Equals("Player", StringComparison.OrdinalIgnoreCase) ||
                              col.Equals("AI", StringComparison.OrdinalIgnoreCase);
                bool hasIdx = col.EndsWith("]") && col.Contains('[');

                if (isBare && !hasIdx)
                {
                    string baseName = col;
                    string dispName = $"{baseName} [{r.CarIdx}]";

                    if (ST.DriverDisplay.TryGetValue(r.CarIdx, out var betterName))
                        dispName = betterName;

                    string raw = string.IsNullOrWhiteSpace(r.DriverRaw) ? baseName : r.DriverRaw;

                    rows[i] = r with { DriverColumn = dispName, DriverRaw = raw };
                }
            }
        }

        private static string SessionTypeName(byte s) => s switch
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

        private static string PacketName(byte id) => id switch
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

        private static string TrackName(byte trackId) => TrackNames.TryGetValue(trackId, out var n) ? n : $"Track_{trackId}";

        private static string DecodeName(ReadOnlySpan<byte> nameBytes)
        {
            int len = nameBytes.IndexOf((byte)0);
            if (len < 0) len = nameBytes.Length;
            if (len <= 0) return "Unknown";

            string s = Encoding.UTF8.GetString(nameBytes[..len]);

            var sb = new StringBuilder(s.Length);
            foreach (var ch in s) if (!char.IsControl(ch)) sb.Append(ch);
            s = sb.ToString().Trim();

            if (string.IsNullOrWhiteSpace(s))
            {
                s = Encoding.GetEncoding("ISO-8859-1").GetString(nameBytes[..len]).Trim();
                if (string.IsNullOrWhiteSpace(s)) s = "Unknown";
            }
            if (s.Length > 64) s = s[..64];
            return s;
        }

        private static bool IsPct(int v) => v >= 0 && v <= 100;

        private static string TyreName(byte actual, byte visual)
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

        private static string Csv(string s)
        {
            if (string.IsNullOrEmpty(s)) return "";
            if (s.Contains(',') || s.Contains('"')) return "\"" + s.Replace("\"", "\"\"") + "\"";
            return s;
        }

        private static string MsToStr(int ms)
        {
            if (ms <= 0) return "";
            double s = ms / 1000.0;
            int m = (int)(s / 60);
            s -= m * 60;
            return $"{m}:{s:00.000}";
        }

        private static int TyreOrder(string tyre) => tyre switch
        {
            "Soft" => 0,
            "Medium" => 1,
            "Hard" => 2,
            "Inter" => 3,
            "Wet" => 4,
            _ => 99
        };

        private static ParsedRow? ParseCsvRow(string line)
        {
            var p = SplitCsv(line);
            if (p == null || p.Length != 21) return null;

            return new ParsedRow(
                SessionUID: ulong.Parse(p[0]),
                SessionType: p[1],
                CarIdx: int.Parse(p[2]),
                DriverColumn: p[3],
                DriverRaw: p[4],
                Lap: int.Parse(p[5]),
                Tyre: p[6],
                LapTimeMs: int.Parse(p[7]),
                WearAvg: string.IsNullOrWhiteSpace(p[8]) ? (double?)null : double.Parse(p[8], CultureInfo.InvariantCulture),
                RL: string.IsNullOrWhiteSpace(p[9]) ? (int?)null : int.Parse(p[9], CultureInfo.InvariantCulture),
                RR: string.IsNullOrWhiteSpace(p[10]) ? (int?)null : int.Parse(p[10], CultureInfo.InvariantCulture),
                FL: string.IsNullOrWhiteSpace(p[11]) ? (int?)null : int.Parse(p[11], CultureInfo.InvariantCulture),
                FR: string.IsNullOrWhiteSpace(p[12]) ? (int?)null : int.Parse(p[12], CultureInfo.InvariantCulture),
                SessTimeSec: double.TryParse(p[13], NumberStyles.Any, CultureInfo.InvariantCulture, out var st) ? st : 0.0,
                ScStatus: p[14],
                TyreAge: int.TryParse(p[15], out var age) ? age : -1,
                S1Ms: int.TryParse(p[16], out var s1) ? s1 : 0,
                S2Ms: int.TryParse(p[17], out var s2) ? s2 : 0,
                S3Ms: int.TryParse(p[18], out var s3) ? s3 : 0,
                FuelMix: string.IsNullOrWhiteSpace(p[19]) ? (byte?)null : byte.Parse(p[19]),
                FuelMixHistory: p.Length > 20 ? p[20] : null
            );
        }

        private static string[] SplitCsv(string line)
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
                        if (i + 1 < line.Length && line[i + 1] == '"') { sb.Append('"'); i++; }
                        else { quoted = false; }
                    }
                    else sb.Append(ch);
                }
                else
                {
                    if (ch == ',') { res.Add(sb.ToString()); sb.Clear(); }
                    else if (ch == '"') { quoted = true; }
                    else sb.Append(ch);
                }
            }
            res.Add(sb.ToString());
            return res.ToArray();
        }

        private static List<(int pid, string name)> FindUdpOwners(int port)
        {
            var result = new List<(int pid, string name)>();
            try
            {
                var psi = new System.Diagnostics.ProcessStartInfo
                {
                    FileName = "cmd.exe",
                    Arguments = "/c netstat -ano -p udp",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };
                using var proc = System.Diagnostics.Process.Start(psi)!;
                string output = proc.StandardOutput.ReadToEnd();
                proc.WaitForExit();

                var owners = new HashSet<int>();
                foreach (var raw in output.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    if (!raw.Contains("UDP", StringComparison.OrdinalIgnoreCase)) continue;
                    var needle = $":{port}";
                    if (!raw.Contains(needle)) continue;

                    var parts = raw.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length < 4) continue;

                    if (int.TryParse(parts[^1], out int pid) && pid > 0) owners.Add(pid);
                }

                foreach (var pid in owners)
                {
                    string name;
                    try { name = System.Diagnostics.Process.GetProcessById(pid).ProcessName; }
                    catch { name = "Unknown"; }
                    result.Add((pid, name));
                }
            }
            catch { }
            return result;
        }

        // ========= Models / State =========

        private sealed record PacketHeader(
            ushort PacketFormat, byte GameMajor, byte GameMinor, byte PacketVersion, byte PacketId,
            ulong SessionUID, float SessionTime, uint FrameIdentifier, byte PlayerCarIndex, byte SecondaryPlayerCarIndex)
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

        private sealed class State
        {
            public Dictionary<int, byte> LastFuelMix { get; } = new();
            public Dictionary<int, List<(byte mix, float timestamp)>> FuelMixHistory { get; } = new();
            public Dictionary<int, SectorBuffer> BufferedSectors { get; } = new();
            public sealed record SectorBuffer(int S1, int S2, int LapNum, ulong SessionUID);
            public Dictionary<int, int> LiveS1 { get; } = new();
            public Dictionary<int, int> LiveS2 { get; } = new();
            public Dictionary<int, int> LiveLapNo { get; } = new();
            public Dictionary<int, int> LiveSectorIdx { get; } = new();
            public HashSet<(ulong uid, int car, int lap)> InvalidLaps { get; } = new();
            public HashSet<(ulong uid, int car, int lap)> LiveInvalidLaps { get; } = new();
            public bool AnchorInitialized { get; set; } = false;
            public ulong AnchorSessionUID { get; set; } = 0;
            public string AnchorLocalDate { get; set; } = DateTime.Now.ToString("yyyy-MM-dd");
            public string AnchorTrackName { get; set; } = "UnknownTrack";
            public DateTime LastFinalSaveUtc { get; set; } = DateTime.MinValue;
            public byte? AnchorTrackId { get; set; } = null;
            public string SessionType { get; set; } = "Unknown";
            public byte CurrentSC { get; set; } = 0;
            public byte? LastSCFromSession { get; set; } = null;
            public byte? TrackId { get; set; } = null;
            public ConcurrentDictionary<ulong, ConcurrentQueue<ParsedRow>> SessionBuffers { get; } = new();
            public ConcurrentDictionary<ulong, List<ScWindow>> ScWindows { get; } = new();
            public Dictionary<int, string> DriverDisplay { get; } = new();
            public Dictionary<int, string> DriverRawName { get; } = new();
            public Dictionary<int, byte> TeamByCar { get; } = new();
            public Dictionary<int, int> LastLapNum { get; } = new();
            public Dictionary<int, float> LastLapTimeSeen { get; } = new();
            public Dictionary<int, int> WrittenLapCount { get; } = new();
            public Dictionary<int, WearInfo> LastWear { get; } = new();
            public Dictionary<int, string> CurrentTyre { get; } = new();
            public Dictionary<int, int> LastTyreAge { get; } = new();
            public Dictionary<int, double> LapStartSec { get; } = new();
            public ConcurrentDictionary<ulong, string> SessionTypeByUid { get; } = new();
            public Dictionary<int, int> MaxSpeedQuali { get; } = new();
            public Dictionary<int, int> MaxSpeedRace { get; } = new();
            public ConcurrentDictionary<ulong,
                ConcurrentDictionary<int, Dictionary<int, SectorRec>>> LapSectors
            { get; } = new();
            public sealed record SectorRec(int S1, int S2, int S3, bool V1, bool V2, bool V3, bool LapValid);
            public Dictionary<(ulong uid, string sessionType), List<int>> FinalOrder { get; } = new();
            public ConcurrentDictionary<ulong, List<IncidentRow>> Incidents { get; } = new();
            public sealed class DriverAgg
            {
                public int Laps;
                public long TotalMs;
                public int BestMs = int.MaxValue;
            }
            public ConcurrentDictionary<ulong, ConcurrentDictionary<int, DriverAgg>> Agg { get; } = new();
            public bool DebugPackets { get; set; } = false;
            public readonly int[] PidCounters = new int[64];
            public bool AutoSaveEnabled { get; set; } = true;
            private ulong? _lastUid = null;
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

        private sealed record WearInfo(int RL, int RR, int FL, int FR)
        {
            public double Avg => Math.Round((RL + RR + FL + FR) / 4.0, 1);
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

        private sealed record IncidentRow(
            int Lap,
            int CarIdx,
            string Accident,
            string Penalty,
            int Seconds,
            int PlacesGained,
            int OtherCar,
            double SessTimeSec
        );

        private sealed record ScWindow(string Kind, double StartSec, double? EndSec);

        private static readonly Dictionary<byte, string> TrackNames = new()
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

        private static string PenaltyName(byte id) => id switch
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

        private static string InfringementName(byte id) => id switch
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
    }
}