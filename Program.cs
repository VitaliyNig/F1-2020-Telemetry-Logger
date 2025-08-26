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
using OfficeOpenXml;
using OfficeOpenXml.Style;
using ClosedXML.Excel;

namespace F12020TelemetryLogger
{
    internal static class Program
    {
        // ========= Protocol / runtime =========
        private const int HEADER_SIZE = 24;
        private const int MAX_CARS = 22;
        private const int UDP_PORT_DEFAULT = 20777;

        private const int LAPDATA_STRIDE = 53;
        private const int PARTICIPANT_STRIDE = 54;
        private const int CARSTATUS_STRIDE = 60;
        private const int CARTELEM_STRIDE = 58;

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

            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

            int port = UDP_PORT_DEFAULT;
            if (args is { Length: > 0 } && int.TryParse(args[0], out var p) && p is > 0 and < 65536)
                port = p;

            PrintBanner();
            PrintMenu();

            // Create UDP with SO_REUSEADDR + сообщение кто держит порт
            try
            {
                _udp = new UdpClient(AddressFamily.InterNetwork);
                _udp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                _udp.Client.Bind(new IPEndPoint(IPAddress.Any, port));
                _udp.Client.ReceiveTimeout = 50;
                _udp.Client.ReceiveBufferSize = 1 << 20;
                Console.WriteLine($"[i] UDP listening on: {port} (ReuseAddress=ON)");
            }
            catch (SocketException se) when (se.SocketErrorCode == SocketError.AddressAlreadyInUse || se.SocketErrorCode == SocketError.AccessDenied)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"❌ Порт {port} недоступен: {se.Message}");
                Console.ResetColor();

                var owners = FindUdpOwners(port);
                if (owners.Count > 0)
                {
                    Console.WriteLine("Кто держит порт: ");
                    foreach (var (pid, name) in owners)
                        Console.WriteLine($"  • {name} (PID {pid})");
                }
                else
                {
                    Console.WriteLine("Не удалось определить процесс, занявший порт.");
                }

                Console.WriteLine();
                Console.WriteLine($"→ Закройте приложение выше или запустите логгер на другом порту, например:");
                Console.WriteLine($"   dotnet run -- {port + 1}");
                Console.WriteLine($"   telemetry.exe {port + 1}");
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

                        AnchorsInitialization(hdr);

                        switch (hdr.PacketId)
                        {
                            case 1: ParseSession(buf, hdr); break;
                            case 2: ParseLapData(buf, hdr); break;
                            case 3: ParseEvent(buf, hdr); break;
                            case 4: ParseParticipants(buf, hdr); break;
                            case 6: ParseCarTelemetry(buf, hdr); break;
                            case 7: ParseCarStatus(buf, hdr); break;
                            case 8: ST.PendingFinalSave = true; break;
                            default: break;
                        }

                        if (ST.PendingFinalSave && !_saving)
                        {
                            ST.PendingFinalSave = false;
                            CloseOpenScWindows();
                            await SaveAllAsync(makeExcel: true);
                            Console.WriteLine("[✓] Session finished — saved CSV/XLSX.");
                        }

                        if (ST.AutoSaveEnabled && DateTime.UtcNow - _lastAutoSaveUtc >= AutoSaveEvery && !_saving)
                        {
                            await SaveAllAsync(makeExcel: false);
                            _lastAutoSaveUtc = DateTime.UtcNow;
                            Console.WriteLine("[i] Auto-saved CSV.");
                        }
                    }
                    catch (SocketException) { }
                    catch (ObjectDisposedException) { break; }
                    catch (Exception ex)
                    {
                        Console.WriteLine("[warn] " + ex.Message);
                    }
                }
            }
            finally
            {
                await FinalizeAndExitAsync("Main loop end");
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
                        Console.WriteLine("[i] Saved (manual).");
                        break;

                    case ConsoleKey.D2:
                    case ConsoleKey.NumPad2:
                        PrintBuffer();
                        break;

                    case ConsoleKey.D3:
                    case ConsoleKey.NumPad3:
                        ST.AutoSaveEnabled = !ST.AutoSaveEnabled;
                        Console.WriteLine(ST.AutoSaveEnabled ? "[i] Auto-save: ON" : "[i] Auto-save: OFF");
                        break;

                    case ConsoleKey.D4:
                    case ConsoleKey.NumPad4:
                        TryOpenFolder();
                        break;

                    case ConsoleKey.D0:
                    case ConsoleKey.NumPad0:
                        TriggerExit("Menu Exit");
                        return;

                    case ConsoleKey.N:
                        var fakeHdr = new PacketHeader(0, 0, 0, 0, 0, ST.AnchorSessionUID + 1, 0f, 0, 0, 0);
                        ReAnchorForNewWeekend(fakeHdr, ST.TrackId);
                        Console.WriteLine("[i] Started new session (manual).");
                        break;
                }
            }
        }

        private static void PrintBanner()
        {
            Console.WriteLine("[i] F1 2020 Telemetry Logger v5");
        }

        private static void PrintMenu()
        {
            Console.WriteLine("\n    [1] Save now (+Excel)\n    [2] Buffer\n    [3] Auto-save ON/OFF\n    [4] Open folder\n    [M] Menu\n    [N] New weekend\n    [0] Exit\n");
        }

        private static void PrintBuffer()
        {
            int total = ST.SessionBuffers.Sum(kv => kv.Value.Count);
            Console.WriteLine($"[i] Buffered rows: {total}");
            foreach (var kv in ST.SessionBuffers.OrderBy(k => k.Key))
            {
                var uid = kv.Key;
                var list = kv.Value;
                var byDrv = list.GroupBy(r => r.DriverColumn).OrderBy(g => g.Key, StringComparer.OrdinalIgnoreCase);
                Console.WriteLine($"  Session {uid}: rows={list.Count}, drivers={byDrv.Count()}");
                foreach (var g in byDrv)
                {
                    int laps = g.Select(x => x.Lap).Distinct().Count();
                    var avg = g.Average(x => x.LapTimeMs);
                    Console.WriteLine($"    {g.Key}  laps={laps,-3} avg={MsToStr((int)Math.Round(avg))}");
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
            }
            catch (Exception ex) { Console.WriteLine("[warn] open folder: " + ex.Message); }
        }

        private static void TriggerExit(string reason)
        {
            if (!_running) return;
            _running = false;
            Console.WriteLine($"[i] Exiting… ({reason})");
            _ = Task.Run(async () => await FinalizeAndExitAsync(from: reason));
        }

        private static async Task FinalizeAndExitAsync(string from)
        {
            try
            {
                CloseOpenScWindows();
                await SaveAllAsync(makeExcel: true);
            }
            catch (Exception ex)
            {
                Console.WriteLine("[warn] Final save failed: " + ex.Message);
            }

            try { _udp?.Close(); _udp?.Dispose(); } catch { }
            try { _cts?.Cancel(); _cts?.Dispose(); } catch { }

            Console.WriteLine("[i] Bye.");
            Environment.Exit(0);
        }

        // ========== Packet parsing ==========

        private static void AnchorsInitialization(PacketHeader hdr)
        {
            if (ST.AnchorInitialized) return;
            ST.AnchorInitialized = true;
            ST.AnchorSessionUID = hdr.SessionUID;
            ST.AnchorLocalDate = DateTime.Now.ToString("yyyy-MM-dd");
            if (ST.TrackId.HasValue) ST.AnchorTrackId = ST.TrackId;
            if (string.IsNullOrEmpty(ST.AnchorTrackName)) ST.AnchorTrackName = "UnknownTrack";
            Console.WriteLine($"[i] Anchor: date={ST.AnchorLocalDate}, sessionUID={ST.AnchorSessionUID}");
        }

        private static void ParseCarTelemetry(ReadOnlySpan<byte> buf, PacketHeader hdr)
        {
            int off = HEADER_SIZE;
            if (off + CARTELEM_STRIDE * MAX_CARS > buf.Length) return;

            bool isQuali = IsQualiNow();
            bool isRace = IsRaceSession(new ParsedRow(0, hdr.SessionUID, ST.SessionType, 0, "", "", 0, "", 0, true, null, null, null, null, null));

            if (!isQuali && !isRace) return;

            for (int car = 0; car < MAX_CARS; car++)
            {
                int start = off + car * CARTELEM_STRIDE;
                if (start + 2 > buf.Length) break;

                ushort speed = BinaryPrimitives.ReadUInt16LittleEndian(buf.Slice(start, 2));
                if (speed == 0) continue;

                if (isQuali)
                {
                    if (!ST.MaxSpeedQuali.TryGetValue(car, out int cur) || speed > cur)
                        ST.MaxSpeedQuali[car] = speed;
                }
                else if (isRace)
                {
                    if (!ST.MaxSpeedRace.TryGetValue(car, out int cur) || speed > cur)
                        ST.MaxSpeedRace[car] = speed;
                }
            }
        }

        private static void ParseSession(ReadOnlySpan<byte> buf, PacketHeader hdr)
        {
            int off = HEADER_SIZE;

            try
            {
                byte? trackIdNow = null;
                {
                    int tmp = HEADER_SIZE;
                    // weather(1), trackTemp(1), airTemp(1), totalLaps(1), trackLength(2)
                    tmp += 1 + 1 + 1 + 1 + 2;

                    // sessionType (1)
                    if (tmp < buf.Length) tmp += 1;

                    // trackId (1)
                    if (tmp < buf.Length) trackIdNow = buf[tmp];
                }

                bool trackChanged = trackIdNow.HasValue && ST.AnchorTrackId.HasValue && trackIdNow.Value != ST.AnchorTrackId.Value;

                if (trackChanged)
                {
                    ReAnchorForNewWeekend(hdr, trackIdNow);
                }
            }
            catch { }

            // ---------- Основной парсинг Session ----------

            // weather(1), trackTemp(1), airTemp(1), totalLaps(1), trackLength(2)
            off += 1 + 1 + 1 + 1 + 2;

            // sessionType (1)
            if (off < buf.Length)
            {
                byte stype = buf[off]; off++;
                ST.SessionType = SessionTypeName(stype);
            }

            // trackId (1)
            if (off < buf.Length)
            {
                byte tId = buf[off]; off++;
                ST.TrackId = tId;

                if (!ST.AnchorTrackId.HasValue) ST.AnchorTrackId = tId;

                ST.AnchorTrackName = TrackName(tId);
            }

            // formula(1) sessionTimeLeft(2) sessionDuration(2) pitSpeedLimit(1) gamePaused(1) isSpectating(1) spectatorCarIndex(1) sliProNativeSupport(1)
            off += 1 + 2 + 2 + 1 + 1 + 1 + 1 + 1;

            // marshal zones (numZones(1) + 21 * 5 bytes)
            if (off < buf.Length) off += 1 + 21 * 5;

            // safetyCarStatus (1) — 0 none, 1 SC, 2 VSC, 3 formation lap
            if (off < buf.Length)
            {
                byte sc = buf[off];

                if (sc <= 3) ST.CurrentSC = sc;
                if (ST.LastSCFromSession is null) ST.LastSCFromSession = sc;
                else if (ST.LastSCFromSession != sc)
                {
                    var prev = ST.LastSCFromSession.Value;

                    // Закрываем/открываем окна SC/VSC по смене статуса
                    if (prev == 1 && sc != 1) EndScWindow(hdr.SessionUID, "SC", hdr.SessionTime, "[Session]");
                    if (prev != 1 && sc == 1) StartScWindow(hdr.SessionUID, "SC", hdr.SessionTime, "[Session]");

                    if ((prev == 2 || prev == 3) && sc != 2) EndScWindow(hdr.SessionUID, "VSC", hdr.SessionTime, "[Session]");
                    if (sc == 2 && prev != 2) StartScWindow(hdr.SessionUID, "VSC", hdr.SessionTime, "[Session]");

                    ST.LastSCFromSession = sc;
                }
                off++;
            }

            ST.OnPotentialNewSession(hdr.SessionUID);
        }

        private static void ParseEvent(ReadOnlySpan<byte> buf, PacketHeader hdr)
        {
            if (buf.Length < HEADER_SIZE + 4) return;
            string code = Encoding.ASCII.GetString(buf.Slice(HEADER_SIZE, 4));

            switch (code)
            {
                case "SSTA": break;
                case "SEND":
                    ST.PendingFinalSave = true;
                    ST.LastSessionClosed = true;
                    Console.WriteLine("[EVT] Session ended");
                    break;
                case "SCDP":
                    ST.CurrentSC = 1;
                    StartScWindow(hdr.SessionUID, "SC", hdr.SessionTime, "[Event]");
                    Console.WriteLine("[EVT] SC deployed");
                    break;
                case "SCDE":
                    ST.CurrentSC = 0;
                    EndScWindow(hdr.SessionUID, "SC", hdr.SessionTime, "[Event]");
                    Console.WriteLine("[EVT] SC ended");
                    break;
                case "VSCN":
                    ST.CurrentSC = 2;
                    StartScWindow(hdr.SessionUID, "VSC", hdr.SessionTime, "[Event]");
                    Console.WriteLine("[EVT] VSC on");
                    break;
                case "VSCF":
                    ST.CurrentSC = 3;
                    EndScWindow(hdr.SessionUID, "VSC", hdr.SessionTime, "[Event]");
                    Console.WriteLine("[EVT] VSC finished");
                    break;
                default:
                    break;
            }
        }

        private static void ParseParticipants(ReadOnlySpan<byte> buf, PacketHeader hdr)
        {
            int off = HEADER_SIZE;
            if (buf.Length < off + 1) return;
            int numActive = buf[off]; off += 1;

            // Каждый блок участника — 54 байта (см. PARTICIPANT_STRIDE)
            for (int car = 0; car < Math.Min(numActive, MAX_CARS); car++)
            {
                int start = off + car * PARTICIPANT_STRIDE;
                if (start + PARTICIPANT_STRIDE > buf.Length) break;

                byte aiControlled = buf[start + 0];            // 1=AI, 0=Human
                byte driverId = buf[start + 1];             // не используем
                byte teamId = buf[start + 2];
                byte raceNumber = buf[start + 3];             // не используем
                byte nationality = buf[start + 4];             // не используем

                // name[48] ASCII
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

                foreach (var session in ST.SessionBuffers)
                {
                    var list = session.Value;
                    for (int i = 0; i < list.Count; i++)
                    {
                        var r = list[i];
                        if (r.CarIdx != car) continue;

                        bool isUnknownPlaceholder = r.DriverColumn.Equals($"Unknown [{car}]", StringComparison.OrdinalIgnoreCase);
                        bool isBarePlayerOrAI = r.DriverColumn.Equals("Player", StringComparison.OrdinalIgnoreCase)
                                                || r.DriverColumn.Equals("AI", StringComparison.OrdinalIgnoreCase);

                        if (isUnknownPlaceholder || isBarePlayerOrAI)
                            list[i] = r with { DriverColumn = display, DriverRaw = name };
                    }
                }
            }
        }

        private static void ParseCarStatus(ReadOnlySpan<byte> buf, PacketHeader hdr)
        {
            int off = HEADER_SIZE;
            if (off + CARSTATUS_STRIDE * MAX_CARS > buf.Length) return;

            for (int car = 0; car < MAX_CARS; car++)
            {
                int start = off + car * CARSTATUS_STRIDE;
                var block = buf.Slice(start, CARSTATUS_STRIDE);

                // Wear approx RL,RR,FL,FR @ 25..28
                int rl = block[25], rr = block[26], fl = block[27], fr = block[28];
                if (IsPct(rl) && IsPct(rr) && IsPct(fl) && IsPct(fr))
                    ST.LastWear[car] = new WearInfo(rl, rr, fl, fr);

                // tyre compound: actual(29), visual(30)
                string tyre = TyreName(block[29], block[30]);
                ST.CurrentTyre[car] = tyre;

                // NEW: возраст комплекта шин (круги) @ 31
                // В F1 2020 это uint8; иногда может быть 255/неизвестно.
                int age = block.Length > 31 ? block[31] : -1;
                if (age >= 0 && age <= 200) // простая фильтрация мусора
                    ST.LastTyreAge[car] = age;
                else
                    ST.LastTyreAge[car] = -1; // неизвестно
            }
        }

        private static void ParseLapData(ReadOnlySpan<byte> buf, PacketHeader hdr)
        {
            int off = HEADER_SIZE;
            if (off + LAPDATA_STRIDE * MAX_CARS > buf.Length) return;

            for (int car = 0; car < MAX_CARS; car++)
            {
                int start = off + car * LAPDATA_STRIDE;

                float lastLapSec = BitConverter.ToSingle(buf.Slice(start + 0, 4));
                if (!(lastLapSec > 0.0001f)) continue;

                float currentLapTimeSec = BitConverter.ToSingle(buf.Slice(start + 4, 4));

                double curLapStartSec = Math.Max(0, hdr.SessionTime - currentLapTimeSec);
                ST.LapStartSec[car] = curLapStartSec;

                int currentLapNum = buf[start + 45];

                float prevLast = ST.LastLapTimeSeen.TryGetValue(car, out var p) ? p : -1f;
                ST.LastLapTimeSeen[car] = lastLapSec;

                int prevLap = ST.LastLapNum.TryGetValue(car, out var pl) ? pl : currentLapNum;
                ST.LastLapNum[car] = currentLapNum;

                bool lapChanged = (currentLapNum > prevLap) || (Math.Abs(lastLapSec - prevLast) > 1e-3);
                if (!lapChanged) continue;

                int lapNo = currentLapNum > 0 ? currentLapNum - 1
                                              : (ST.WrittenLapCount.TryGetValue(car, out var cnt) ? cnt : 0) + 1;
                if (lapNo <= 0) lapNo = 1;
                ST.WrittenLapCount[car] = lapNo;

                double lapEndSec = hdr.SessionTime;
                double lapStartSec = ST.LapStartSec.TryGetValue(car, out var s) ? s : Math.Max(0, lapEndSec - lastLapSec);

                string scTag = GetScOverlap(hdr.SessionUID, lapStartSec, lapEndSec);

                string display = ST.DriverDisplay.TryGetValue(car, out var disp) ? disp : "Unknown";
                string rawName = ST.DriverRawName.TryGetValue(car, out var nm) ? nm : "Unknown";
                ST.LastWear.TryGetValue(car, out var wear);
                string tyre = ST.CurrentTyre.TryGetValue(car, out var t) ? t : "Unknown";

                int tyreAge = ST.LastTyreAge.TryGetValue(car, out var a) ? a : -1;

                var row = new ParsedRow(
                    TimeStamp: DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    SessionUID: hdr.SessionUID,
                    SessionType: ST.SessionType,
                    CarIdx: car,
                    DriverColumn: display,
                    DriverRaw: rawName,
                    Lap: lapNo,
                    Tyre: tyre,
                    LapTimeMs: (int)Math.Round(lastLapSec * 1000.0),
                    Valid: true,
                    WearAvg: wear?.Avg, RL: wear?.RL, RR: wear?.RR, FL: wear?.FL, FR: wear?.FR,
                    SessTimeSec: hdr.SessionTime,
                    ScStatus: scTag,
                    TyreAge: tyreAge
                );

                var list = ST.SessionBuffers.GetOrAdd(hdr.SessionUID, _ => new());
                if (!list.Any(r => r.CarIdx == car && r.Lap == lapNo && Math.Abs(r.LapTimeMs - row.LapTimeMs) <= 1))
                    list.Add(row);
            }
        }

        // ========== Save / Excel ==========

        private static async Task SaveAllAsync(bool makeExcel)
        {
            lock (SaveLock) _saving = true;
            try
            {
                var (csvPath, xlsxPath) = GetStagePaths();
                Directory.CreateDirectory(Path.GetDirectoryName(csvPath)!);

                int appended = 0;
                bool newFile = !File.Exists(csvPath);
                using (var fs = new FileStream(csvPath, FileMode.Append, FileAccess.Write, FileShare.Read))
                using (var sw = new StreamWriter(fs, Encoding.UTF8))
                {
                    if (newFile)
                    {
                        await sw.WriteLineAsync("timestamp,session_uid,session_type,car_idx,driver_column,driver_raw,lap,tyre,lap_time_ms,valid,wear_avg,wear_rl,wear_rr,wear_fl,wear_fr,sess_time_sec,sc_status,tyre_age");
                    }

                    foreach (var kv in ST.SessionBuffers.ToArray())
                    {
                        var uid = kv.Key;
                        var list = kv.Value;
                        if (list.Count == 0) continue;

                        foreach (var r in list)
                        {
                            string sc = !string.IsNullOrEmpty(r.ScStatus)
                                ? r.ScStatus!
                                : GetScAt(uid, r.SessTimeSec);

                            await sw.WriteLineAsync(string.Join(",",
                                r.TimeStamp, r.SessionUID, r.SessionType, r.CarIdx,
                                Csv(r.DriverColumn), Csv(r.DriverRaw), r.Lap,
                                Csv(r.Tyre), r.LapTimeMs, "1",
                                r.WearAvg?.ToString(CultureInfo.InvariantCulture) ?? "",
                                r.RL?.ToString() ?? "", r.RR?.ToString() ?? "", r.FL?.ToString() ?? "", r.FR?.ToString() ?? "",
                                r.SessTimeSec.ToString(CultureInfo.InvariantCulture),
                                sc,
                                r.TyreAge.ToString(CultureInfo.InvariantCulture)
                            ));
                            appended++;
                        }

                        list.Clear();
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
                        Console.WriteLine($"[✓] {Path.GetFileName(xlsxPath)} rebuilt");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("[warn] Excel build failed: " + ex.Message);
                    }

                    ST.LastFinalSaveUtc = DateTime.UtcNow;
                }
            }
            finally
            {
                lock (SaveLock) _saving = false;
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

        // ========== ClosedXML Excel builder ==========

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

            // Листы кругов
            AddLapSheetXL(wb, "Race", rows.Where(IsRaceSession));
            if (rows.Any(r => r.SessionType.Equals("ShortQ", StringComparison.OrdinalIgnoreCase)))
            {
                AddLapSheetXL(wb, "ShortQ", rows.Where(r => r.SessionType.Equals("ShortQ", StringComparison.OrdinalIgnoreCase)));
            }
            else
            {
                if (rows.Any(r => r.SessionType.Equals("Q1", StringComparison.OrdinalIgnoreCase)))
                    AddLapSheetXL(wb, "Q1", rows.Where(r => r.SessionType.Equals("Q1", StringComparison.OrdinalIgnoreCase)));
                if (rows.Any(r => r.SessionType.Equals("Q2", StringComparison.OrdinalIgnoreCase)))
                    AddLapSheetXL(wb, "Q2", rows.Where(r => r.SessionType.Equals("Q2", StringComparison.OrdinalIgnoreCase)));
                if (rows.Any(r => r.SessionType.Equals("Q3", StringComparison.OrdinalIgnoreCase)))
                    AddLapSheetXL(wb, "Q3", rows.Where(r => r.SessionType.Equals("Q3", StringComparison.OrdinalIgnoreCase)));
            }

            // Сводки
            AddAveragePaceXL(wb, rows);
            AddMaxSpeedXL(wb);

            // Инфо
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
            var list = data.Where(r => r.Valid)
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

            var drivers = list.Select(r => r.DriverColumn)
                              .Distinct(StringComparer.OrdinalIgnoreCase)
                              .OrderBy(s => s, StringComparer.OrdinalIgnoreCase)
                              .ToList();

            ws.Cell(1, 1).Value = "Lap";
            for (int c = 0; c < drivers.Count; c++)
            {
                var headerCell = ws.Cell(1, 2 + c);
                headerCell.Value = drivers[c];
                headerCell.Style.Font.SetBold(); // жирный шрифт для имени

                // Подберём цвет по команде
                XLColor bg = XLColor.NoColor;
                int? carIdx = TryExtractCarIndexFromDisplay(drivers[c]);
                if (carIdx.HasValue && ST.TeamByCar.TryGetValue(carIdx.Value, out var teamId))
                    bg = TeamColor(teamId);

                if (bg != XLColor.NoColor)
                {
                    headerCell.Style.Fill.BackgroundColor = bg;
                }

                // Центрируем текст по вертикали для аккуратности
                headerCell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;
            }

            var byDriver = list.GroupBy(r => r.DriverColumn, StringComparer.OrdinalIgnoreCase)
                               .ToDictionary(g => g.Key, g => g.OrderBy(x => x.Lap).ToList(), StringComparer.OrdinalIgnoreCase);

            int maxRows = byDriver.Values.Select(v => v.Count).DefaultIfEmpty(0).Max();
            for (int i = 0; i < maxRows; i++)
            {
                ws.Cell(2 + i, 1).Value = i + 1;
                for (int c = 0; c < drivers.Count; c++)
                {
                    var d = drivers[c];
                    if (byDriver.TryGetValue(d, out var laps) && i < laps.Count)
                    {
                        var r = laps[i];
                        string cellText = $"{MsToStr(r.LapTimeMs)} ({r.Tyre})"
                                          + (r.WearAvg.HasValue ? $" — {Math.Round(r.WearAvg.Value)}%" : "")
                                          + (string.IsNullOrEmpty(r.ScStatus) ? "" : $" [{r.ScStatus}]");

                        var cell = ws.Cell(2 + i, 2 + c);
                        cell.Value = cellText;

                        // Пастельная раскраска по шинам
                        var color = GetTyreColor(r.Tyre);
                        if (color != XLColor.NoColor)
                        {
                            cell.Style.Fill.BackgroundColor = color;
                        }
                    }
                }
            }

            // Закрепить заголовок
            ws.SheetView.Freeze(1, 1);

            // Условное форматирование SC/VSC
            var used = ws.Range(2, 2, Math.Max(2, maxRows + 1), Math.Max(2, drivers.Count + 1));
            var scRule = used.AddConditionalFormat().WhenContains("[SC]");
            scRule.Font.SetItalic();
            scRule.Font.SetFontColor(GetTyreColor("SC"));
            var vscRule = used.AddConditionalFormat().WhenContains("[VSC]");
            vscRule.Font.SetItalic();
            vscRule.Font.SetFontColor(GetTyreColor("VSC"));

            ws.Columns().AdjustToContents();
            ws.Rows().AdjustToContents();
            ws.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;
            TrimWorksheet(ws);
        }

        private static void AddMaxSpeedXL(XLWorkbook wb)
        {
            if (ST.MaxSpeedQuali.Count == 0 && ST.MaxSpeedRace.Count == 0) return;

            var ws = wb.AddWorksheet("Max Speed");

            // Заголовки
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
            TrimWorksheet(ws);
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

            // ===== QUALI =====
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

            // ===== RACE =====
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
            TrimWorksheet(ws);
        }

        private static bool IsRaceSession(ParsedRow r)
            => r.SessionType.Equals("Race", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("R2", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("R", StringComparison.OrdinalIgnoreCase);

        private static bool IsQualiSession(ParsedRow r)
            => r.SessionType.Equals("ShortQ", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("Q1", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("Q2", StringComparison.OrdinalIgnoreCase)
            || r.SessionType.Equals("Q3", StringComparison.OrdinalIgnoreCase);

        // В рантайме по ST.SessionType:
        private static bool IsQualiNow()
            => ST.SessionType.Equals("ShortQ", StringComparison.OrdinalIgnoreCase)
            || ST.SessionType.Equals("Q1", StringComparison.OrdinalIgnoreCase)
            || ST.SessionType.Equals("Q2", StringComparison.OrdinalIgnoreCase)
            || ST.SessionType.Equals("Q3", StringComparison.OrdinalIgnoreCase);

        private static void AddLapSheet(ExcelPackage pkg, string sheetName, IEnumerable<ParsedRow> data)
        {
            var list = data.Where(r => r.Valid)
                           .OrderBy(r => r.DriverColumn, StringComparer.OrdinalIgnoreCase)
                           .ThenBy(r => r.Lap)
                           .ToList();

            var ws = pkg.Workbook.Worksheets.Add(sheetName);

            if (list.Count == 0)
            {
                ws.Cells[1, 1].Value = $"В {sheetName} нет данных";
                ws.Cells.AutoFitColumns();
                return;
            }

            // Столбцы — DriverColumn ("Ник [carIndex]" для MP)
            var drivers = list.Select(r => r.DriverColumn)
                              .Distinct(StringComparer.OrdinalIgnoreCase)
                              .OrderBy(s => s, StringComparer.OrdinalIgnoreCase)
                              .ToList();

            ws.Cells[1, 1].Value = "Lap";
            for (int c = 0; c < drivers.Count; c++)
                ws.Cells[1, 2 + c].Value = drivers[c];

            var byDriver = list.GroupBy(r => r.DriverColumn, StringComparer.OrdinalIgnoreCase)
                               .ToDictionary(g => g.Key, g => g.OrderBy(x => x.Lap).ToList(), StringComparer.OrdinalIgnoreCase);

            int maxRows = byDriver.Values.Select(v => v.Count).DefaultIfEmpty(0).Max();
            for (int i = 0; i < maxRows; i++)
            {
                ws.Cells[2 + i, 1].Value = i + 1;
                for (int c = 0; c < drivers.Count; c++)
                {
                    var d = drivers[c];
                    if (byDriver.TryGetValue(d, out var laps) && i < laps.Count)
                    {
                        var r = laps[i];
                        string cell = $"{MsToStr(r.LapTimeMs)} ({r.Tyre})"
                                      + (r.WearAvg.HasValue ? $" — {Math.Round(r.WearAvg.Value)}%" : "")
                                      + (string.IsNullOrEmpty(r.ScStatus) ? "" : $" [{r.ScStatus}]");
                        ws.Cells[2 + i, 2 + c].Value = cell;
                    }
                }
            }

            using var rng = ws.Cells[1, 1, Math.Max(2, maxRows + 1), Math.Max(2, drivers.Count + 1)];
            rng.AutoFitColumns();
            rng.Style.VerticalAlignment = ExcelVerticalAlignment.Center;
        }

        // ========== SC/VSC windows ==========

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
            const double EPS = 0.001; // 1 мс буфер
            if (!ST.ScWindows.TryGetValue(uid, out var list) || list.Count == 0) return "";
            bool hasSC = false, hasVSC = false;
            foreach (var w in list)
            {
                var wEnd = w.EndSec ?? double.MaxValue;
                // пересечение [startSec, endSec] и [w.StartSec, wEnd] с эпсилоном
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
            if (list.Count > 0)
            {
                var last = list[^1];
                if (last.Kind == kind && (last.EndSec is null || last.EndSec == double.MaxValue))
                    return;
            }
            list.Add(new ScWindow(kind, startSec, null));
        }

        private static void EndScWindow(ulong uid, string kind, double endSec, string srcTag)
        {
            if (!ST.ScWindows.TryGetValue(uid, out var list)) return;
            for (int i = list.Count - 1; i >= 0; i--)
            {
                if (list[i].Kind == kind && list[i].EndSec is null)
                {
                    list[i] = list[i] with { EndSec = endSec };
                    return;
                }
            }
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

        private static bool IsValidLap(ParsedRow r)
        {
            if (r == null) return false;
            if (!r.Valid) return false;
            if (r.LapTimeMs <= 0) return false;

            // Пересечение интервала круга с окнами SC/VSC (с небольшим эпсилоном)
            const double EPS = 0.001;
            double start = r.SessTimeSec - (r.LapTimeMs / 1000.0);
            if (start < 0) start = 0;
            double end = r.SessTimeSec;

            if (ST.ScWindows.TryGetValue(r.SessionUID, out var winList) && winList.Count > 0)
            {
                foreach (var w in winList)
                {
                    var wEnd = w.EndSec ?? double.MaxValue;
                    if ((start - EPS) <= (wEnd + EPS) && (end + EPS) >= (w.StartSec - EPS))
                        return false; // круг пересекся с SC/VSC
                }
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

                    // если TyreAge неизвестен — НЕ считаем это признаком out-lap
                    bool pitStop = compoundChanged || ageReset;

                    if (pitStop)
                    {
                        // предыдущий — in-lap: если уже добавили, уберём
                        if (cleaned.Count > 0 && prev != null &&
                        string.Equals(cleaned[^1].DriverColumn, g.Key, StringComparison.OrdinalIgnoreCase) &&
                        cleaned[^1].Lap == prevLap)
                        {
                            cleaned.RemoveAt(cleaned.Count - 1);
                        }
                        // текущий — out-lap: не добавляем
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
            // очистить окна SC/VSC и буферы предыдущих сессий (мы начинаем НОВЫЙ лог)
            ST.ScWindows.Clear();
            ST.SessionBuffers.Clear();

            // пере-якорить имя файлов на новый уикенд
            ST.AnchorInitialized = true;
            ST.AnchorSessionUID = hdr.SessionUID;
            ST.AnchorLocalDate = DateTime.Now.ToString("yyyy-MM-dd");
            if (trackIdHint.HasValue) ST.AnchorTrackId = trackIdHint;
            ST.AnchorTrackName = trackIdHint.HasValue ? TrackName(trackIdHint.Value) : ST.AnchorTrackName;

            // сбросить live-кеши машин
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

            ST.LastSessionClosed = false;

            Console.WriteLine($"[i] New weekend anchor: date={ST.AnchorLocalDate}, track={ST.AnchorTrackName}, sessionUID={ST.AnchorSessionUID}");
        }

        private static XLColor GetTyreColor(string tyreCompound)
        {
            if (string.IsNullOrWhiteSpace(tyreCompound)) return XLColor.NoColor;
            switch (tyreCompound.Trim().ToUpperInvariant())
            {
                case "SOFT": return XLColor.FromHtml("#FFC0C0"); // светло-красный
                case "MEDIUM": return XLColor.FromHtml("#FFFACD"); // светло-жёлтый
                case "HARD": return XLColor.FromHtml("#F5F5F5"); // светло-серый/белый
                case "INTER": return XLColor.FromHtml("#CCFFCC"); // светло-зелёный
                case "WET": return XLColor.FromHtml("#CCE5FF"); // светло-голубой
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
                12 => XLColor.FromHtml("#D9EDFC"), // Williams
                _ => XLColor.FromHtml("#F0F0F0")  // Unknown / F2 / др.
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

        // --- Post-fix: заменить "Unknown [carIdx]" на реальные ники в уже записанном CSV ---
        private static void FixCsvDriverNames(string csvPath)
        {
            try
            {
                if (!File.Exists(csvPath)) return;

                // построим маппинг car_idx -> "Имя [id]" из текущего состояния
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
                    if (parts.Length < 16) { sw.WriteLine(line); continue; }

                    if (int.TryParse(parts[3], out int carIdx) && map.TryGetValue(carIdx, out var display))
                    {
                        // проверяем ровно Unknown [id], который мы сами писали раньше
                        var drvCol = parts[4].Trim('"');
                        if (drvCol.Equals($"Unknown [{carIdx}]", StringComparison.OrdinalIgnoreCase))
                        {
                            parts[4] = Csv(display);

                            // обновим driver_raw, если там Unknown или пусто
                            var raw = parts[5].Trim('"');
                            if (string.IsNullOrWhiteSpace(raw) || raw.Equals("Unknown", StringComparison.OrdinalIgnoreCase))
                            {
                                var p = display.LastIndexOf('[');
                                var rawName = p > 0 ? display.Substring(0, p).Trim() : display;
                                parts[5] = Csv(rawName);
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
                Console.WriteLine("[warn] FixCsvDriverNames: " + ex.Message);
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
                    if (parts.Length < 16) { sw.WriteLine(line); continue; }

                    if (!int.TryParse(parts[3], out int carIdx)) { sw.WriteLine(line); continue; }

                    string drvCol = parts[4].Trim('"');
                    string raw = parts[5].Trim('"');

                    bool isBarePlayerOrAi =
                        drvCol.Equals("Player", StringComparison.OrdinalIgnoreCase) ||
                        drvCol.Equals("AI", StringComparison.OrdinalIgnoreCase);

                    bool hasIndex = drvCol.EndsWith($"]") && drvCol.Contains('[');

                    if (isBarePlayerOrAi && !hasIndex)
                    {
                        string display = ST.DriverDisplay.TryGetValue(carIdx, out var disp)
                                         ? disp
                                         : $"{drvCol} [{carIdx}]";

                        parts[4] = Csv(display);

                        if (string.IsNullOrWhiteSpace(raw))
                        {
                            var p = display.LastIndexOf('[');
                            var rawName = p > 0 ? display.Substring(0, p).Trim() : display;
                            parts[5] = Csv(rawName);
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
                Console.WriteLine("[warn] FixCsvBarePlayerAi: " + ex.Message);
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

        private static double? TryDouble(string s) => double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var v) ? v : null;
        private static int? TryInt(string s) => int.TryParse(s, out var v) ? v : null;

        private static ParsedRow? ParseCsvRow(string line)
        {
            var p = SplitCsv(line);

            // новый формат: ровно 18 колонок (0..17)
            if (p.Length >= 18)
            {
                return new ParsedRow(
                    TimeStamp: long.Parse(p[0]),
                    SessionUID: ulong.Parse(p[1]),
                    SessionType: p[2],
                    CarIdx: int.Parse(p[3]),
                    DriverColumn: p[4],
                    DriverRaw: p[5],
                    Lap: int.Parse(p[6]),
                    Tyre: p[7],
                    LapTimeMs: int.Parse(p[8]),
                    Valid: p[9] == "1" || p[9].Equals("true", StringComparison.OrdinalIgnoreCase),
                    WearAvg: TryDouble(p[10]),
                    RL: TryInt(p[11]),
                    RR: TryInt(p[12]),
                    FL: TryInt(p[13]),
                    FR: TryInt(p[14]),
                    SessTimeSec: TryDouble(p[15]) ?? 0.0,
                    ScStatus: p[16],
                    TyreAge: TryInt(p[17]) ?? -1
                );
            }

            // обратная совместимость: старый формат (16 колонок, без sess_time_sec и tyre_age)
            if (p.Length >= 16)
            {
                return new ParsedRow(
                    TimeStamp: long.Parse(p[0]),
                    SessionUID: ulong.Parse(p[1]),
                    SessionType: p[2],
                    CarIdx: int.Parse(p[3]),
                    DriverColumn: p[4],
                    DriverRaw: p[5],
                    Lap: int.Parse(p[6]),
                    Tyre: p[7],
                    LapTimeMs: int.Parse(p[8]),
                    Valid: p[9] == "1" || p[9].Equals("true", StringComparison.OrdinalIgnoreCase),
                    WearAvg: TryDouble(p[10]),
                    RL: TryInt(p[11]),
                    RR: TryInt(p[12]),
                    FL: TryInt(p[13]),
                    FR: TryInt(p[14]),
                    SessTimeSec: 0.0,          // не было в старом
                    ScStatus: p[15],
                    TyreAge: -1            // не было в старом
                );
            }

            return null;
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

        // --- Helpers: кто держит UDP-порт (через netstat) ---
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
            // Anchors (единое имя файлов на весь уикенд)
            public bool AnchorInitialized { get; set; } = false;
            public ulong AnchorSessionUID { get; set; } = 0;
            public string AnchorLocalDate { get; set; } = DateTime.Now.ToString("yyyy-MM-dd");
            public string AnchorTrackName { get; set; } = "UnknownTrack";
            public DateTime LastFinalSaveUtc { get; set; } = DateTime.MinValue;
            public bool LastSessionClosed { get; set; } = false;
            public byte? AnchorTrackId { get; set; } = null;

            // live session
            public string SessionType { get; set; } = "Unknown";
            public byte CurrentSC { get; set; } = 0;
            public byte? LastSCFromSession { get; set; } = null;
            public byte? TrackId { get; set; } = null;

            // buffers
            public ConcurrentDictionary<ulong, List<ParsedRow>> SessionBuffers { get; } = new();
            public ConcurrentDictionary<ulong, List<ScWindow>> ScWindows { get; } = new();

            // per-car live
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

            public Dictionary<int, int> MaxSpeedQuali { get; } = new();
            public Dictionary<int, int> MaxSpeedRace { get; } = new();

            // control
            public bool AutoSaveEnabled { get; set; } = true;
            public bool PendingFinalSave { get; set; } = false;

            // session switch detection
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
                }
            }
        }

        private sealed record WearInfo(int RL, int RR, int FL, int FR)
        {
            public double Avg => Math.Round((RL + RR + FL + FR) / 4.0, 1);
        }

        private sealed record ParsedRow(
            long TimeStamp,
            ulong SessionUID,
            string SessionType,
            int CarIdx,
            string DriverColumn,
            string DriverRaw,
            int Lap,
            string Tyre,
            int LapTimeMs,
            bool Valid,
            double? WearAvg,
            int? RL,
            int? RR,
            int? FL,
            int? FR,
            double SessTimeSec = 0.0,
            string ScStatus = "",
            int TyreAge = -1
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
    }
}