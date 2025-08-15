using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using OfficeOpenXml;
using OfficeOpenXml.Style;

namespace F12020TelemetryLogger;

internal static class Program
{
    // ===== Protocol constants for F1 2020 =====
    private const int HEADER_SIZE = 24;     // PacketHeader v2020 bytes
    private const int UDP_PORT = 20777;

    // Heuristic per-car record sizes for F1 2020 (empirically workable)
    private const int LAPDATA_STRIDE = 53;
    private const int PARTICIPANT_STRIDE = 54;
    private const int CARSTATUS_STRIDE = 60;

    // ===== App settings =====
    private static readonly TimeSpan AutoSaveInterval = TimeSpan.FromSeconds(10);
    private static readonly object SaveLock = new();

    // ===== Runtime =====
    private static readonly State ST = new();
    private static volatile bool _running = true;
    private static volatile bool _savingNow = false;
    private static DateTime _lastAutoSaveUtc = DateTime.UtcNow;
    private static UdpClient? _udp;
    private static Task? _menuTask;
    private static CancellationTokenSource? _cts;

    public static async Task<int> Main()
    {
        Console.OutputEncoding = Encoding.UTF8;
        ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

        Console.WriteLine($"[i] F1 2020 logger — UDP Port: {UDP_PORT}");
        PrintMenu();

        _cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; TriggerGracefulExit("Ctrl+C"); };

        _menuTask = Task.Run(MenuLoop);

        _udp = new UdpClient(UDP_PORT);
        _udp.Client.ReceiveTimeout = 50;
        _udp.Client.ReceiveBufferSize = 1 << 20;

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

                    // Первое касание — назначим якоря для названия файлов
                    MaybeInitAnchors(hdr);

                    switch (hdr.PacketId)
                    {
                        case 1:  ParseSession(buf, hdr); break;
                        case 2:  ParseLapData(buf, hdr); break;
                        case 3:  ParseEvent(buf, hdr); break;
                        case 4:  ParseParticipants(buf, hdr); break;
                        case 7:  ParseCarStatus(buf, hdr); break;
                        case 8:  // FinalClassification
                            ST.PendingFinalSave = true;
                            break;
                        default: break;
                    }

                    if (ST.PendingFinalSave && !_savingNow)
                    {
                        ST.PendingFinalSave = false;
                        CloseOpenScWindows();
                        await SaveAllAsync(makeExcel: true);
                        Console.WriteLine("[✓] Session finished — saved CSV/XLSX.");
                    }

                    if (DateTime.UtcNow - _lastAutoSaveUtc > AutoSaveInterval && !_savingNow)
                    {
                        await SaveAllAsync(makeExcel: false);
                        _lastAutoSaveUtc = DateTime.UtcNow;
                        Console.WriteLine("[i] Auto-saved CSV.");
                    }
                }
                catch (SocketException) { /* timeout tick */ }
                catch (ObjectDisposedException) { break; } // udp closed on exit
                catch (Exception ex)
                {
                    Console.WriteLine("[warn] " + ex.Message);
                }
            }
        }
        finally
        {
            await FinalizeAndExitAsync(exitFrom: "Main loop end");
        }

        return 0;
    }

    // ============ MENU ============

    private static async Task MenuLoop()
    {
        while (_running)
        {
            if (!Console.KeyAvailable)
            {
                await Task.Delay(50);
                continue;
            }
            var k = Console.ReadKey(true).Key;
            switch (k)
            {
                case ConsoleKey.D1:
                case ConsoleKey.NumPad1:
                    await SaveAllAsync(makeExcel: true);
                    Console.WriteLine("[i] Saved (manual).");
                    break;

                case ConsoleKey.D2:
                case ConsoleKey.NumPad2:
                    PrintSummary();
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

                case ConsoleKey.M:
                    PrintMenu();
                    break;

                case ConsoleKey.D0:
                case ConsoleKey.NumPad0:
                case ConsoleKey.Escape:
                    TriggerGracefulExit("Menu Exit");
                    return;
            }
        }
    }

    private static void TriggerGracefulExit(string reason)
    {
        if (!_running) return;
        _running = false;
        Console.WriteLine($"[i] Exiting… ({reason})");
        _ = Task.Run(async () => await FinalizeAndExitAsync(exitFrom: reason));
    }

    private static async Task FinalizeAndExitAsync(string exitFrom)
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

        try
        {
            _udp?.Close();
            _udp?.Dispose();
        }
        catch { /* ignore */ }

        try { _cts?.Cancel(); } catch { }
        try { _cts?.Dispose(); } catch { }

        Console.WriteLine("[i] Bye.");
        Environment.Exit(0);
    }

    private static void PrintMenu()
    {
        Console.WriteLine();
        Console.WriteLine("  [1] Save now (+Excel)   [2] Summary   [3] Auto-save ON/OFF   [4] Open folder   [M] Menu   [0/Esc] Exit");
        Console.WriteLine();
    }

    private static void PrintSummary()
    {
        int totalBuf = ST.SessionBuffers.Sum(kv => kv.Value.Count);
        Console.WriteLine($"[i] Buffered rows: {totalBuf}");
        foreach (var (uid, list) in ST.SessionBuffers.OrderBy(k => k.Key))
        {
            var byDrv = list.GroupBy(r => r.DriverName).OrderBy(g => g.Key, StringComparer.OrdinalIgnoreCase);
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
            var folder = Path.GetFullPath("./");
            var psi = new System.Diagnostics.ProcessStartInfo { FileName = folder, UseShellExecute = true };
            System.Diagnostics.Process.Start(psi);
        }
        catch (Exception ex) { Console.WriteLine("[warn] open folder: " + ex.Message); }
    }

    // ============ Parsing ============

    private static void MaybeInitAnchors(PacketHeader hdr)
    {
        if (ST.AnchorInitialized) return;

        ST.AnchorSessionUID = hdr.SessionUID; // первый увиденный — якорь
        ST.AnchorLocalDate = DateTime.Now.ToString("dd-MM-yyyy");
        // TrackName могут сообщить позже (после первого Session пакета), поэтому временно Unknown
        if (string.IsNullOrEmpty(ST.AnchorTrackName)) ST.AnchorTrackName = "UnknownTrack";

        ST.AnchorInitialized = true;
        Console.WriteLine($"[i] Anchor set → date={ST.AnchorLocalDate}, sessionUID={ST.AnchorSessionUID}");
    }

    private static void ParseSession(ReadOnlySpan<byte> buf, PacketHeader hdr)
    {
        int off = HEADER_SIZE;

        // weather(1) trackTemp(1) airTemp(1) totalLaps(1) trackLength(2)
        off += 1 + 1 + 1 + 1 + 2;

        // sessionType (1)
        if (off < buf.Length)
        {
            byte stype = buf[off];
            ST.SessionType = SessionTypeName(stype);
            off += 1;
        }

        // trackId (1)
        if (off < buf.Length)
        {
            byte tId = buf[off];
            ST.TrackId = tId;
            ST.AnchorTrackName = TrackName(tId);
            off += 1;
        }

        // formula(1), sessionTimeLeft(2), sessionDuration(2), pitSpeedLimit(1), gamePaused(1), isSpectating(1),
        // spectatorCarIndex(1), sliProNativeSupport(1)
        off += 1 + 2 + 2 + 1 + 1 + 1 + 1 + 1;

        // marshal zones count + zones
        if (off < buf.Length) off += 1 + 21 * 5;

        // safetyCarStatus (1)
        if (off < buf.Length)
        {
            byte sc = buf[off];
            if (sc <= 3) ST.CurrentSC = sc;

            if (ST.LastScFromSession is null) ST.LastScFromSession = sc;
            else if (ST.LastScFromSession != sc)
            {
                var prev = ST.LastScFromSession.Value;

                if (prev == 1 && sc != 1) EndScWindow(hdr.SessionUID, "SC", hdr.SessionTime, "[Session]");
                if (prev != 1 && sc == 1) StartScWindow(hdr.SessionUID, "SC", hdr.SessionTime, "[Session]");

                if ((prev == 2 || prev == 3) && sc != 2) EndScWindow(hdr.SessionUID, "VSC", hdr.SessionTime, "[Session]");
                if (sc == 2 && prev != 2) StartScWindow(hdr.SessionUID, "VSC", hdr.SessionTime, "[Session]");

                ST.LastScFromSession = sc;
            }
        }

        ST.OnPotentialNewSession(hdr.SessionUID);
    }

    private static void ParseEvent(ReadOnlySpan<byte> buf, PacketHeader hdr)
    {
        if (buf.Length < HEADER_SIZE + 4) return;
        string code = Encoding.ASCII.GetString(buf.Slice(HEADER_SIZE, 4));

        switch (code)
        {
            case "SCDP": ST.CurrentSC = 1; StartScWindow(hdr.SessionUID, "SC",  hdr.SessionTime, "[Event]"); Console.WriteLine("[EVT] SC deployed"); break;
            case "SCDE": ST.CurrentSC = 0; EndScWindow  (hdr.SessionUID, "SC",  hdr.SessionTime, "[Event]"); Console.WriteLine("[EVT] SC ended");    break;
            case "VSCN": ST.CurrentSC = 2; StartScWindow(hdr.SessionUID, "VSC", hdr.SessionTime, "[Event]"); Console.WriteLine("[EVT] VSC on");      break;
            case "VSCF": ST.CurrentSC = 3; EndScWindow  (hdr.SessionUID, "VSC", hdr.SessionTime, "[Event]"); Console.WriteLine("[EVT] VSC finished"); break;
            case "SEND":
                ST.PendingFinalSave = true;
                Console.WriteLine("[EVT] Session ended");
                break;
            default: break;
        }
    }

    private static void ParseParticipants(ReadOnlySpan<byte> buf, PacketHeader hdr)
    {
        int off = HEADER_SIZE;
        if (buf.Length < off + 1) return;
        int numActive = buf[off];
        off += 1;

        for (int car = 0; car < 22; car++)
        {
            int start = off + car * PARTICIPANT_STRIDE;
            if (start + PARTICIPANT_STRIDE > buf.Length) break;

            byte aiControlled = buf[start + 0];       // 0/1
            // driverId @ +1 (можно использовать для маппинга имён в сингле, но игра уже кладёт строки в name)
            byte networkId = buf[start + 2];          // multiplayer network id
            // teamId @ +3 (не используем для вывода)
            // myTeam @ +4 (не используем)
            // name[48] @ +5
            var nameBytes = buf.Slice(start + 5, 48).ToArray();
            string name = DecodeName(nameBytes);
            if (string.IsNullOrWhiteSpace(name))
                name = aiControlled == 1 ? "AI" : "Player";

            ST.DriverName[car] = name;         // <-- в мультиплеере это будет НИК
            ST.NetworkId[car] = networkId;     // для внутренней идентификации
        }

        // Обновим уже буферизованные записи, где имя было Unknown
        foreach (var kv in ST.SessionBuffers)
        {
            var list = kv.Value;
            for (int i = 0; i < list.Count; i++)
            {
                var r = list[i];
                if (r.DriverName == "Unknown" && ST.DriverName.TryGetValue(r.CarIdx, out var nm))
                    list[i] = r with { DriverName = nm };
            }
        }
    }

    private static void ParseCarStatus(ReadOnlySpan<byte> buf, PacketHeader hdr)
    {
        int off = HEADER_SIZE;
        if (off + CARSTATUS_STRIDE * 22 > buf.Length) return;

        for (int car = 0; car < 22; car++)
        {
            int start = off + car * CARSTATUS_STRIDE;
            var block = buf.Slice(start, CARSTATUS_STRIDE);

            // tyres wear (approx): RL, RR, FL, FR @ 25..28
            int rl = block[25], rr = block[26], fl = block[27], fr = block[28];
            if (IsPct(rl) && IsPct(rr) && IsPct(fl) && IsPct(fr))
                ST.LastWear[car] = new WearInfo(rl, rr, fl, fr);

            // tyre compound: actual(29), visual(30)
            string tyre = TyreName(block[29], block[30]);
            ST.CurrentTyre[car] = tyre;
        }
    }

    private static void ParseLapData(ReadOnlySpan<byte> buf, PacketHeader hdr)
    {
        int off = HEADER_SIZE;
        if (off + LAPDATA_STRIDE * 22 > buf.Length) return;

        long ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        for (int car = 0; car < 22; car++)
        {
            int start = off + car * LAPDATA_STRIDE;

            // lastLapTime (float32) @ +0
            float lastLapSec = BitConverter.ToSingle(buf.Slice(start, 4));
            if (!(lastLapSec > 0.0001f)) continue;

            // currentLapNum @ +45 (uint8)
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

            string driver = ST.DriverName.TryGetValue(car, out var nm) ? nm : "Unknown";
            ST.LastWear.TryGetValue(car, out var wear);
            string tyre = ST.CurrentTyre.TryGetValue(car, out var t) ? t : "Unknown";

            var row = new LapRow(
                Ts: ts,
                SessionUID: hdr.SessionUID,
                SessionType: ST.SessionType,
                CarIdx: car,
                DriverName: driver,                 // В Excel будет использовано ТОЛЬКО имя/ник
                Lap: lapNo,
                Tyre: tyre,
                LapTimeMs: (int)Math.Round(lastLapSec * 1000.0),
                Valid: true,
                WearAvg: wear?.Avg, RL: wear?.RL, RR: wear?.RR, FL: wear?.FL, FR: wear?.FR,
                SessTimeSec: hdr.SessionTime
            );

            var list = ST.SessionBuffers.GetOrAdd(hdr.SessionUID, _ => new());
            // Дедупликация
            if (!list.Any(r => r.CarIdx == car && r.Lap == lapNo && Math.Abs(r.LapTimeMs - row.LapTimeMs) <= 1))
                list.Add(row);
        }
    }

    // ============ Save & Excel ============

    private static async Task SaveAllAsync(bool makeExcel)
    {
        if (!_running && !makeExcel && ST.SessionBuffers.All(kv => kv.Value.Count == 0))
            return; // ничего нового

        lock (SaveLock) { _savingNow = true; }
        try
        {
            // Подготовим базу имени
            var (csvPath, xlsxPath) = GetStagePaths();

            int appended = 0;
            bool newFile = !File.Exists(csvPath);
            using (var fs = new FileStream(csvPath, FileMode.Append, FileAccess.Write, FileShare.Read))
            using (var sw = new StreamWriter(fs, Encoding.UTF8))
            {
                if (newFile)
                {
                    await sw.WriteLineAsync("timestamp,session_uid,session_type,car_idx,driver_name,lap,tyre,lap_time_ms,valid,wear_avg,wear_rl,wear_rr,wear_fl,wear_fr,sc_status");
                }

                foreach (var kv in ST.SessionBuffers.ToArray())
                {
                    var uid = kv.Key;
                    var list = kv.Value;
                    if (list.Count == 0) continue;

                    foreach (var r in list)
                    {
                        string sc = GetScAt(uid, r.SessTimeSec);
                        await sw.WriteLineAsync(string.Join(",",
                            r.Ts, r.SessionUID, r.SessionType, r.CarIdx, Csv(r.DriverName),
                            r.Lap, Csv(r.Tyre), r.LapTimeMs, "1",
                            r.WearAvg?.ToString(CultureInfo.InvariantCulture) ?? "",
                            r.RL?.ToString() ?? "", r.RR?.ToString() ?? "", r.FL?.ToString() ?? "", r.FR?.ToString() ?? "",
                            sc
                        ));
                        appended++;
                    }

                    list.Clear();
                }
                await sw.FlushAsync();
            }

            if (appended > 0)
                Console.WriteLine($"[+] {Path.GetFileName(csvPath)} +{appended}");

            if (makeExcel)
            {
                try
                {
                    BuildExcel(csvPath, xlsxPath);
                    Console.WriteLine($"[✓] {Path.GetFileName(xlsxPath)} rebuilt");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("[warn] Excel build failed: " + ex.Message);
                }
            }
        }
        finally
        {
            lock (SaveLock) { _savingNow = false; }
        }
    }

    private static (string csvPath, string xlsxPath) GetStagePaths()
    {
        string date = ST.AnchorLocalDate ?? DateTime.Now.ToString("dd-MM-yyyy");
        string track = string.IsNullOrWhiteSpace(ST.AnchorTrackName) ? "UnknownTrack" : SafeName(ST.AnchorTrackName);
        string uid = (ST.AnchorSessionUID != 0) ? ST.AnchorSessionUID.ToString() : "session";
        string baseName = $"log_f1_2020_{date}_{track}_{uid}";
        var folder = Path.GetFullPath("./");
        return (Path.Combine(folder, baseName + ".csv"), Path.Combine(folder, baseName + ".xlsx"));
    }

    private static string SafeName(string s)
    {
        var invalid = Path.GetInvalidFileNameChars();
        var sb = new StringBuilder(s.Length);
        foreach (var ch in s)
            sb.Append(invalid.Contains(ch) ? '_' : ch);
        return sb.ToString();
    }

    private static void BuildExcel(string csvPath, string xlsxPath)
    {
        var lines = File.ReadAllLines(csvPath);
        if (lines.Length <= 1)
        {
            using var emptyPkg = new ExcelPackage();
            emptyPkg.Workbook.Worksheets.Add("Info").Cells[1, 1].Value = "Нет данных";
            emptyPkg.SaveAs(new FileInfo(xlsxPath));
            return;
        }

        var rows = lines.Skip(1)
                        .Select(ParseCsvRow)
                        .Where(r => r != null)
                        .Select(r => r!)
                        .ToList();

        using var pkg = new ExcelPackage();

        // Листы кругов
        AddLapSheet(pkg, "Race", rows.Where(IsRaceSession));
        if (rows.Any(r => r.SessionType.Equals("ShortQ", StringComparison.OrdinalIgnoreCase)))
        {
            AddLapSheet(pkg, "ShortQ", rows.Where(r => r.SessionType.Equals("ShortQ", StringComparison.OrdinalIgnoreCase)));
        }
        else
        {
            if (rows.Any(r => r.SessionType.Equals("Q1", StringComparison.OrdinalIgnoreCase)))
                AddLapSheet(pkg, "Q1", rows.Where(r => r.SessionType.Equals("Q1", StringComparison.OrdinalIgnoreCase)));
            if (rows.Any(r => r.SessionType.Equals("Q2", StringComparison.OrdinalIgnoreCase)))
                AddLapSheet(pkg, "Q2", rows.Where(r => r.SessionType.Equals("Q2", StringComparison.OrdinalIgnoreCase)));
            if (rows.Any(r => r.SessionType.Equals("Q3", StringComparison.OrdinalIgnoreCase)))
                AddLapSheet(pkg, "Q3", rows.Where(r => r.SessionType.Equals("Q3", StringComparison.OrdinalIgnoreCase)));
        }

        // Сводные
        AddAverages(pkg, rows);
        AddAvgByTyrePerBucket(pkg, "Race", rows.Where(IsRaceSession));

        if (rows.Any(r => r.SessionType.Equals("ShortQ", StringComparison.OrdinalIgnoreCase)))
            AddAvgByTyrePerBucket(pkg, "ShortQ", rows.Where(r => r.SessionType.Equals("ShortQ", StringComparison.OrdinalIgnoreCase)));
        else
        {
            if (rows.Any(r => r.SessionType.Equals("Q1", StringComparison.OrdinalIgnoreCase)))
                AddAvgByTyrePerBucket(pkg, "Q1", rows.Where(r => r.SessionType.Equals("Q1", StringComparison.OrdinalIgnoreCase)));
            if (rows.Any(r => r.SessionType.Equals("Q2", StringComparison.OrdinalIgnoreCase)))
                AddAvgByTyrePerBucket(pkg, "Q2", rows.Where(r => r.SessionType.Equals("Q2", StringComparison.OrdinalIgnoreCase)));
            if (rows.Any(r => r.SessionType.Equals("Q3", StringComparison.OrdinalIgnoreCase)))
                AddAvgByTyrePerBucket(pkg, "Q3", rows.Where(r => r.SessionType.Equals("Q3", StringComparison.OrdinalIgnoreCase)));
        }

        // Инфо-лист
        var info = pkg.Workbook.Worksheets.Add("Info");
        var (csvFinal, xlsxFinal) = GetStagePaths();
        info.Cells[1, 1].Value = "CSV";
        info.Cells[1, 2].Value = Path.GetFileName(csvFinal);
        info.Cells[2, 1].Value = "XLSX";
        info.Cells[2, 2].Value = Path.GetFileName(xlsxFinal);
        info.Cells[3, 1].Value = "Track";
        info.Cells[3, 2].Value = ST.AnchorTrackName;
        info.Cells[4, 1].Value = "Date";
        info.Cells[4, 2].Value = ST.AnchorLocalDate;
        info.Cells[5, 1].Value = "SessionUID(anchor)";
        info.Cells[5, 2].Value = ST.AnchorSessionUID.ToString();
        info.Cells.AutoFitColumns();

        pkg.SaveAs(new FileInfo(xlsxPath));
    }

    private static bool IsRaceSession(ParsedRow r)
        => r.SessionType.Equals("Race", StringComparison.OrdinalIgnoreCase)
        || r.SessionType.Equals("R2", StringComparison.OrdinalIgnoreCase)
        || r.SessionType.Equals("R", StringComparison.OrdinalIgnoreCase);

    private static void AddLapSheet(ExcelPackage pkg, string sheetName, IEnumerable<ParsedRow> data)
    {
        var list = data.Where(r => r.Valid)
                       .OrderBy(r => r.DriverName, StringComparer.OrdinalIgnoreCase)
                       .ThenBy(r => r.Lap)
                       .ToList();

        var ws = pkg.Workbook.Worksheets.Add(sheetName);

        if (list.Count == 0)
        {
            ws.Cells[1, 1].Value = $"В {sheetName} нет данных";
            ws.Cells.AutoFitColumns();
            return;
        }

        // Только НИКИ — без network_id
        var drivers = list.Select(r => r.DriverName)
                          .Distinct(StringComparer.OrdinalIgnoreCase)
                          .OrderBy(s => s, StringComparer.OrdinalIgnoreCase)
                          .ToList();

        ws.Cells[1, 1].Value = "Lap";
        for (int c = 0; c < drivers.Count; c++)
            ws.Cells[1, 2 + c].Value = drivers[c];

        var byDriver = list.GroupBy(r => r.DriverName, StringComparer.OrdinalIgnoreCase)
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

    private static void AddAverages(ExcelPackage pkg, List<ParsedRow> rows)
    {
        var ws = pkg.Workbook.Worksheets.Add("Averages");
        ws.Cells[1, 1].Value = "session_type";
        ws.Cells[1, 2].Value = "driver";
        ws.Cells[1, 3].Value = "tyre";
        ws.Cells[1, 4].Value = "avg_lap_time";
        ws.Cells[1, 5].Value = "laps";

        int r = 2;
        foreach (var g in rows.Where(x => x.Valid)
                              .GroupBy(x => new { x.SessionType, x.DriverName, x.Tyre })
                              .OrderBy(g => g.Key.SessionType)
                              .ThenBy(g => g.Key.DriverName, StringComparer.OrdinalIgnoreCase)
                              .ThenBy(g => g.Key.Tyre, StringComparer.OrdinalIgnoreCase))
        {
            double avgMs = g.Average(x => x.LapTimeMs);
            ws.Cells[r, 1].Value = g.Key.SessionType;
            ws.Cells[r, 2].Value = g.Key.DriverName; // только ник
            ws.Cells[r, 3].Value = g.Key.Tyre;
            ws.Cells[r, 4].Value = MsToStr((int)Math.Round(avgMs));
            ws.Cells[r, 5].Value = g.Count();
            r++;
        }
        ws.Cells[1, 1, Math.Max(2, r), 5].AutoFitColumns();
    }

    private static void AddAvgByTyrePerBucket(ExcelPackage pkg, string bucketName, IEnumerable<ParsedRow> data)
    {
        var rows = data.Where(r => r.Valid).ToList();
        var ws = pkg.Workbook.Worksheets.Add($"Avg by Tyre — {bucketName}");

        if (rows.Count == 0)
        {
            ws.Cells[1, 1].Value = $"{bucketName}: нет данных";
            ws.Cells.AutoFitColumns();
            return;
        }

        // Только ники
        var drivers = rows.Select(r => r.DriverName)
                          .Distinct(StringComparer.OrdinalIgnoreCase)
                          .OrderBy(s => s, StringComparer.OrdinalIgnoreCase)
                          .ToList();
        var tyres = rows.Select(r => r.Tyre).Distinct().OrderBy(TyreOrder).ToList();

        ws.Cells[1, 1].Value = "Driver";
        for (int c = 0; c < tyres.Count; c++) ws.Cells[1, 2 + c].Value = tyres[c];

        int r = 2;
        foreach (var d in drivers)
        {
            ws.Cells[r, 1].Value = d;
            for (int c = 0; c < tyres.Count; c++)
            {
                string t = tyres[c];
                var laps = rows.Where(x => x.DriverName.Equals(d, StringComparison.OrdinalIgnoreCase) && x.Tyre == t).ToList();
                if (laps.Count > 0)
                {
                    double avgMs = laps.Average(x => x.LapTimeMs);
                    ws.Cells[r, 2 + c].Value = MsToStr((int)Math.Round(avgMs));
                }
            }
            r++;
        }
        ws.Cells[1, 1, Math.Max(2, r), Math.Max(2, tyres.Count + 1)].AutoFitColumns();
    }

    // ============ SC/VSC windows ============

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

    private static void StartScWindow(ulong uid, string kind, double startSec, string srcTag)
    {
        var list = ST.ScWindows.GetOrAdd(uid, _ => new());
        if (list.Count > 0)
        {
            var last = list[^1];
            if (last.Kind == kind && (last.EndSec is null || last.EndSec == double.MaxValue))
                return; // уже открыто
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

    // ============ Helpers ============

    private static string SessionTypeName(byte sessionType) => sessionType switch
    {
        0  => "Unknown",
        1  => "P1",
        2  => "P2",
        3  => "P3",
        4  => "ShortP",
        5  => "Q1",
        6  => "Q2",
        7  => "Q3",
        8  => "ShortQ",
        9  => "OSQ",
        10 => "Race",
        11 => "R2",
        12 => "TT",
        _  => "Unknown"
    };

    private static string TrackName(byte trackId) => TrackNames.TryGetValue(trackId, out var n) ? n : $"Track_{trackId}";

    private static string DecodeName(byte[] nameBytes)
    {
        int zero = Array.IndexOf(nameBytes, (byte)0);
        if (zero >= 0) nameBytes = nameBytes[..zero];
        string s = Encoding.UTF8.GetString(nameBytes);
        var sb = new StringBuilder(s.Length);
        foreach (var ch in s) if (ch >= 0x20) sb.Append(ch);
        s = sb.ToString().Trim();
        return string.IsNullOrWhiteSpace(s) ? "Unknown" : s;
    }

    private static bool IsPct(int v) => v >= 0 && v <= 100;

    private static string TyreName(byte actual, byte visual)
    {
        // визуальные чаще всего: 16 Soft, 17 Medium, 18 Hard, 7 Inter, 8 Wet
        return visual switch
        {
            16 => "Soft",
            17 => "Medium",
            18 => "Hard",
            7  => "Inter",
            8  => "Wet",
            _ => actual switch
            {
                16 => "C5", 17 => "C4", 18 => "C3", 19 => "C2", 20 => "C1",
                7 => "Inter", 8 => "Wet",
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
        "Soft" => 0, "Medium" => 1, "Hard" => 2, "Inter" => 3, "Wet" => 4, _ => 99
    };

    private static double? TryDouble(string s) => double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var v) ? v : null;
    private static int? TryInt(string s) => int.TryParse(s, out var v) ? v : null;

    private static ParsedRow? ParseCsvRow(string line)
    {
        var parts = SplitCsv(line);
        if (parts.Length < 15) return null;
        return new ParsedRow(
            Ts: long.Parse(parts[0]),
            SessionUID: ulong.Parse(parts[1]),
            SessionType: parts[2],
            CarIdx: int.Parse(parts[3]),
            DriverName: parts[4],
            Lap: int.Parse(parts[5]),
            Tyre: parts[6],
            LapTimeMs: int.Parse(parts[7]),
            Valid: parts[8] == "1",
            WearAvg: TryDouble(parts[9]),
            RL: TryInt(parts[10]),
            RR: TryInt(parts[11]),
            FL: TryInt(parts[12]),
            FR: TryInt(parts[13]),
            ScStatus: parts[14]
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

    // ============ Models & State ============

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
        // Anchors for naming & grouping
        public bool AnchorInitialized { get; set; } = false;
        public ulong AnchorSessionUID { get; set; } = 0;
        public string AnchorLocalDate { get; set; } = DateTime.Now.ToString("dd-MM-yyyy");
        public string AnchorTrackName { get; set; } = "UnknownTrack";

        // Session state
        public string SessionType { get; set; } = "Unknown";
        public byte CurrentSC { get; set; } = 0;
        public byte? LastScFromSession { get; set; } = null;
        public byte? TrackId { get; set; } = null;

        // Buffers
        public ConcurrentDictionary<ulong, List<LapRow>> SessionBuffers { get; } = new();
        public ConcurrentDictionary<ulong, List<ScWindow>> ScWindows { get; } = new();

        // Per-car live state
        public Dictionary<int, string> DriverName { get; } = new();
        public Dictionary<int, int> LastLapNum { get; } = new();
        public Dictionary<int, float> LastLapTimeSeen { get; } = new();
        public Dictionary<int, int> WrittenLapCount { get; } = new();
        public Dictionary<int, WearInfo> LastWear { get; } = new();
        public Dictionary<int, string> CurrentTyre { get; } = new();
        public Dictionary<int, byte> NetworkId { get; } = new();

        // Control
        public bool AutoSaveEnabled { get; set; } = true;
        public bool PendingFinalSave { get; set; } = false;

        // Session switch tracking
        private ulong? _lastUid = null;
        public void OnPotentialNewSession(ulong uid)
        {
            if (_lastUid != uid)
            {
                _lastUid = uid;
                LastLapNum.Clear();
                LastLapTimeSeen.Clear();
                WrittenLapCount.Clear();
                CurrentSC = 0;
                // Не трогаем якоря (Anchor*), чтобы имя файла оставалось единым, даже при смене дня
            }
        }
    }

    private sealed record WearInfo(int RL, int RR, int FL, int FR)
    {
        public double Avg => Math.Round((RL + RR + FL + FR) / 4.0, 1);
    }

    private sealed record LapRow(
        long Ts, ulong SessionUID, string SessionType,
        int CarIdx, string DriverName, int Lap, string Tyre,
        int LapTimeMs, bool Valid, double? WearAvg, int? RL, int? RR, int? FL, int? FR,
        double SessTimeSec
    );

    private sealed record ParsedRow(
        long Ts, ulong SessionUID, string SessionType,
        int CarIdx, string DriverName, int Lap, string Tyre,
        int LapTimeMs, bool Valid, double? WearAvg, int? RL, int? RR, int? FL, int? FR,
        string ScStatus
    );

    private sealed record ScWindow(string Kind, double StartSec, double? EndSec);

    // Track names for F1 2020 (id -> name). Значения соответствуют игре (могут отличаться в модах).
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
        { 15, "Texas" },          // COTA
        { 16, "Brazil" },         // Interlagos
        { 17, "Austria" },
        { 18, "Sochi" },
        { 19, "Mexico" },
        { 20, "Baku" },
        { 21, "SakhirShort" },
        { 22, "SilverstoneShort" },
        { 23, "TexasShort" },
        { 24, "SuzukaShort" },
        { 25, "Hanoi" },          // F1 2020
        { 26, "Zandvoort" }       // F1 2020
    };
}