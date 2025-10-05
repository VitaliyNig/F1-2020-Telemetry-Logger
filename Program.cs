using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using F12020TelemetryLogger.Models;
using F12020TelemetryLogger.Parsers;
using F12020TelemetryLogger.Services;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;
using F12020TelemetryLogger.Menu;

namespace F12020TelemetryLogger
{
    internal static class Program
    {
        private static readonly AppState ST = new();
        private static volatile bool _running = true;
        private static bool _saving = false;
        private static DateTime _lastAutoSaveUtc = DateTime.UtcNow;
        private static readonly TimeSpan AutoSaveEvery = TimeSpan.FromSeconds(10);

        private static UdpClient? _udp;
        private static CancellationTokenSource? _cts;

        public static async Task<int> Main(string[]? args = null)
        {
            Console.OutputEncoding = Encoding.UTF8;
            try { Console.InputEncoding = Encoding.UTF8; } catch { }

            int port = Constants.UDP_PORT_DEFAULT;
            if (args is { Length: > 0 } && int.TryParse(args[0], out var p) && p is > 0 and < 65536)
                port = p;

            PrintBanner();
            MenuHandler.PrintMenu();

            try
            {
                _udp = new UdpClient(AddressFamily.InterNetwork);
                _udp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                _udp.Client.Bind(new IPEndPoint(IPAddress.Any, port));
                _udp.Client.ReceiveTimeout = 50;
                _udp.Client.ReceiveBufferSize = 1 << 20;
                Log.Info($"[i] UDP Listening Enabled: {port} (ReuseAddress=ON)");
            }
            catch (SocketException se) when (se.SocketErrorCode == SocketError.AddressAlreadyInUse ||
                                             se.SocketErrorCode == SocketError.AccessDenied)
            {
                Log.Warn($"[warning] Порт {port} недоступен: {se.Message}");
                var owners = NetworkHelpers.FindUdpOwners(port);

                if (owners.Count > 0)
                {
                    Log.Warn("[warning] Порт занят процессом: ");
                    foreach (var (pid, name) in owners)
                        Console.WriteLine($"  • {name} (PID {pid})");
                }
                else
                    Log.Warn("[warning] Не удалось определить процесс, занявший порт");

                Console.WriteLine($"\n[i] Закройте приложение выше или запустите логгер на другом порту, например:");
                Console.WriteLine($"    dotnet run -- {port + 1}");
                Console.WriteLine($"    telemetry.exe {port + 1}");
                return 2;
            }

            _cts = new CancellationTokenSource();
            var menuTask = Task.Run(() => MenuHandler.RunMenuLoop(ST, () => _running, (v) => _running = v, TriggerSave, _udp), _cts.Token);

            try
            {
                while (_running)
                {
                    try
                    {
                        var res = await _udp.ReceiveAsync();
                        var buf = res.Buffer;
                        if (buf.Length < Constants.HEADER_SIZE) continue;

                        var hdr = PacketHeader.Parse(buf);
                        if (hdr.PacketId < ST.PidCounters.Length)
                            ST.PidCounters[hdr.PacketId]++;

                        InitializeAnchors(hdr);

                        switch (hdr.PacketId)
                        {
                            case 1:
                                SessionParser.Parse(buf, hdr, ST);
                                break;
                            case 2:
                                if (ST.DebugPackets)
                                    Console.WriteLine("[DBG] Packet ID=2 (LapData) received");
                                LapDataParser.Parse(buf, hdr, ST);
                                break;
                            case 3:
                                EventParser.Parse(buf, hdr, ST, TriggerSave);
                                break;
                            case 4:
                                ParticipantsParser.Parse(buf, hdr, ST);
                                break;
                            case 6:
                                CarTelemetryParser.Parse(buf, hdr, ST);
                                break;
                            case 7:
                                CarStatusParser.Parse(buf, hdr, ST);
                                break;
                        }

                        if (ST.AutoSaveEnabled &&
                            DateTime.UtcNow - _lastAutoSaveUtc >= AutoSaveEvery &&
                            !_saving)
                        {
                            await TriggerSave(makeExcel: false);
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
                await FinalizeAndExit();
            }

            return 0;
        }

        private static void InitializeAnchors(PacketHeader hdr)
        {
            if (ST.AnchorInitialized) return;

            if (hdr.PacketFormat != Constants.EXPECTED_PACKET_FORMAT)
            {
                Log.Warn($"[warning] Unexpected packet format: {hdr.PacketFormat}, expected {Constants.EXPECTED_PACKET_FORMAT}");
            }

            ST.AnchorInitialized = true;
            ST.AnchorSessionUID = hdr.SessionUID;
            ST.AnchorLocalDate = DateTime.Now.ToString("yyyy-MM-dd");
            if (ST.TrackId.HasValue) ST.AnchorTrackId = ST.TrackId;
            if (string.IsNullOrEmpty(ST.AnchorTrackName)) ST.AnchorTrackName = "UnknownTrack";
            Console.WriteLine($"[i] Anchor: Date={ST.AnchorLocalDate}, SessionUID={ST.AnchorSessionUID}, Format={hdr.PacketFormat}");
        }

        private static async Task TriggerSave(bool makeExcel)
        {
            if (_saving) return;

            try
            {
                _saving = true;
                ScWindowService.CloseOpenWindows(ST);
                await SaveService.SaveAllAsync(ST, makeExcel);
            }
            catch (Exception ex)
            {
                Log.Warn("[warning] Save Failed: " + ex.Message);
            }
            finally
            {
                _saving = false;
            }
        }

        private static async Task FinalizeAndExit()
        {
            Log.Info("[i] Shutting down...");

            try { _cts?.Cancel(); } catch { }
            try { _udp?.Close(); } catch { }
            try { _udp?.Dispose(); } catch { }

            await Task.Delay(100);

            try
            {
                ScWindowService.CloseOpenWindows(ST);
                
                var saveTask = SaveService.SaveAllAsync(ST, makeExcel: true);
                if (await Task.WhenAny(saveTask, Task.Delay(5000)) == saveTask)
                {
                    await saveTask;
                    Log.Success("[✓] Final Save Complete");
                }
                else
                {
                    Log.Warn("[warning] Final save timed out after 5 seconds");
                }
            }
            catch (Exception ex)
            {
                Log.Warn("[warning] Final Save Failed: " + ex.Message);
            }

            Log.Info("[i] Bye");
            await Task.Delay(500);
        }

        private static void PrintBanner()
        {
            Console.BackgroundColor = ConsoleColor.DarkMagenta;
            Console.WriteLine("    Telemetry Logger | F1 2020 | v7.2    ");
            Console.ResetColor();
        }

        public static AppState GetState() => ST;
    }
}