using System;
using System.Collections.Generic;
using System.Linq;
using ClosedXML.Excel;
using F12020TelemetryLogger.Models;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Excel
{
    internal static class IncidentsBuilder
    {
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

        public static void AddIncidentsSheet(XLWorkbook wb, AppState state)
        {
            if (state.Incidents == null || state.Incidents.Count == 0) return;

            var ws = wb.AddWorksheet("Incidents");

            ws.Cell(1, 1).Value = "Session";
            ws.Cell(1, 2).Value = "Lap";
            ws.Cell(1, 3).Value = "Driver";
            ws.Cell(1, 4).Value = "Incident";
            ws.Cell(1, 5).Value = "Penalty";
            ws.Range(1, 1, 1, 5).Style.Font.SetBold();

            int r = 2;

            var sessions = state.Incidents
                .Select(kv =>
                {
                    var uid = kv.Key;
                    var sessName = state.SessionTypeByUid.TryGetValue(uid, out var nm) ? nm : "Unknown";
                    return new
                    {
                        Uid = uid,
                        Name = sessName,
                        Order = Helpers.SessionOrderKey(sessName),
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
                    string driver = (x.CarIdx >= 0 && state.DriverDisplay.TryGetValue(x.CarIdx, out var disp))
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
    }
}