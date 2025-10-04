using System;
using System.Collections.Generic;
using System.Linq;
using ClosedXML.Excel;
using F12020TelemetryLogger.Models;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Excel
{
    internal static class RaceSheetBuilder
    {
        public static void AddRaceSheet(XLWorkbook wb, AppState state, string sheetName, IEnumerable<ParsedRow> data)
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

            if (state.FinalOrder.TryGetValue((sessionUid, "Race"), out var order) && order.Count > 0)
            {
                var orderedDrivers = order.Where(car => drivers.Contains(car)).ToList();
                var missingDrivers = drivers.Except(orderedDrivers).OrderBy(car =>
                    state.DriverDisplay.TryGetValue(car, out var d) ? d : $"Car {car}",
                    StringComparer.OrdinalIgnoreCase).ToList();
                drivers = orderedDrivers.Concat(missingDrivers).ToList();
            }
            else
            {
                drivers = drivers.OrderBy(car => state.DriverDisplay.TryGetValue(car, out var d) ? d : $"Car {car}",
                                          StringComparer.OrdinalIgnoreCase).ToList();
            }

            var driverColumns = drivers.Select(car =>
                state.DriverDisplay.TryGetValue(car, out var d) ? d : $"Car {car}"
            ).ToList();

            ws.Cell(1, 1).Value = "Lap";
            ws.Cell(1, 1).Style.Font.SetBold();
            ws.Cell(1, 1).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            ws.Cell(1, 1).Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

            for (int c = 0; c < driverColumns.Count; c++)
            {
                var headerCell = ws.Cell(1, 2 + c);
                headerCell.Value = driverColumns[c];
                headerCell.Style.Font.SetBold();
                headerCell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

                XLColor bg = XLColor.NoColor;
                int? carIdx = Helpers.TryExtractCarIndexFromDisplay(driverColumns[c]);
                if (carIdx.HasValue && state.TeamByCar.TryGetValue(carIdx.Value, out var teamId))
                    bg = ColorSchemes.TeamColor(teamId);

                if (bg != XLColor.NoColor)
                {
                    headerCell.Style.Fill.BackgroundColor = bg;
                }
            }

            var byDriver = list.GroupBy(r => r.DriverColumn, StringComparer.OrdinalIgnoreCase)
                               .ToDictionary(g => g.Key, g => g.OrderBy(x => x.Lap).ToList(), StringComparer.OrdinalIgnoreCase);

            int maxRows = byDriver.Values.Select(v => v.Count).DefaultIfEmpty(0).Max();

            for (int i = 0; i < maxRows; i++)
            {
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
                        string cellText = $"{Helpers.MsToStr(r.LapTimeMs)} ({r.Tyre})"
                                          + (r.WearAvg.HasValue ? $" — {Math.Round(r.WearAvg.Value)}%" : "")
                                          + (string.IsNullOrEmpty(r.ScStatus) ? "" : $" [{r.ScStatus}]");

                        var cell = ws.Cell(2 + i, 2 + c);
                        cell.Value = cellText;
                        cell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

                        var color = ColorSchemes.GetTyreColor(r.Tyre);
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
            scRule.Font.SetFontColor(ColorSchemes.GetTyreColor("SC"));
            var vscRule = used.AddConditionalFormat().WhenContains("[VSC]");
            vscRule.Font.SetItalic();
            vscRule.Font.SetFontColor(ColorSchemes.GetTyreColor("VSC"));

            ws.Column(1).Width = 5;
            for (int c = 2; c <= driverColumns.Count + 1; c++)
            {
                ws.Column(c).Width = 27;
            }
        }
    }
}