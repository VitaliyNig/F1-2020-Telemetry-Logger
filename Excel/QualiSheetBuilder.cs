using System;
using System.Collections.Generic;
using System.Linq;
using ClosedXML.Excel;
using F12020TelemetryLogger.Models;
using F12020TelemetryLogger.Services;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Excel
{
    internal static class QualiSheetBuilder
    {
        public static void AddQualiSheet(XLWorkbook wb, AppState state, string sheetName, IEnumerable<ParsedRow> data)
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

            if (state.FinalOrder.TryGetValue((sessUid, "Quali"), out var order) && order.Count > 0)
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

            int MinValidSectorFromHistory(int car, int sector)
            {
                if (!state.LapSectors.TryGetValue(sessUid, out var byCar) ||
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

            ws.Cell(1, 1).Value = "Lap";
            ws.Cell(1, 1).Style.Font.SetBold();
            ws.Cell(1, 1).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            ws.Cell(1, 1).Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

            int col = 2;
            foreach (var car in drivers)
            {
                string name = state.DriverDisplay.TryGetValue(car, out var d) ? d : $"Car {car}";

                ws.Range(1, col, 1, col + 1).Merge();
                var headerCell = ws.Cell(1, col);
                headerCell.Value = name;
                headerCell.Style.Font.SetBold();
                headerCell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

                if (state.TeamByCar.TryGetValue(car, out var teamId))
                    headerCell.Style.Fill.BackgroundColor = ColorSchemes.TeamColor(teamId);

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

            BuildLapRows(ws, state, list, drivers, perDriverBest, bestAllS1, bestAllS2, bestAllS3, maxLap);
            BuildVBLRow(ws, state, drivers, perDriverBest, realPosByCar, totalReal, vblPosByCar, totalVbl, maxLap);

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
        }

        private static void BuildLapRows(IXLWorksheet ws, AppState state, List<ParsedRow> list, 
            List<int> drivers, Dictionary<int, (int s1, int s2, int s3)> perDriverBest,
            int bestAllS1, int bestAllS2, int bestAllS3, int maxLap)
        {
            for (int lap = 1; lap <= maxLap; lap++)
            {
                int baseRow = 2 + (lap - 1) * 3;

                ws.Range(baseRow, 1, baseRow + 2, 1).Merge();
                var lapCell = ws.Cell(baseRow, 1);
                lapCell.Value = lap;
                lapCell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;
                lapCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;

                int col = 2;
                foreach (var car in drivers)
                {
                    var r = list.FirstOrDefault(x => x.CarIdx == car && x.Lap == lap);

                    int colTime = col;
                    int colSecs = col + 1;
                    col += 2;

                    ws.Range(baseRow, colTime, baseRow + 2, colTime).Merge();
                    var lapTimeCell = ws.Cell(baseRow, colTime);
                    if (r != null)
                    {
                        lapTimeCell.Value = $"{Helpers.MsToStr(r.LapTimeMs)} ({r.Tyre})"
                                        + (r.WearAvg.HasValue ? $" — {Math.Round(r.WearAvg.Value)}%" : "")
                                        + (string.IsNullOrEmpty(r.ScStatus) ? "" : $" [{r.ScStatus}]");

                        var color = ColorSchemes.GetTyreColor(r.Tyre);
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

                    WriteSector(ws.Cell(baseRow + 0, colSecs), state, r, r?.S1Ms ?? 0, perDriverBest[car].s1, bestAllS1, 1);
                    WriteSector(ws.Cell(baseRow + 1, colSecs), state, r, r?.S2Ms ?? 0, perDriverBest[car].s2, bestAllS2, 2);
                    WriteSector(ws.Cell(baseRow + 2, colSecs), state, r, r?.S3Ms ?? 0, perDriverBest[car].s3, bestAllS3, 3);
                }

                if (lap < maxLap)
                {
                    var bottomRow = ws.Range(baseRow + 2, 1, baseRow + 2, drivers.Count * 2 + 1);
                    bottomRow.Style.Border.BottomBorder = XLBorderStyleValues.Thin;
                    bottomRow.Style.Border.BottomBorderColor = XLColor.Black;
                }
            }
        }

        private static void BuildVBLRow(IXLWorksheet ws, AppState state, List<int> drivers,
            Dictionary<int, (int s1, int s2, int s3)> perDriverBest,
            Dictionary<int, int> realPosByCar, int totalReal,
            Dictionary<int, int> vblPosByCar, int totalVbl, int maxLap)
        {
            int vblRow = 3 * maxLap + 2;
            var vblLapCell = ws.Cell(vblRow, 1);
            vblLapCell.Value = "VBL";
            vblLapCell.Style.Font.SetBold();
            vblLapCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            vblLapCell.Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

            int col = 2;
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
                    vblCell.Value = Helpers.MsToStr(vbl);

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
        }

        private static void WriteSector(IXLCell cell, AppState state, ParsedRow? row, int ms, int bestDriver, int bestAll, int sectorNo)
        {
            if (ms <= 0)
            {
                cell.Value = "";
                return;
            }

            bool valid = (row == null) ? true : ValidationService.IsSectorValid(state, row, sectorNo);

            cell.Value = Helpers.MsToStr(ms);
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
}