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
    internal static class TyreDegradationBuilder
    {
        public static void AddTyreDegradationSheet(XLWorkbook wb, AppState state, List<ParsedRow> allRows)
        {
            var raceData = allRows.Where(ValidationService.IsRaceSession).Where(r => r.WearAvg.HasValue).ToList();
            if (raceData.Count == 0) return;

            var ws = wb.AddWorksheet("Tyre Degradation");
            ulong sessionUid = raceData[0].SessionUID;

            var drivers = raceData.Select(r => r.CarIdx).Distinct().ToList();

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

            var byDriver = raceData.GroupBy(r => r.CarIdx)
                                   .ToDictionary(g => g.Key, g => g.OrderBy(x => x.Lap).ToList());

            var driverStints = DetectStints(byDriver, drivers, state);
            var representativeLaps = CalculateRepresentativeLaps(driverStints, state);

            int maxLap = raceData.Max(r => r.Lap);
            int currentCol = 1;

            BuildHeaders(ws, state, drivers, ref currentCol);
            FillDataRows(ws, state, drivers, byDriver, driverStints, representativeLaps, maxLap);

            ws.SheetView.Freeze(2, 1);
            SetColumnWidths(ws, drivers);
        }

        private static Dictionary<int, List<List<ParsedRow>>> DetectStints(
            Dictionary<int, List<ParsedRow>> byDriver, List<int> drivers, AppState state)
        {
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

                    bool compoundChanged = !string.Equals(curr.Tyre ?? "", prev.Tyre ?? "", StringComparison.OrdinalIgnoreCase);
                    bool ageReset = curr.TyreAge >= 0 && prev.TyreAge >= 0 && curr.TyreAge < prev.TyreAge;
                    bool wearJump = curr.WearAvg.HasValue && prev.WearAvg.HasValue &&
                                   (curr.WearAvg.Value - prev.WearAvg.Value) < Constants.WEAR_JUMP_THRESHOLD;

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

            return driverStints;
        }

        private static Dictionary<int, Dictionary<int, (double time, int index)>> CalculateRepresentativeLaps(
            Dictionary<int, List<List<ParsedRow>>> driverStints, AppState state)
        {
            var representativeLaps = new Dictionary<int, Dictionary<int, (double, int)>>();

            foreach (var (carIdx, stints) in driverStints)
            {
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

            return representativeLaps;
        }

        private static void BuildHeaders(IXLWorksheet ws, AppState state, List<int> drivers, ref int currentCol)
        {
            ws.Range(1, 1, 2, 1).Merge();
            ws.Cell(1, 1).Value = "Lap";
            ws.Cell(1, 1).Style.Font.SetBold();
            ws.Cell(1, 1).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            ws.Cell(1, 1).Style.Alignment.Vertical = XLAlignmentVerticalValues.Center;

            currentCol = 2;
            foreach (var carIdx in drivers)
            {
                string driverName = state.DriverDisplay.TryGetValue(carIdx, out var d) ? d : $"Car [{carIdx}]";

                ws.Range(1, currentCol, 1, currentCol + 3).Merge();
                var headerCell = ws.Cell(1, currentCol);
                headerCell.Value = driverName;
                headerCell.Style.Font.SetBold();
                headerCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;

                if (state.TeamByCar.TryGetValue(carIdx, out var teamId))
                {
                    headerCell.Style.Fill.BackgroundColor = ColorSchemes.TeamColor(teamId);
                }

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
        }

        private static void FillDataRows(IXLWorksheet ws, AppState state, List<int> drivers,
            Dictionary<int, List<ParsedRow>> byDriver,
            Dictionary<int, List<List<ParsedRow>>> driverStints,
            Dictionary<int, Dictionary<int, (double time, int index)>> representativeLaps,
            int maxLap)
        {
            for (int lap = 1; lap <= maxLap; lap++)
            {
                int row = 2 + lap;

                ws.Cell(row, 1).Value = lap;
                ws.Cell(row, 1).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
                ws.Cell(row, 1).Style.Font.SetBold();

                int currentCol = 2;
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
                        int stintNum = -1;
                        int lapInStint = -1;

                        for (int s = 0; s < stints.Count; s++)
                        {
                            var stint = stints[s];
                            var foundIndex = stint.FindIndex(r => r.Lap == lap);
                            if (foundIndex >= 0)
                            {
                                stintNum = s;
                                lapInStint = foundIndex;
                                break;
                            }
                        }

                        double lapTime = lapData.LapTimeMs / 1000.0;
                        double wear = lapData.WearAvg.Value;
                        string tyre = lapData.Tyre ?? "Unknown";

                        FillLapTimeCell(ws, row, currentCol, lapData, tyre);
                        FillDeltaCell(ws, state, row, currentCol + 1, carIdx, stintNum, lapInStint, lapTime, representativeLaps);
                        FillWearCell(ws, row, currentCol + 2, wear, lapData);
                        FillFuelMixCell(ws, row, currentCol + 3, lapData);
                    }

                    currentCol += 4;
                }
            }
        }

        private static void FillLapTimeCell(IXLWorksheet ws, int row, int col, ParsedRow lapData, string tyre)
        {
            var lapTimeCell = ws.Cell(row, col);
            lapTimeCell.Value = $"{Helpers.MsToStr(lapData.LapTimeMs)} ({tyre})";

            var tyreColor = ColorSchemes.GetTyreColor(tyre);
            if (tyreColor != XLColor.NoColor)
            {
                lapTimeCell.Style.Fill.BackgroundColor = tyreColor;
            }
        }

        private static void FillDeltaCell(IXLWorksheet ws, AppState state, int row, int col, 
            int carIdx, int stintNum, int lapInStint, double lapTime,
            Dictionary<int, Dictionary<int, (double time, int index)>> representativeLaps)
        {
            if (stintNum >= 0 && representativeLaps[carIdx].TryGetValue(stintNum, out var repLap))
            {
                double delta = lapTime - repLap.time;
                var deltaCell = ws.Cell(row, col);

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
        }

        private static void FillWearCell(IXLWorksheet ws, int row, int col, double wear, ParsedRow lapData)
        {
            var wearCell = ws.Cell(row, col);
            double wearPercent = 100.0 - wear;
            wearCell.Value = $"{wearPercent:F1}%";
            wearCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;

            var wearColors = new (int maxPercent, string hex)[]
            {
                (100, "#B6F2B6"), (90, "#CFF7B0"), (80, "#E8FCA8"),
                (70, "#FFF7A1"), (60, "#FFE89C"), (50, "#FFD3A1"),
                (40, "#FFBFA1"), (30, "#FFA8A8"), (20, "#FF8A8A"), (10, "#D97A7A")
            };

            foreach (var (maxPercent, hex) in wearColors)
            {
                if (wearPercent <= maxPercent)
                {
                    wearCell.Style.Fill.BackgroundColor = XLColor.FromHtml(hex);
                    break;
                }
            }

            if (lapData.FL.HasValue || lapData.FR.HasValue || lapData.RL.HasValue || lapData.RR.HasValue)
            {
                string fl = lapData.FL.HasValue ? $"{100 - lapData.FL.Value}%" : "?";
                string fr = lapData.FR.HasValue ? $"{100 - lapData.FR.Value}%" : "?";
                string rl = lapData.RL.HasValue ? $"{100 - lapData.RL.Value}%" : "?";
                string rr = lapData.RR.HasValue ? $"{100 - lapData.RR.Value}%" : "?";

                var comment = wearCell.CreateComment();
                comment.AddText($"Wear:\nFL: {fl}  FR: {fr}\nRL: {rl}  RR: {rr}\nAge: {lapData.TyreAge} laps");
            }
        }

        private static void FillFuelMixCell(IXLWorksheet ws, int row, int col, ParsedRow lapData)
        {
            if (lapData.FuelMix.HasValue)
            {
                var mixCell = ws.Cell(row, col);
                mixCell.Value = Helpers.GetFuelMixName(lapData.FuelMix.Value);
                mixCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;

                var mixColor = ColorSchemes.GetFuelMixColor(lapData.FuelMix.Value);
                if (mixColor != XLColor.NoColor)
                {
                    mixCell.Style.Fill.BackgroundColor = mixColor;
                }

                if (!string.IsNullOrEmpty(lapData.FuelMixHistory))
                {
                    var comment = mixCell.CreateComment();
                    comment.AddText($"Mix changes:\n{lapData.FuelMixHistory}");
                }
            }
        }

        private static void SetColumnWidths(IXLWorksheet ws, List<int> drivers)
        {
            ws.Column(1).Width = 5;
            int currentCol = 2;
            foreach (var carIdx in drivers)
            {
                ws.Column(currentCol).Width = 18;
                ws.Column(currentCol + 1).Width = 9;
                ws.Column(currentCol + 2).Width = 9;
                ws.Column(currentCol + 3).Width = 9;
                currentCol += 4;
            }
        }
    }
}