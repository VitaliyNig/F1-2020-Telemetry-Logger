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
    internal static class AveragePaceBuilder
    {
        public static void AddAveragePaceSheet(XLWorkbook wb, AppState state, List<ParsedRow> allRows)
        {
            var ws = wb.AddWorksheet("Average Pace");

            var qualiBase = allRows.Where(ValidationService.IsQualiSession).Where(r => r.LapTimeMs > 0).ToList();
            var raceBase = allRows.Where(ValidationService.IsRaceSession).Where(r => r.LapTimeMs > 0).ToList();

            var qualiClean = ValidationService.GetCleanLaps(state, qualiBase);
            var raceClean = ValidationService.GetCleanLaps(state, raceBase);

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
                .Select(g => new { 
                    g.Key.DriverColumn, 
                    g.Key.Tyre, 
                    AvgMs = g.Average(x => x.LapTimeMs), 
                    N = g.Count() 
                })
                .OrderBy(x => x.DriverColumn, StringComparer.OrdinalIgnoreCase)
                .ThenBy(x => Helpers.TyreOrder(x.Tyre))
                .ToList();

            foreach (var x in qualiAvg)
            {
                ws.Cell(r, 1).Value = x.DriverColumn;
                ws.Cell(r, 2).Value = x.Tyre;
                ws.Cell(r, 3).Value = Helpers.MsToStr((int)Math.Round(x.AvgMs));
                var col = ColorSchemes.GetTyreColor(x.Tyre);
                if (col != XLColor.NoColor) 
                    ws.Cell(r, 3).Style.Fill.BackgroundColor = col;
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
                .Select(g => new { 
                    g.Key.DriverColumn, 
                    g.Key.Tyre, 
                    AvgMs = g.Average(x => x.LapTimeMs), 
                    N = g.Count() 
                })
                .OrderBy(x => x.DriverColumn, StringComparer.OrdinalIgnoreCase)
                .ThenBy(x => Helpers.TyreOrder(x.Tyre))
                .ToList();

            foreach (var x in raceAvg)
            {
                ws.Cell(r, 5).Value = x.DriverColumn;
                ws.Cell(r, 6).Value = x.Tyre;
                ws.Cell(r, 7).Value = Helpers.MsToStr((int)Math.Round(x.AvgMs));
                var col = ColorSchemes.GetTyreColor(x.Tyre);
                if (col != XLColor.NoColor) 
                    ws.Cell(r, 7).Style.Fill.BackgroundColor = col;
                ws.Cell(r, 7).CreateComment().AddText($"laps: {x.N}");
                r++;
            }

            ws.Columns().AdjustToContents();
            ws.Rows().AdjustToContents();
        }
    }
}