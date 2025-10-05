using System;
using System.Linq;
using ClosedXML.Excel;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Excel
{
    internal static class MaxSpeedBuilder
    {
        public static void AddMaxSpeedSheet(XLWorkbook wb, AppState state)
        {
            if (state.MaxSpeedQuali.Count == 0 && state.MaxSpeedRace.Count == 0) return;

            var ws = wb.AddWorksheet("Max Speed");

            ws.Cell(1, 1).Value = "Qualifying";
            ws.Range(1, 1, 1, 2).Merge().Style.Font.SetBold();
            ws.Cell(1, 1).Style.Fill.BackgroundColor = XLColor.FromHtml("#9B3FF5");
            ws.Cell(1, 1).Style.Font.SetFontColor(XLColor.White);
            ws.Cell(1, 1).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            ws.Cell(2, 1).Value = "Driver";
            ws.Cell(2, 2).Value = "Max Speed (km/h)";
            ws.Range(2, 1, 2, 2).Style.Font.SetBold();
            ws.Range(2, 1, 2, 2).Style.Fill.BackgroundColor = XLColor.FromHtml("#E0E0E0");
            ws.Range(2, 1, 2, 2).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;

            ws.Cell(1, 4).Value = "Race";
            ws.Range(1, 4, 1, 5).Merge().Style.Font.SetBold();
            ws.Cell(1, 4).Style.Fill.BackgroundColor = XLColor.FromHtml("#40F57B");
            ws.Cell(1, 4).Style.Font.SetFontColor(XLColor.Black);
            ws.Cell(1, 4).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            ws.Cell(2, 4).Value = "Driver";
            ws.Cell(2, 5).Value = "Max Speed (km/h)";
            ws.Range(2, 4, 2, 5).Style.Font.SetBold();
            ws.Range(2, 4, 2, 5).Style.Fill.BackgroundColor = XLColor.FromHtml("#E0E0E0");
            ws.Range(2, 4, 2, 5).Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;

            var quali = state.MaxSpeedQuali
                .Select(kv =>
                {
                    int car = kv.Key; 
                    int vmax = kv.Value;
                    string name = state.DriverDisplay.TryGetValue(car, out var disp) 
                        ? disp 
                        : $"Unknown [{car}]";
                    return (car, name, vmax);
                })
                .OrderByDescending(x => x.vmax)
                .ThenBy(x => x.name, StringComparer.OrdinalIgnoreCase)
                .ToList();

            int rQ = 3;
            foreach (var x in quali)
            {
                var driverCell = ws.Cell(rQ, 1);
                driverCell.Value = x.name;
                
                if (state.TeamByCar.TryGetValue(x.car, out var teamId))
                    driverCell.Style.Fill.BackgroundColor = ColorSchemes.TeamColor(teamId);

                ws.Cell(rQ, 2).Value = x.vmax;
                rQ++;
            }

            var race = state.MaxSpeedRace
                .Select(kv =>
                {
                    int car = kv.Key; 
                    int vmax = kv.Value;
                    string name = state.DriverDisplay.TryGetValue(car, out var disp) 
                        ? disp 
                        : $"Unknown [{car}]";
                    return (car, name, vmax);
                })
                .OrderByDescending(x => x.vmax)
                .ThenBy(x => x.name, StringComparer.OrdinalIgnoreCase)
                .ToList();

            int rR = 3;
            foreach (var x in race)
            {
                var driverCell = ws.Cell(rR, 4);
                driverCell.Value = x.name;
                
                if (state.TeamByCar.TryGetValue(x.car, out var teamId))
                    driverCell.Style.Fill.BackgroundColor = ColorSchemes.TeamColor(teamId);

                ws.Cell(rR, 5).Value = x.vmax;
                rR++;
            }

            ws.Columns().AdjustToContents();
            ws.Rows().AdjustToContents();
            ws.SheetView.Freeze(2, 0);
        }
    }
}