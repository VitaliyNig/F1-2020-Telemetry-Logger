using System;
using System.Linq;
using ClosedXML.Excel;
using F12020TelemetryLogger.State;

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
            ws.Cell(2, 1).Value = "Driver";
            ws.Cell(2, 2).Value = "Max Speed (km/h)";
            ws.Range(2, 1, 2, 2).Style.Font.SetBold();

            ws.Cell(1, 4).Value = "Race";
            ws.Range(1, 4, 1, 5).Merge().Style.Font.SetBold();
            ws.Cell(2, 4).Value = "Driver";
            ws.Cell(2, 5).Value = "Max Speed (km/h)";
            ws.Range(2, 4, 2, 5).Style.Font.SetBold();

            var quali = state.MaxSpeedQuali
                .Select(kv =>
                {
                    int car = kv.Key; 
                    int vmax = kv.Value;
                    string name = state.DriverDisplay.TryGetValue(car, out var disp) 
                        ? disp 
                        : $"Unknown [{car}]";
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

            var race = state.MaxSpeedRace
                .Select(kv =>
                {
                    int car = kv.Key; 
                    int vmax = kv.Value;
                    string name = state.DriverDisplay.TryGetValue(car, out var disp) 
                        ? disp 
                        : $"Unknown [{car}]";
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
    }
}