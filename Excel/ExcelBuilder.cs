using System;
using System.IO;
using System.Linq;
using ClosedXML.Excel;
using F12020TelemetryLogger.Services;
using F12020TelemetryLogger.State;
using F12020TelemetryLogger.Utilities;

namespace F12020TelemetryLogger.Excel
{
    internal static class ExcelBuilder
    {
        public static void BuildExcel(AppState state, string csvPath, string xlsxPath)
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
                            .Select(CsvService.ParseCsvRow)
                            .Where(r => r != null)
                            .Select(r => r!)
                            .ToList();

            try { CsvService.NormalizeDriverNames(state, rows); } catch { }

            using var wb = new XLWorkbook();

            var sessionTypes = rows.Select(r => r.SessionType).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            
            if (sessionTypes.Contains("ShortQ", StringComparer.OrdinalIgnoreCase))
            {
                QualiSheetBuilder.AddQualiSheet(wb, state, "ShortQ",
                    rows.Where(r => r.SessionType.Equals("ShortQ", StringComparison.OrdinalIgnoreCase)));
            }
            else if (sessionTypes.Contains("OSQ", StringComparer.OrdinalIgnoreCase))
            {
                QualiSheetBuilder.AddQualiSheet(wb, state, "OSQ",
                    rows.Where(r => r.SessionType.Equals("OSQ", StringComparison.OrdinalIgnoreCase)));
            }
            else
            {
                if (sessionTypes.Contains("Q1", StringComparer.OrdinalIgnoreCase))
                    QualiSheetBuilder.AddQualiSheet(wb, state, "Q1",
                        rows.Where(r => r.SessionType.Equals("Q1", StringComparison.OrdinalIgnoreCase)));

                if (sessionTypes.Contains("Q2", StringComparer.OrdinalIgnoreCase))
                    QualiSheetBuilder.AddQualiSheet(wb, state, "Q2",
                        rows.Where(r => r.SessionType.Equals("Q2", StringComparison.OrdinalIgnoreCase)));

                if (sessionTypes.Contains("Q3", StringComparer.OrdinalIgnoreCase))
                    QualiSheetBuilder.AddQualiSheet(wb, state, "Q3",
                        rows.Where(r => r.SessionType.Equals("Q3", StringComparison.OrdinalIgnoreCase)));
            }

            RaceSheetBuilder.AddRaceSheet(wb, state, "Race", 
                rows.Where(r => ValidationService.IsRaceSession(r)));
            
            TyreDegradationBuilder.AddTyreDegradationSheet(wb, state, rows);
            AveragePaceBuilder.AddAveragePaceSheet(wb, state, rows);
            MaxSpeedBuilder.AddMaxSpeedSheet(wb, state);
            IncidentsBuilder.AddIncidentsSheet(wb, state);

            var info = wb.AddWorksheet("Info");
            var (csvFinal, xlsxFinal) = SaveService.GetStagePaths(state);
            info.Cell(1, 1).Value = "CSV"; info.Cell(1, 2).Value = Path.GetFileName(csvFinal);
            info.Cell(2, 1).Value = "XLSX"; info.Cell(2, 2).Value = Path.GetFileName(xlsxFinal);
            info.Cell(3, 1).Value = "Track"; info.Cell(3, 2).Value = state.AnchorTrackName;
            info.Cell(4, 1).Value = "Date"; info.Cell(4, 2).Value = state.AnchorLocalDate;
            info.Cell(5, 1).Value = "SessionUID(anchor)"; info.Cell(5, 2).Value = state.AnchorSessionUID.ToString();

            info.Columns().AdjustToContents();
            wb.SaveAs(xlsxPath);
        }
    }
}