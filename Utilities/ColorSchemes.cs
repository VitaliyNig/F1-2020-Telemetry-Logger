using ClosedXML.Excel;

namespace F12020TelemetryLogger.Utilities
{
    internal static class ColorSchemes
    {
        public static XLColor GetTyreColor(string tyreCompound)
        {
            if (string.IsNullOrWhiteSpace(tyreCompound)) return XLColor.NoColor;
            
            switch (tyreCompound.Trim().ToUpperInvariant())
            {
                case "SOFT": 
                    return XLColor.FromHtml("#FFC0C0");
                case "MEDIUM": 
                    return XLColor.FromHtml("#FFFACD");
                case "HARD": 
                    return XLColor.FromHtml("#F5F5F5");
                case "INTER": 
                    return XLColor.FromHtml("#CCFFCC");
                case "WET": 
                    return XLColor.FromHtml("#CCE5FF");
                case "VSC":
                case "SC":
                    return XLColor.FromHtml("#1E1E1E");
                default: 
                    return XLColor.NoColor;
            }
        }

        public static XLColor GetFuelMixColor(byte mix) => mix switch
        {
            0 => XLColor.FromHtml("#B8E6F5"), // Lean
            1 => XLColor.FromHtml("#E1E1E1"), // Std
            2 => XLColor.FromHtml("#FFE8B8"), // Rich
            3 => XLColor.FromHtml("#FFB8B8"), // Max
            _ => XLColor.NoColor
        };

        public static XLColor TeamColor(byte teamId)
        {
            return teamId switch
            {
                0 => XLColor.FromHtml("#A6EDE6"),  // Mercedes
                1 => XLColor.FromHtml("#F5A3A3"),  // Ferrari
                2 => XLColor.FromHtml("#A8B6FF"),  // Red Bull
                3 => XLColor.FromHtml("#AFD8FA"),  // Williams
                4 => XLColor.FromHtml("#F9CFE2"),  // Racing Point
                5 => XLColor.FromHtml("#FFE7A8"),  // Renault
                6 => XLColor.FromHtml("#9BB8CC"),  // AlphaTauri
                7 => XLColor.FromHtml("#CFCFCF"),  // Haas
                8 => XLColor.FromHtml("#FFC999"),  // McLaren
                9 => XLColor.FromHtml("#E6A3A3"),  // Alfa Romeo
                10 => XLColor.FromHtml("#9BB8CC"), // Toro Rosso
                11 => XLColor.FromHtml("#F9CFE2"), // Racing Point
                _ => XLColor.FromHtml("#F5F5F5")   // Unknown / F2 / др.
            };
        }
    }
}