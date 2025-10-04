using System;

namespace F12020TelemetryLogger.Utilities
{
    internal static class Log
    {
        public static void Info(string message)
        {
            Console.WriteLine(message);
        }

        public static void Success(string message)
        {
            var prev = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(message);
            Console.ForegroundColor = prev;
        }

        public static void Warn(string message)
        {
            var prev = Console.BackgroundColor;
            Console.BackgroundColor = ConsoleColor.Red;
            Console.WriteLine(message);
            Console.BackgroundColor = prev;
        }
    }
}