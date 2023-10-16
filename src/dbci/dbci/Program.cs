using dbci.Util;
using Elem;
using Microsoft.Extensions.CommandLineUtils;
using System;
using System.Configuration;
using System.Data.Common;
using System.Data.SQLite;
using System.IO;
using System.Linq;

namespace dbci
{
    class Program
    {
        static void Main(string[] args)
        {
            var app = new CommandLineApplication(false);

            app.Name = "dbci";
            app.Description = "dbci - DataBase Commandline Interface";
            app.HelpOption(template: "-?|-h|--help");
            app.ExtendedHelpText = "";

            app.Command("export", (cmd) =>
            {
                cmd.Description = "Export table data as CSV file.";
                cmd.HelpOption(template: "-?|-h|--help");
                var argDatabase = cmd.Argument("[target]", "Target database name to connect.");
                var argPath = cmd.Argument("[path]", "CSV file path to export.");
                var argQuery = cmd.Argument("[query]", "Query to get output data.");
                var optEncoding = cmd.Option("-e|--encoding", "Encoding {Shift_JIS|UTF-8}", CommandOptionType.SingleValue);

                cmd.OnExecute(() =>
                {
                    try
                    {
                        using (var conn = DataSourceUtil.Instance.CreateConnection(argDatabase.Value))
                        {
                            conn.Open();
                            var proc = new ProcData();
                            proc.Export(argDatabase.Value, argPath.Value, argQuery.Value, conn, ((optEncoding.HasValue())? optEncoding.Value() : "UTF-8"));
                            conn.Close();
                        }
                        return 0;
                    }
                    catch (BusinessLogicException ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"ERROR: {ex.Message}");
                        Console.ResetColor();
                        return 1;
                    }
                });
            });

            app.Command("bulk-export", (cmd) =>
            {
                cmd.Description = "Export multiple table data as CSV file.";
                cmd.HelpOption(template: "-?|-h|--help");
                var argDatabase = cmd.Argument("[target]", "Target database name to connect.");
                var argPath = cmd.Argument("[path]", "Output directory path.");
                var argTables = cmd.Argument("[tables]", "Tables to output. Ex. TBLA TBLB", true);
                var optEncoding = cmd.Option("-e|--encoding", "Encoding {Shift_JIS|UTF-8}", CommandOptionType.SingleValue);

                cmd.OnExecute(() =>
                {
                    try
                    {
                        using (var conn = DataSourceUtil.Instance.CreateConnection(argDatabase.Value))
                        {
                            conn.Open();
                            var proc = new ProcData();
                            proc.BulkExport(argDatabase.Value, Path.GetFullPath(argPath.Value), conn, argTables.Values, ((optEncoding.HasValue()) ? optEncoding.Value() : "UTF-8"));
                            conn.Close();
                        }
                        return 0;
                    }
                    catch (BusinessLogicException ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"ERROR: {ex.Message}");
                        Console.ResetColor();
                        return 1;
                    }
                });
            });

            app.Command("import", (cmd) =>
            {
                cmd.Description = "Import table data from CSV file.";
                cmd.HelpOption(template: "-?|-h|--help");
                var argDatabase = cmd.Argument("[target]", "Target database name to connect.");
                var argPath = cmd.Argument("[path]", "CSV file path to import.");
                var argTable = cmd.Argument("[table name]", "Target table name.");
                var optInitScript = cmd.Option("-i|--init", "Init script.", CommandOptionType.SingleValue);

                cmd.OnExecute(() =>
                {
                    try
                    {
                        using (var conn = DataSourceUtil.Instance.CreateConnection(argDatabase.Value))
                        {
                            conn.Open();
                            var proc = new ProcData();
                            proc.Import(argDatabase.Value, argPath.Value, argTable.Value, conn, optInitScript.Value() ?? "");
                            conn.Close();
                        }
                        return 0;
                    }
                    catch (BusinessLogicException ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"ERROR: {ex.Message}");
                        Console.ResetColor();
                        return 1;
                    }
                });
            });

            app.Command("gen-sqlldr-ctl", (cmd) =>
            {
                cmd.Description = "Generate SQL*Loader control file and runner batch script.";
                cmd.HelpOption(template: "-?|-h|--help");
                var argCsvPath = cmd.Argument("[path]", "Path to CSV file.");
                var argTable = cmd.Argument("[table name]", "Destination table name.");
                var argConnectionString = cmd.Argument("[connection string]", "Connection string. ex. {username}/{password}@{host}:{port}/{database}");
                var optAbsolutePath = cmd.Option("-a|--abs", "Use absolute path", CommandOptionType.NoValue);

                cmd.OnExecute(() =>
                {
                    try
                    {
                        var ldrutil = new OracleSqlLoaderUtil();
                        ldrutil.CreateLoadingPackage(
                            Path.GetFullPath(argCsvPath.Value),
                            argTable.Value,
                            argConnectionString.Value,
                            optAbsolutePath.HasValue());
                        return 0;
                    }
                    catch (BusinessLogicException ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"ERROR: {ex.Message}");
                        Console.ResetColor();
                        return 1;
                    }
                });
            });

            app.Command("bulk-gen-sqlldr-ctl", (cmd) =>
            {
                cmd.Description = "Generate multiple SQL*Loader control files and runner batch scripts.";
                cmd.HelpOption(template: "-?|-h|--help");
                var argConnectionString = cmd.Argument("[connection string]", "Connection string. ex. {username}/{password}@{host}:{port}/{database}");
                var argCsvPath = cmd.Argument("[paths]", "Path to CSV files.", true);
                var optAbsolutePath = cmd.Option("-a|--abs", "Use absolute path", CommandOptionType.NoValue);

                cmd.OnExecute(() =>
                {
                    try
                    {
                        var ldrutil = new OracleSqlLoaderUtil();
                        ldrutil.BulkCreateLoadingPackage(
                            argCsvPath.Values.Select(p => Path.GetFullPath(p)).ToList(),
                            argConnectionString.Value,
                            optAbsolutePath.HasValue());
                        return 0;
                    }
                    catch (BusinessLogicException ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"ERROR: {ex.Message}");
                        Console.ResetColor();
                        return 1;
                    }
                });
            });

            app.Command("start-sqlldr-server", (cmd) =>
            {
                cmd.Description = "Start SQL*Loader driver server.";
                cmd.HelpOption(template: "-?|-h|--help");

                cmd.OnExecute(() =>
                {
                    try
                    {
                        WebContext ctx = new WebContext("./context.xml");
                        ((Server)ctx.GetBean(typeof(Server))).Start(/* PORT */8080);
                        return 0;
                    }
                    catch (BusinessLogicException ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"ERROR: {ex.Message}");
                        Console.ResetColor();
                        return 1;
                    }
                });
            });

            app.Execute(args);
        }
    }
}
