﻿using dbci.Util;
using Elem;
using Microsoft.Extensions.CommandLineUtils;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

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

            app.Command("exp", (cmd) =>
            {
                cmd.Description = "Export table data as CSV file.";
                cmd.HelpOption(template: "-?|-h|--help");
                var argTarget = cmd.Argument("[target]", "Target database name to connect.");
                var optTable = cmd.Option("-t|--table", "Table to export.", CommandOptionType.SingleValue);
                var optQuery = cmd.Option("-q|--query", "Query to export.", CommandOptionType.SingleValue);
                var optOut = cmd.Option("-o|--out", "Path to output.", CommandOptionType.SingleValue);
                var optEncoding = cmd.Option("-e|--encoding", "Encoding {Shift_JIS|UTF-8}", CommandOptionType.SingleValue);

                cmd.OnExecute(() =>
                {
                    try
                    {
                        if (!(optTable.HasValue() || optQuery.HasValue()))
                        {
                            throw new BusinessLogicException("Either Table(-t) or Query(-q) must be set.");
                        }

                        using (var conn = DataSourceUtil.Instance.CreateConnection(argTarget.Value))
                        {
                            var filename = (optTable.HasValue()) ? $"{optTable.Value()}.csv" : "out.csv";
                            conn.Open();
                            var proc = new ProcData();
                            proc.Export(
                                argTarget.Value,
                                (optOut.HasValue()) ? Path.GetFullPath(optOut.Value()) : Path.Combine(Directory.GetCurrentDirectory(), filename),
                                // TODO: Risk of SQL injection
                                (optQuery.HasValue()) ? optQuery.Value() : $"select * from {optTable.Value()}",
                                conn,
                                ((optEncoding.HasValue()) ? optEncoding.Value() : "UTF-8"));
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

            app.Command("expi", (cmd) =>
            {
                cmd.Description = "Export table data as CSV file.";
                cmd.HelpOption(template: "-?|-h|--help");
                var argTarget = cmd.Argument("[target]", "Target database name to connect.");
                var optEncoding = cmd.Option("-e|--encoding", "Encoding {Shift_JIS|UTF-8}", CommandOptionType.SingleValue);
                var optParallel = cmd.Option("-p|--parallel", "Number of threads for parallel export.", CommandOptionType.SingleValue);

                cmd.OnExecute(() =>
                {
                    try
                    {
                        var breakSignal = "@q";
                        var statusSignal = "@s";

                        var origBackCol = Console.BackgroundColor;
                        var origForeCol = Console.ForegroundColor;

                        int threads = (optParallel.HasValue()) ? int.Parse(optParallel.Value()) : 1;
                        int[] threadStatus = new int[threads];
                        IDbConnection[] threadConnection = new IDbConnection[threads];
                        for (int i = 0; i < threads; i++)
                        {
                            threadStatus[i] = 0;
                            var conn = DataSourceUtil.Instance.CreateConnection(argTarget.Value);
                            conn.Open();
                            threadConnection[i] = conn;
                        }

                        Console.WriteLine("*** dbci interactive export mode started.");
                        Console.WriteLine("*** Enter @q to quit.");
                        Console.WriteLine("");

                        bool interrupt = false;
                        int threadActive = 0;
                        int sleepInterval = 100;
                        while (true)
                        {
                            if (interrupt)
                            {
                                if (threadActive > 0)
                                {
                                    Thread.Sleep(sleepInterval);
                                    continue;
                                }
                                else
                                {
                                    break;
                                }
                            }

                            for (int i = 0; i < threads; i++)
                            {
                                if (threadStatus[i] != 0) { continue; }

                                Console.ForegroundColor = ConsoleColor.Yellow;
                                Console.Write("{0}> ", i + 1);
                                Console.BackgroundColor = origBackCol;
                                Console.ForegroundColor = origForeCol;
                                var filename = Console.ReadLine();
                                if (filename.Trim() == breakSignal)
                                {
                                    interrupt = true;
                                    if (threadActive > 0)
                                    {
                                        Console.WriteLine("Waiting for other threads...");
                                    }
                                    break;
                                }
                                if (filename.Trim() == statusSignal)
                                {
                                    Console.WriteLine("*** Status");
                                    for (int j = 0; j < threads; j++)
                                    {
                                        Console.WriteLine("** THREAD {0} => {1}", j + 1, (threadStatus[j] == 0)? "READY" : "BUSY");
                                    }
                                    break;
                                }

                                Console.ForegroundColor = ConsoleColor.Yellow;
                                Console.Write("? ");
                                Console.BackgroundColor = origBackCol;
                                Console.ForegroundColor = origForeCol;
                                string query = "";
                                bool result = ReadMultiLineQuery(out query, breakSignal);
                                if (!result)
                                {
                                    interrupt = true;
                                    if (threadActive > 0)
                                    {
                                        Console.WriteLine("Waiting for other threads...");
                                    }
                                    break;
                                }

                                int n = i;
                                threadActive = threadActive + 1;
                                threadStatus[n] = 1;
                                var t = Task.Run(() =>
                                {
                                    try
                                    {
                                        var conn = threadConnection[n];
                                        var proc = new ProcData();
                                        proc.Export(
                                            argTarget.Value,
                                            Path.GetFullPath(filename),
                                            // TODO: Risk of SQL injection
                                            query,
                                            conn,
                                            ((optEncoding.HasValue()) ? optEncoding.Value() : "UTF-8"));
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.ForegroundColor = ConsoleColor.Red;
                                        Console.WriteLine("");
                                        Console.WriteLine(ex.Message);
                                        Console.WriteLine("");
                                        Console.ForegroundColor = origForeCol;
                                    }
                                    finally
                                    {
                                        threadStatus[n] = 0;
                                        threadActive = threadActive - 1;
                                    }
                                });
                            }

                            Thread.Sleep(sleepInterval);
                        }

                        for (int i = 0; i < threads; i++)
                        {
                            threadConnection[i].Close();
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

            app.Command("bulkexp", (cmd) =>
            {
                cmd.Description = "Export multiple table data as CSV file.";
                cmd.HelpOption(template: "-?|-h|--help");
                var argTarget = cmd.Argument("[target]", "Target database name to connect.");
                var optTable = cmd.Option("-t|--table", "Tables to export.", CommandOptionType.MultipleValue);
                var optQuery = cmd.Option("-q|--query", "Queries to export.", CommandOptionType.MultipleValue);
                var optOut = cmd.Option("-o|--out", "Path to output.", CommandOptionType.SingleValue);
                var optName = cmd.Option("-n|--name", "Names for query to export.", CommandOptionType.MultipleValue);
                var optEncoding = cmd.Option("-e|--encoding", "Encoding {Shift_JIS|UTF-8}", CommandOptionType.SingleValue);

                cmd.OnExecute(() =>
                {
                    try
                    {
                        if (!(optTable.HasValue() || optQuery.HasValue()))
                        {
                            throw new BusinessLogicException("Either Table(-t) or Query(-q) must be set.");
                        }

                        using (var conn = DataSourceUtil.Instance.CreateConnection(argTarget.Value))
                        {
                            conn.Open();
                            var proc = new ProcData();

                            if (optTable.HasValue())
                            {
                                optTable.Values.ForEach(t =>
                                {
                                    proc.Export(
                                        argTarget.Value,
                                        (optOut.HasValue()) ? Path.Combine(Path.GetFullPath(optOut.Value()), $"{t}.csv") : Path.Combine(Directory.GetCurrentDirectory(), $"{t}.csv"),
                                        // TODO: Risk of SQL injection
                                        $"select * from {t}",
                                        conn,
                                        ((optEncoding.HasValue()) ? optEncoding.Value() : "UTF-8"));
                                });
                            }

                            if (optQuery.HasValue())
                            {
                                var i = 1;
                                optQuery.Values.ForEach((q) =>
                                {
                                    proc.Export(
                                        argTarget.Value,
                                        (optOut.HasValue()) ? Path.Combine(Path.GetFullPath(optOut.Value()), $"out{i}.csv") : Path.Combine(Directory.GetCurrentDirectory(), $"out{i}.csv"),
                                        q,
                                        conn,
                                        ((optEncoding.HasValue()) ? optEncoding.Value() : "UTF-8"));
                                    i++;
                                });
                            }

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

            app.Command("imp", (cmd) =>
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


            app.Command("genctl", (cmd) =>
            {
                cmd.Description = "Generate SQL*Loader control file.";
                cmd.HelpOption(template: "-?|-h|--help");
                var argCsvPath = cmd.Argument("[path]", "Path to CSV file.", true);
                var optExclude = cmd.Option("-x|--exclude", "Exclude path", CommandOptionType.MultipleValue);
                var optAbsolutePath = cmd.Option("-a|--abs", "Use absolute path", CommandOptionType.NoValue);

                cmd.OnExecute(() =>
                {
                    try
                    {
                        var ldrutil = new OracleSqlLoaderUtil();

                        HashSet<string> excludes = new HashSet<string>();
                        if (optExclude.HasValue())
                        {
                            foreach (var path in optExclude.Values)
                            {
                                var fullpath = Path.GetFullPath(path);
                                var parent = Path.GetDirectoryName(fullpath);
                                var filename = Path.GetFileName(fullpath);
                                var files = Directory.GetFiles(parent, filename);
                                foreach (var file in files)
                                {
                                    excludes.Add(file);
                                }
                            }
                        }

                        foreach (var path in argCsvPath.Values)
                        {
                            var fullpath = Path.GetFullPath(path);
                            var parent = Path.GetDirectoryName(fullpath);
                            var filename = Path.GetFileName(fullpath);
                            var files = Directory.GetFiles(parent, filename);

                            foreach (var file in files)
                            {
                                if (excludes.Contains(file)) { continue; }

                                ldrutil.CreateLoadingPackage(
                                    Path.GetFullPath(file),
                                    Path.GetFileNameWithoutExtension(file),
                                    "",
                                    optAbsolutePath.HasValue(),
                                    false);
                            }
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


            app.Command("genrun", (cmd) =>
            {
                cmd.Description = "Generate SQL*Loader runner batch script.";
                cmd.HelpOption(template: "-?|-h|--help");
                var argConnectionString = cmd.Argument("[connection string]", "Connection string. ex. {username}/{password}@{host}:{port}/{database}");
                var argCtlPath = cmd.Argument("[path]", "Path to control file.", true);
                var optExclude = cmd.Option("-x|--exclude", "Exclude path", CommandOptionType.MultipleValue);
                var optOut = cmd.Option("-o|--out", "Output path", CommandOptionType.SingleValue);
                var optAbsolutePath = cmd.Option("-a|--abs", "Use absolute path", CommandOptionType.NoValue);

                cmd.OnExecute(() =>
                {
                    try
                    {
                        var ldrutil = new OracleSqlLoaderUtil();

                        HashSet<string> excludes = new HashSet<string>();
                        if (optExclude.HasValue())
                        {
                            foreach (var path in optExclude.Values)
                            {
                                var fullpath = Path.GetFullPath(path);
                                var parent = Path.GetDirectoryName(fullpath);
                                var filename = Path.GetFileName(fullpath);
                                var files = Directory.GetFiles(parent, filename);
                                foreach (var file in files)
                                {
                                    excludes.Add(file);
                                }
                            }
                        }

                        var ctls = new List<string>();
                        foreach (var path in argCtlPath.Values)
                        {
                            var fullpath = Path.GetFullPath(path);
                            var parent = Path.GetDirectoryName(fullpath);
                            var filename = Path.GetFileName(fullpath);
                            var files = Directory.GetFiles(parent, filename);

                            foreach (var file in files)
                            {
                                if (excludes.Contains(file)) { continue; }
                                ctls.Add(file);
                            }
                        }

                        ldrutil.CreateMultipleRunnerWindowsBatchFile(
                            (optOut.HasValue()) ? Path.GetFullPath(optOut.Value()) : Path.Combine(Directory.GetCurrentDirectory(), $"runner.bat"),
                            argConnectionString.Value,
                            ctls,
                            true);

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

            app.Command("runsvr", (cmd) =>
            {
                cmd.Description = "Start SQL*Loader driver server.";
                cmd.HelpOption(template: "-?|-h|--help");

                cmd.OnExecute(() =>
                {
                    try
                    {
                        WebContext ctx = new WebContext(Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "context.xml"));
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

        static bool ReadMultiLineQuery(out string query, string interruptSignal)
        {
            var text = "";
            while (true)
            {
                var input = Console.ReadLine();
                if (input.Trim() == interruptSignal)
                {
                    query = "";
                    return false;
                }
                text = text + input + "\n";

                if (text.Trim().EndsWith(";"))
                {
                    text = text.Substring(0, text.Length - 2);
                    break;
                }
            }

            query = text;
            return true;
        }
    }
}
