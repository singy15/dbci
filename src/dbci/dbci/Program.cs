using Microsoft.Extensions.CommandLineUtils;
using System;
using System.Configuration;
using System.Data.Common;
using System.Data.SQLite;

namespace dbci
{
    class Program
    {
        static void Main(string[] args)
        {

            var app = new Microsoft.Extensions.CommandLineUtils.CommandLineApplication(false);

            app.Description = "dbci - DataBase Commandline Interface";

            app.HelpOption(template: "-?");

            app.ExtendedHelpText = "Help";

            app.Command("export", (cmd) =>
            {
                cmd.Description = "Export table data as CSV";
                var argDatabase = cmd.Argument("Database", "");
                var argPath = cmd.Argument("Path", "");
                var argQuery = cmd.Argument("Query", "");

                cmd.OnExecute(() =>
                {
                    using (var conn = DataSourceUtil.Instance.CreateConnection(argDatabase.Value))
                    {
                        conn.Open();
                        var proc = new ProcData();
                        proc.Export(argDatabase.Value, argPath.Value, argQuery.Value, conn);
                        conn.Close();
                    }
                    return 0;
                });
            });

            app.Command("import", (cmd) =>
            {
                cmd.Description = "Import table data from CSV";
                var argDatabase = cmd.Argument("Database", "");
                var argPath = cmd.Argument("Path", "");
                var argTable = cmd.Argument("Table", "");
                var optInitScript = cmd.Option("-i|--init", "Init script", CommandOptionType.SingleValue);

                cmd.OnExecute(() =>
                {
                    using (var conn = DataSourceUtil.Instance.CreateConnection(argDatabase.Value))
                    {
                        conn.Open();
                        var proc = new ProcData();
                        proc.Import(argDatabase.Value, argPath.Value, argTable.Value, conn, optInitScript.Value() ?? "");
                        conn.Close();
                    }
                    return 0;
                });
            });


            app.Execute(args);
        }
    }
}
