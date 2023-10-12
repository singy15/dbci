using System;
using System.Configuration;
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

            var connStr = GetConnectionStringByName("main");

            app.Command("export", (cmd) =>
            {
                cmd.Description = "Export table data as CSV";
                var argPath = cmd.Argument("Path", "");
                var argQuery = cmd.Argument("Query", "");

                cmd.OnExecute(() =>
                {
                    using (var conn = new SQLiteConnection(connStr))
                    {
                        conn.Open();
                        var proc = new ProcData();
                        proc.Export(argPath.Value, argQuery.Value, conn);
                        conn.Close();
                    }
                    return 0;
                });
            });

            app.Command("import", (cmd) =>
            {
                cmd.Description = "Import table data from CSV";
                var argPath = cmd.Argument("Path", "");
                var argTable = cmd.Argument("Table", "");

                cmd.OnExecute(() =>
                {
                    using (var conn = new SQLiteConnection(connStr))
                    {
                        conn.Open();
                        var proc = new ProcData();
                        proc.Import(argPath.Value, argTable.Value, conn);
                        conn.Close();
                    }
                    return 0;
                });
            });


            app.Execute(args);
        }
        private static string GetConnectionStringByName(string name)
        {
            string returnValue = null;
            ConnectionStringSettings settings = ConfigurationManager.ConnectionStrings[name];
            if (settings != null) returnValue = settings.ConnectionString;
            return returnValue;
        }
    }
}
