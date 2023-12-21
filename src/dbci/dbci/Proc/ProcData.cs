using CsvHelper;
using SqlKata;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Globalization;
using System.IO;
using System.Text;
using System.Linq;
using SqlKata.Compilers;
using System.Dynamic;
using dbci.Util;
using System.Diagnostics;
using Oracle.ManagedDataAccess.Client;
using CsvHelper.Configuration;

namespace dbci
{
    public class ProcData
    {
        public int Export(string database, string path, string sql, IDbConnection conn, string encoding = "UTF-8")
        {
            using (var tx = conn.BeginTransaction())
            {

                var lineEndingChars = new char[] { '\r', '\n' };
                var config = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    ShouldQuote = (ShouldQuoteArgs args) =>
                    {
                        var rowConfig = args.Row.Configuration;
                        return args.Field.Contains(rowConfig.Quote)
                        || args.Field[0] == ' '
                        || args.Field[args.Field.Length - 1] == ' '
                        || (rowConfig.Delimiter.Length > 0 && args.Field.Contains(rowConfig.Delimiter))
                        || !rowConfig.IsNewLineSet && args.Field.IndexOfAny(lineEndingChars) > -1
                        || rowConfig.IsNewLineSet && args.Field.Contains(rowConfig.NewLine);
                    }
                };

                Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
                using (var textWriter = new StreamWriter(path, false, Encoding.GetEncoding(encoding)))
                using (var csv = new CsvWriter(textWriter, config))
                using (var cmd = tx.Connection.CreateCommand())
                {
                    cmd.CommandText = sql;

                    csv.Context.TypeConverterOptionsCache.GetOptions<DateTime>().Formats = new string[] { "o" };

                    using (var reader = cmd.ExecuteReader())
                    {
                        var provider = DataSourceUtil.Instance.GetProviderName(database);

                        if (provider == "oracle")
                        {
                            ((OracleDataReader)reader).FetchSize = ((OracleCommand)cmd).RowSize * 10000;
                        }

                        // Get schema
                        var schemaTable = reader.GetSchemaTable();

                        var ncolumns = schemaTable.Rows.Count;
                        var columns = new string[ncolumns];

                        for (int i = 0; i < ncolumns; i++)
                        {
                            columns[i] = (string)schemaTable.Rows[i]["ColumnName"];
                        }

                        // Write header
                        for (int i = 0; i < ncolumns; i++)
                        {
                            csv.WriteField(columns[i]);
                        }
                        csv.NextRecord();

                        // Write rows
                        while (reader.Read())
                        {
                            for (int i = 0; i < ncolumns; i++)
                            {
                                csv.WriteField(reader.GetValue(i));
                            }
                            csv.NextRecord();
                        }

                        reader.Close();

                        csv.Flush();
                    }
                }
            }

            return 0;
        }

        public int Import(string database, string path, string table, IDbConnection conn, string initScript = "")
        {
            var sqlgen = new CompatibleSqlGenerator();

            using (var tx = conn.BeginTransaction())
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;
                    var sql = (initScript != "") ? initScript : sqlgen.DeleteAll(database, table);
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                    tx.Commit();
                }
            }

            var batchSize = 100;
            var n = 0;
            var startDateTime = DateTime.Now;
            using (var loader = new CsvLoader(path))
            {
                while (loader.GetRecords(batchSize))
                {
                    using (var tx = conn.BeginTransaction())
                    {
                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.Connection = conn;
                            cmd.Transaction = tx;
                            cmd.CommandText = sqlgen.MultiInsert(database, table, loader.Table);
                            cmd.ExecuteNonQuery();
                            tx.Commit();
                        }
                    }

#if DEBUG
                    var sec = DateTime.Now.Subtract(startDateTime).Seconds;
                    if (sec > 0)
                    {
                        Debug.WriteLine($"{(n * batchSize) / sec} rows per sec");
                    }
#endif

                    n++;
                }
            }

            return 0;
        }

        public int ImportSlow(string database, string path, string table, IDbConnection conn)
        {
            var compiler = DataSourceUtil.Instance.GetCompiler(database);

            using (var tx = conn.BeginTransaction())
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;
                    var sql = compiler.Compile(new Query(table).AsDelete()).ToString();
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                    tx.Commit();
                }
            }

            var loader = new CsvLoader(path);
            while (loader.GetRecords(1000))
            {
                using (var tx = conn.BeginTransaction())
                {
                    var columns = loader.Table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();
                    foreach (DataRow row in loader.Table.Rows)
                    {
                        var query = new Query(table).AsInsert(columns, row.ItemArray);
                        var sql = compiler.Compile(query).ToString();

                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.Connection = conn;
                            cmd.Transaction = tx;
                            cmd.CommandText = sql;
                            cmd.ExecuteNonQuery();
                        }

                    }
                    tx.Commit();
                }
            }

            return 0;
        }

        public void BulkExport(string database, string dirPath, IDbConnection conn, List<string> tables, string encoding = "UTF-8")
        {
            foreach (var tbl in tables)
            {
                Export(database, Path.Combine(Path.GetDirectoryName(Path.GetFullPath(dirPath)), tbl + ".csv"), $"select * from {tbl}", conn, encoding);
            }
        }

    }
}
