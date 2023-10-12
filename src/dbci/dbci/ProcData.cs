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

namespace dbci
{
    public class ProcData
    {
        public int Export(string path, string sql, IDbConnection conn)
        {
            var dbutil = new DbUtil(conn);

            using (var tx = conn.BeginTransaction())
            {
                using (var textWriter = File.CreateText(path))
                using (var csv = new CsvWriter(textWriter, CultureInfo.InvariantCulture))
                {
                    using (var reader = dbutil.OpenReader(tx, sql))
                    {
                        // Get schema
                        var schemaTable = reader.GetSchemaTable();

                        // Write header
                        foreach (DataRow row in schemaTable.Rows)
                        {
                            csv.WriteField(row["ColumnName"]);
                        }
                        csv.NextRecord();

                        // Create DataTable for work
                        DataTable dt = null;
                        dt = new DataTable();
                        foreach (DataRow row in schemaTable.Rows)
                        {
                            dt.Columns.Add((string)row["ColumnName"], (Type)row["DataType"]);
                        }

                        // Write rows
                        while (reader.Read())
                        {
                            foreach (DataRow row in schemaTable.Rows)
                            {
                                Object val = reader[(string)row["ColumnName"]];
                                csv.WriteField(val);
                            }
                            csv.NextRecord();
                        }

                        reader.Close();
                    }
                }
            }

            return 0;
        }

        public int Import(string path, string table, IDbConnection conn)
        {
            var compiler = new SqliteCompiler();

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
                var query = new Query(table).AsInsert(
                    loader.Table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray(),
                    loader.Table.Rows.Cast<DataRow>().Select(r => r.ItemArray).ToArray());
                var sql = compiler.Compile(query).ToString();

                using (var tx = conn.BeginTransaction())
                {
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tx;
                        cmd.CommandText = sql;
                        cmd.ExecuteNonQuery();
                        tx.Commit();
                    }
                }

            }

            return 0;
        }
    }
}
