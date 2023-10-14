using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace dbci.Util
{
    public class DialectSqlGenerator
    {
        public string GenerateOracleInsertAll(string tableName, DataTable dt)
        {
            var sb = new StringBuilder();
            sb.AppendLine("INSERT ALL ");

            var columns = dt.Columns.Cast<DataColumn>().Select(c => $"{c.ColumnName}").ToArray();
            var columnListString = String.Join(",", columns.Select(c => $"\"{c}\""));
            foreach (DataRow row in dt.Rows)
            {
                var valueListString = String.Join(",", columns.Select(c => $"'{row[c].ToString()}'"));
                sb.AppendLine($"INTO {tableName} ({columnListString}) values ({valueListString}) ");
            }

            sb.AppendLine("SELECT * FROM DUAL");

            return sb.ToString();
        }
    }
}
