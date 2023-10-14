using SqlKata;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace dbci.Util
{
    public class CompatibleSqlGenerator
    {
        public string DeleteAll(string database, string tableName)
        {
            return DataSourceUtil.Instance.GetCompiler(database)
                .Compile(new Query(tableName).AsDelete()).ToString();
        }

        public string MultiInsert(string database, string tableName, DataTable dt)
        {
            var provider = DataSourceUtil.Instance.GetProviderName(database);
            string sql = null;
            if (provider == "oracle")
            {
                sql = new DialectSqlGenerator().GenerateOracleInsertAll(tableName, dt);
            }
            else
            {
                sql = DataSourceUtil.Instance.GetCompiler(database)
                    .Compile(new Query(tableName).AsInsert(
                        dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray(),
                        dt.Rows.Cast<DataRow>().Select(r => r.ItemArray).ToArray())).ToString();
            }

            return sql;
        }
    }
}
