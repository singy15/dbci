using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace dbci
{
    public interface IDbUtil
    {
        public IDbTransaction CreateTransaction();

        public DataTable Query(IDbTransaction tx, string sql);

        public IDbConnection Connection { get; }

        public long Insert(IDbTransaction tx, string tableName, Dictionary<string, object> param);

        public int Update(IDbTransaction tx, string tableName, Dictionary<string, object> param, Dictionary<string, object> key);

        public int Delete(IDbTransaction tx, string tableName, Dictionary<string, object> param);
    }

    public class DbUtil : IDbUtil
    {
        public IDbConnection Connection { get; }

        public DbUtil(IDbConnection connection)
        {
            Connection = connection;
        }

        public IDbTransaction CreateTransaction()
        {
            return this.Connection.BeginTransaction();
        }

        public DataTable Query(IDbTransaction tx, string sql)
        {
            using (var cmd = tx.Connection.CreateCommand())
            {
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();
                var dt = new DataTable();
                dt.Load(reader);
                return dt;
            }
        }

        public IDataReader OpenReader(IDbTransaction tx, string sql) { 
            using (var cmd = tx.Connection.CreateCommand())
            {
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();
                return reader;
            }
        }
        public int Execute(IDbTransaction tx, string sql)
        {
            using (var cmd = tx.Connection.CreateCommand())
            {
                cmd.CommandText = sql;
                return cmd.ExecuteNonQuery();
            }
        }


        public long Insert(IDbTransaction tx, string tableName, Dictionary<string, object> param) { 

            using (var cmd = tx.Connection.CreateCommand())
            {
                foreach (string k in param.Keys) {
                    var prm = cmd.CreateParameter();
                    prm.ParameterName = "@" + k;
                    prm.Direction = ParameterDirection.Input;
                    prm.Value = param[k];
                    cmd.Parameters.Add(prm);
                }
                    
                var columns = String.Join(",", param.Keys);
                var values = String.Join(",", param.Keys.Select(k => "@" + k));
                var sql = $"insert into {tableName} ({columns}) values ({values}); select last_insert_rowid() as rowid;";

                cmd.CommandText = sql;
                var rowid = cmd.ExecuteScalar();
                return (Int64)rowid;
            }
        }

        public int Update(IDbTransaction tx, string tableName, Dictionary<string, object> param, Dictionary<string, object> key) { 

            using (var cmd = tx.Connection.CreateCommand())
            {
                foreach (string k in param.Keys) {
                    var prm = cmd.CreateParameter();
                    prm.ParameterName = "@" + k;
                    prm.Direction = ParameterDirection.Input;
                    prm.Value = param[k];
                    cmd.Parameters.Add(prm);
                }

                foreach (string k in key.Keys) {
                    var prm = cmd.CreateParameter();
                    prm.ParameterName = "@key_" + k;
                    prm.Direction = ParameterDirection.Input;
                    prm.Value = key[k];
                    cmd.Parameters.Add(prm);
                }
                    
                var values = String.Join(",", param.Keys.Select(k => k + " = @" + k));
                var conds = String.Join(" and ", key.Keys.Select(k => k + " = @key_" + k));
                var sql = $"update {tableName} set {values} where 1 = 1 and {conds}";

                cmd.CommandText = sql;
                return cmd.ExecuteNonQuery();
            }
        }

        public int Delete(IDbTransaction tx, string tableName, Dictionary<string, object> param) { 

            using (var cmd = tx.Connection.CreateCommand())
            {
                foreach (string k in param.Keys) {
                    var prm = cmd.CreateParameter();
                    prm.ParameterName = "@" + k;
                    prm.Direction = ParameterDirection.Input;
                    prm.Value = param[k];
                    cmd.Parameters.Add(prm);
                }
                    
                var conds = String.Join(" and ", param.Keys.Select(k => k + " = @" + k));
                var sql = $"delete from {tableName} where 1 = 1 and {conds}";

                cmd.CommandText = sql;
                return cmd.ExecuteNonQuery();
            }
        }
    }
}
