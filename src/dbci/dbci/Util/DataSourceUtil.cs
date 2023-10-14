using SqlKata.Compilers;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Text;

namespace dbci
{
    public class DataSourceUtil
    {
        private static DataSourceUtil _instance;

        private static Dictionary<string, Compiler> _compiler = new Dictionary<string, Compiler>();

        public static DataSourceUtil Instance
        {
            get
            {
                if (_instance == null)
                {
                    RegisterFactories();
                    _instance = new DataSourceUtil();
                }
                return _instance;
            }
        }

        private static void RegisterFactories()
        {
            DbProviderFactories.RegisterFactory("sqlite3", typeof(System.Data.SQLite.SQLiteFactory));
            _compiler.Add("sqlite3", new SqliteCompiler());

            DbProviderFactories.RegisterFactory("oracle", typeof(Oracle.ManagedDataAccess.Client.OracleClientFactory));
            _compiler.Add("oracle", new OracleCompiler());
        }

        public IDbConnection CreateConnection(string name)
        {
            ConnectionStringSettings settings = GetDataSourceSetting(name);
            var conn = DbProviderFactories.GetFactory(settings.ProviderName).CreateConnection();
            conn.ConnectionString = settings.ConnectionString;
            return conn;
        }

        public Compiler GetCompiler(string name) { 
            return _compiler[GetDataSourceSetting(name).ProviderName];
        }

        public string GetProviderName(string name) { 
            return GetDataSourceSetting(name).ProviderName;
        }

        private ConnectionStringSettings GetDataSourceSetting(string name) { 
            ConnectionStringSettings settings = ConfigurationManager.ConnectionStrings[name];
            if (null == settings)
            {
                throw new ArgumentException($"DataSource '{name}' does not registered. Please configuration .config properly.");
            }

            return settings;
        }
    }
}
