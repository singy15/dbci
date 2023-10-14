using NUnit.Framework;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;

namespace dbci.test
{
    public class CreateEnvTest
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        [Ignore("Execute only when you need")]
        public void CreateSQLite3DatabaseForTest()
        {
            using (var conn = DataSourceUtil.Instance.CreateConnection("test-sqlite3"))
            {
                conn.Open();
                conn.Close();
            }
        }
    }
}