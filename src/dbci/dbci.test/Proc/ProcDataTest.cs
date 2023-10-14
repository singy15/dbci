using NUnit.Framework;
using System;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Threading;

namespace dbci.test
{
    public class ProcDataTest
    {
        [SetUp]
        public void Setup()
        {
        }

  
        [Test]
        public void Test__Export__sqlite3()
        {
            var db = "test-sqlite3";
            using (var conn = DataSourceUtil.Instance.CreateConnection(db))
            {
                conn.Open();
                new ProcData().Export(db, @"ITEM.csv", "SELECT * FROM ITEM", conn);
                new ProcData().Export(db, @"SALE.csv", "SELECT * FROM SALE", conn);
                conn.Close();
            }
        }

        [Test]
        public void Test__Export__oracle()
        {
            var db = "test-oracle";
            using (var conn = DataSourceUtil.Instance.CreateConnection(db))
            {
                conn.Open();
                new ProcData().Export(db, @"ITEM.csv", "SELECT * FROM ITEM", conn);
                new ProcData().Export(db, @"SALE.csv", "SELECT * FROM SALE", conn);
                conn.Close();
            }
        }

        [Test]
        public void Test__Import__sqlite3()
        {
            var db = "test-sqlite3";
            using (var conn = DataSourceUtil.Instance.CreateConnection(db))
            {
                conn.Open();
                new ProcData().Export(db, @"ITEM.csv", "SELECT * FROM ITEM", conn);
                new ProcData().Export(db, @"SALE.csv", "SELECT * FROM SALE", conn);
                new ProcData().Import(db, @"ITEM.csv", "ITEM", conn);
                new ProcData().Import(db, @"SALE.csv", "SALE", conn);
                conn.Close();
            }
        }

        [Test]
        public void Test__Import__oracle()
        {
            var db = "test-oracle";
            using (var conn = DataSourceUtil.Instance.CreateConnection(db))
            {
                conn.Open();
                new ProcData().Export(db, @"ITEM.csv", "SELECT * FROM ITEM", conn);
                new ProcData().Export(db, @"SALE.csv", "SELECT * FROM SALE", conn);
                new ProcData().Import(db, @"ITEM.csv", "ITEM", conn);
                new ProcData().Import(db, @"SALE.csv", "SALE", conn);
                conn.Close();
            }
        }
    }
}