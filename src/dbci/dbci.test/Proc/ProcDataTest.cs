using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.IO;
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
        public void Test__BulkExport__sqlite3()
        {
            var db = "test-sqlite3";
            using (var conn = DataSourceUtil.Instance.CreateConnection(db))
            {
                conn.Open();
                new ProcData().BulkExport(
                    db,
                    Path.Combine(TestContext.CurrentContext.TestDirectory, "TestResource\\"),
                    conn,
                    new List<string>() { "ITEM", "SALE" });
                conn.Close();
            }
        }

        [Test]
        public void Test__Export__sqlite3__sjis()
        {
            var db = "test-sqlite3";
            using (var conn = DataSourceUtil.Instance.CreateConnection(db))
            {
                conn.Open();
                new ProcData().Export(db, @"ITEM.csv", "SELECT * FROM ITEM", conn, "Shift_JIS");
                new ProcData().Export(db, @"SALE.csv", "SELECT * FROM SALE", conn, "Shift_JIS");
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
                new ProcData().Import(db, @"ITEM.csv", "ITEM", conn, "delete from ITEM");
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
                new ProcData().Import(db, @"ITEM.csv", "ITEM", conn, "delete from ITEM");
                new ProcData().Import(db, @"SALE.csv", "SALE", conn);
                conn.Close();
            }
        }
    }
}