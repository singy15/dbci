using NUnit.Framework;
using System;
using System.Configuration;
using System.Data.SQLite;

namespace dbci.test
{
    public class ProcDataTest
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        [Ignore("Execute only when you need")]
        public void CreateDatabaseForTest()
        {
            using (var conn = new SQLiteConnection(GetConnectionStringByName("main")))
            {
                conn.Open();

                var dbutil = new DbUtil(conn);
                using (var tx = conn.BeginTransaction())
                {
                    dbutil.Execute(tx,
@"
CREATE TABLE item (
    id    INTEGER,
    name  INTEGER,
    desc  TEXT,
    flg1  INTEGER,
    PRIMARY KEY(id)
);
                    ");

                    dbutil.Execute(tx,
@"
INSERT INTO item (id, name, desc, flg1) values (1, 'item1', 'desc1', 0);
INSERT INTO item (id, name, desc, flg1) values (2, 'item2', 'desc2', 1);
INSERT INTO item (id, name, desc, flg1) values (3, 'item3', 'desc3', 1);
                    ");


                    dbutil.Execute(tx,
@"
CREATE TABLE seq (
    id    INTEGER
);
                    ");

                    dbutil.Execute(tx,
@"
INSERT INTO seq (id) values (0);
INSERT INTO seq (id) values (1);
INSERT INTO seq (id) values (2);
INSERT INTO seq (id) values (3);
INSERT INTO seq (id) values (4);
INSERT INTO seq (id) values (5);
INSERT INTO seq (id) values (6);
INSERT INTO seq (id) values (7);
INSERT INTO seq (id) values (8);
INSERT INTO seq (id) values (9);
                    ");


                    dbutil.Execute(tx,
@"
CREATE TABLE sale (
	id	INTEGER,
	item_id	INTEGER,
	qty	INTEGER,
	desc	TEXT,
	PRIMARY KEY(id)
);
                    ");

                    dbutil.Execute(tx,
@"
insert into sale (id, item_id, qty, desc)
select
  t1.id * 1000000
+ t2.id * 100000
+ t3.id * 10000
+ t4.id * 1000
+ t5.id * 100
+ t6.id * 10
+ 1,
1,
100,
'desc1'
from seq t1
cross join seq t2
cross join seq t3
cross join seq t4
cross join seq t5
cross join seq t6;
                    ");


                    tx.Commit();

                }

                conn.Close();
            }
        }

        [Test]
        public void Export()
        {
            var fn = new ProcData();
            var sql = "select * from sale";
            using (var conn = new SQLiteConnection(GetConnectionStringByName("main")))
            {
                conn.Open();
                fn.Export(@"out.csv", sql, conn);
                conn.Close();
            }
        }

        [Test]
        public void Export_light()
        {
            var fn = new ProcData();
            var sql = "select * from item";
            using (var conn = new SQLiteConnection(GetConnectionStringByName("main")))
            {
                conn.Open();
                fn.Export(@"item.csv", sql, conn);
                conn.Close();
            }
        }

        [Test]
        public void Import()
        {
            var fn = new ProcData();
            using (var conn = new SQLiteConnection(GetConnectionStringByName("main")))
            {
                conn.Open();
                fn.Import(@"out.csv", "sale", conn);
                conn.Close();
            }
        }

        [Test]
        public void Test_GetConnectionStringByName()
        {
            Console.WriteLine(GetConnectionStringByName("main"));
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