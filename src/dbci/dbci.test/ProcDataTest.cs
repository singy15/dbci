using NUnit.Framework;
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
        public void Export()
        {
            // Assert.Pass();
            var fn = new ProcData();
            var sql = "select * from sale";

            var connStr = "Data Source=C:\\root\\wk\\dbci\\dbci\\test.db";
            using (var conn = new SQLiteConnection(connStr))
            {
                conn.Open();
                fn.Export(@"c:\root\tmp\test.csv", sql, conn);
                conn.Close();
            }
        }

        [Test]
        public void Import()
        {
            // Assert.Pass();
            var fn = new ProcData();

            var connStr = "Data Source=C:\\root\\wk\\dbci\\dbci\\test.db";
            using (var conn = new SQLiteConnection(connStr))
            {
                conn.Open();
                fn.Import(@"c:\root\tmp\sale.csv", "sale", conn);
                conn.Close();
            }
        }
    }
}