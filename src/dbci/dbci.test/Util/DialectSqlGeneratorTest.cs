using dbci.Util;
using NUnit.Framework;
using System;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;

namespace dbci.test
{
    public class DialectSqlGeneratorTest
    {
        [SetUp]
        public void Setup()
        {
        }


        [Test]
        public void Test__GenerateOracleInsertAll()
        {
            var gen = new DialectSqlGenerator();

            DataTable dt = new DataTable();
            dt.Columns.Add("id", typeof(int));
            dt.Columns.Add("name", typeof(string));
            dt.Rows.Add(1, "test1");
            dt.Rows.Add(2, "test2");

            Assert.That(gen.GenerateOracleInsertAll("item", dt), Is.EqualTo(@"INSERT ALL 
INTO item (""id"",""name"") values ('1','test1') 
INTO item (""id"",""name"") values ('2','test2') 
SELECT * FROM DUAL
"));
        }
    }
}