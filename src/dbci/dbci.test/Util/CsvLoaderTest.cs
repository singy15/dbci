using dbci.Util;
using NUnit.Framework;
using System;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.IO;

namespace dbci.test
{
    public class CscLoaderTest
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void Test__GetRecords__all()
        {
            using (var loader = new CsvLoader(Path.Combine(TestContext.CurrentContext.TestDirectory, "TestResource", "ITEM.csv"))) {
                loader.GetRecords();
                Assert.That(loader.Table.Rows.Count, Is.EqualTo(3));
            }
        }

        [Test]
        public void Test__GetRecords__part()
        {
            using (var loader = new CsvLoader(Path.Combine(TestContext.CurrentContext.TestDirectory, "TestResource", "ITEM.csv"))) {
                var result = loader.GetRecords(2);
                Assert.That(loader.Table.Rows.Count, Is.EqualTo(2));
                Assert.That(result, Is.EqualTo(true));
            }
        }

        [Test]
        public void Test__GetRecords__one()
        {
            using (var loader = new CsvLoader(Path.Combine(TestContext.CurrentContext.TestDirectory, "TestResource", "ITEM.csv"))) {
                var result = loader.GetRecords(1);
                Assert.That(loader.Table.Rows.Count, Is.EqualTo(1));
                Assert.That(result, Is.EqualTo(true));
            }
        }

        [Test]
        public void Test__GetRecords__2times()
        {
            using (var loader = new CsvLoader(Path.Combine(TestContext.CurrentContext.TestDirectory, "TestResource", "ITEM.csv"))) {
                loader.GetRecords(2);
                var result = loader.GetRecords(2);
                Assert.That(loader.Table.Rows.Count, Is.EqualTo(1));
                Assert.That(result, Is.EqualTo(true));
            }
        }

        [Test]
        public void Test__GetRecords__last()
        {
            using (var loader = new CsvLoader(Path.Combine(TestContext.CurrentContext.TestDirectory, "TestResource", "ITEM.csv"))) {
                loader.GetRecords();
                var result = loader.GetRecords();
                Assert.That(loader.Table.Rows.Count, Is.EqualTo(0));
                Assert.That(result, Is.EqualTo(false));
            }
        }

        [Test]
        public void Test__GetRecords__empty()
        {
            using (var loader = new CsvLoader(Path.Combine(TestContext.CurrentContext.TestDirectory, "TestResource", "ITEM_empty.csv"))) {
                var result = loader.GetRecords();
                Assert.That(loader.Table.Rows.Count, Is.EqualTo(0));
                Assert.That(result, Is.EqualTo(false));
            }
        }
    }
}