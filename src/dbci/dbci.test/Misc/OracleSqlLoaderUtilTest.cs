using dbci.Util;
using NUnit.Framework;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Reflection;

namespace dbci.test
{
    public class OracleSqlLoaderUtilTest
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        [Ignore("Environment dependent")]
        public void Test__Simple1()
        {
            ProcessStartInfo psi = new ProcessStartInfo();
            psi.FileName = @"sqlldr";
            psi.Arguments = @"userid=test1/test1pwd@localhost:1521/DB1 control=load1.ctl log=load1.log";
            psi.RedirectStandardOutput = true;
            var p = Process.Start(psi);
            string output = p.StandardOutput.ReadToEnd();
            Debug.WriteLine(output);
        }

        [Test]
        public void Test__CreateLoadingPackage()
        {
            var ldrutil = new OracleSqlLoaderUtil();
            ldrutil.CreateLoadingPackage(
                Path.Combine(TestContext.CurrentContext.TestDirectory, "TestResource", "ITEM.csv"),
                "ITEM",
                ldrutil.BuildConnectionString("test1", "test1pwd", "localhost", 1521, "DB1"));
        }

        [Test]
        public void Test__CreateLoadingPackage__AbsolutePath()
        {
            var ldrutil = new OracleSqlLoaderUtil();
            ldrutil.CreateLoadingPackage(
                Path.Combine(TestContext.CurrentContext.TestDirectory, "TestResource", "ITEM.csv"),
                "ITEM",
                ldrutil.BuildConnectionString("test1", "test1pwd", "localhost", 1521, "DB1"),
                true);
        }

        [Test]
        public void Test__CreateBulkLoadingPackage()
        {
            var ldrutil = new OracleSqlLoaderUtil();
            ldrutil.BulkCreateLoadingPackage(
                new List<string>() {
                    Path.Combine(TestContext.CurrentContext.TestDirectory, "TestResource", "ITEM.csv"),
                    Path.Combine(TestContext.CurrentContext.TestDirectory, "TestResource", "SALE.csv")
                },
                ldrutil.BuildConnectionString("test1", "test1pwd", "localhost", 1521, "DB1"));
        }
    }
}