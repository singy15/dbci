using NUnit.Framework;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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
                new ProcData().Export(db, @"LGSALE.csv", "SELECT * FROM LGSALE", conn);
                conn.Close();
            }
        }

        [Test]
        public void Test__ExportParallel__oracle()
        {
            var db = "test-oracle";

            var encoding = "Shift_JIS";

            var nThread = 4;
            var connections = new IDbConnection[nThread];
            for (int i = 0; i < nThread; i++)
            {
                connections[i] = DataSourceUtil.Instance.CreateConnection(db);
                connections[i].Open();
            }

            var columns = new string[] {
                "ID","C1","C2","C3","C4","C5","C6","C7","C8","C9","C10","N1","N2","N3","N4","N5","N6","N7","N8","N9","N10"
            };
            var columnCountPerThread = (int)Math.Floor((decimal)(columns.Length) / (decimal)nThread);

            var memoryStreams = new MemoryStream[nThread];
            for (int i = 0; i < nThread; i++)
            {
                memoryStreams[i] = new MemoryStream();
            }

            var threadStates = new int[nThread];
            var threadComplete = 0;
            var rowsWritten = 0;
            var rowsReady = new int[nThread];
            for (int i = 0; i < nThread; i++)
            {
                rowsReady[i] = 0;
                threadStates[i] = 0;
            }
            var buffer = new string[nThread * 1024 * 1024 * 5];
            Parallel.For(0, nThread + 1, x =>
            {
                int tn = x;
                if (tn == nThread)
                {
                    using (var writer = new StreamWriter("0.csv"))
                    {
                        while (threadComplete != nThread)
                        {
                            Thread.Sleep(100);
                            long ready = rowsReady.Min();
                            if (ready <= rowsWritten) { continue; }

                            while (rowsWritten < ready)
                            {
                                for (int i = 0; i < nThread; i++)
                                {
                                    if (i != 0)
                                    {
                                        writer.Write(",");
                                    }
                                    writer.Write(buffer[rowsWritten * nThread + i]);
                                }

                                writer.Write(Environment.NewLine);
                                rowsWritten++;
                            }
                        }
                    }
                }
                else
                {
                    using (var cmd = connections[x].CreateCommand())
                    {
                        string[] columnsOfThread;
                        if (x == (nThread - 1))
                        {
                            columnsOfThread = columns.Skip(x * columnCountPerThread).TakeWhile((s) => { return true; }).ToArray();
                        }
                        else
                        {
                            columnsOfThread = columns.Skip(x * columnCountPerThread).Take(columnCountPerThread).ToArray();
                        }
                        cmd.CommandText = $"SELECT {string.Join(",", columnsOfThread)} FROM LGSALE";
                        new ProcData().ExportParallel(db, memoryStreams[x], connections[x], cmd, buffer, rowsReady, nThread, x, threadStates, encoding);
                    }

                    threadComplete = threadComplete + 1;
                }
            });

            //var streamReaders = new StreamReader[nThread];
            //for (int i = 0; i < nThread; i++)
            //{
            //    memoryStreams[i].Seek(0, SeekOrigin.Begin);
            //    streamReaders[i] = new StreamReader(memoryStreams[i]);
            //}

            //Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            //using (var writer = new StreamWriter(@"0.csv", false, Encoding.GetEncoding(encoding)))
            //{
            //    while (!streamReaders[0].EndOfStream)
            //    {
            //        for (int i = 0; i < nThread; i++)
            //        {
            //            if (i != 0)
            //            {
            //                writer.Write(",");
            //            }
            //            writer.Write(streamReaders[i].ReadLine());
            //        }

            //        writer.Write(Environment.NewLine);
            //    }
            //}
        }


        [Test]
        public void Test__ExportParallel__2__oracle()
        {
            var db = "test-oracle";

            var encoding = "Shift_JIS";

            var nThread = 4;
            var connections = new IDbConnection[nThread];
            for (int i = 0; i < nThread; i++)
            {
                connections[i] = DataSourceUtil.Instance.CreateConnection(db);
                connections[i].Open();
            }

            var columns = new string[] {
                "ID","C1","C2","C3","C4","C5","C6","C7","C8","C9","C10","N1","N2","N3","N4","N5","N6","N7","N8","N9","N10"
            };
            var columnCountPerThread = (int)Math.Floor((decimal)(columns.Length) / (decimal)nThread);

            var memoryStreams = new MemoryStream[nThread];
            for (int i = 0; i < nThread; i++)
            {
                memoryStreams[i] = new MemoryStream();
            }

            Parallel.For(0, nThread, x =>
            {
                using (var cmd = connections[x].CreateCommand())
                {
                    string[] columnsOfThread;
                    if (x == (nThread - 1))
                    {
                        columnsOfThread = columns.Skip(x * columnCountPerThread).TakeWhile((s) => { return true; }).ToArray();
                    }
                    else
                    {
                        columnsOfThread = columns.Skip(x * columnCountPerThread).Take(columnCountPerThread).ToArray();
                    }
                    cmd.CommandText = $"SELECT {string.Join(",", columnsOfThread)} FROM LGSALE";
                    new ProcData().ExportParallel2(db, memoryStreams[x], connections[x], cmd, encoding);
                }
            });

            var streamReaders = new StreamReader[nThread];
            for (int i = 0; i < nThread; i++)
            {
                memoryStreams[i].Seek(0, SeekOrigin.Begin);
                streamReaders[i] = new StreamReader(memoryStreams[i]);
            }

            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            using (var writer = new StreamWriter(@"0.csv", false, Encoding.GetEncoding(encoding)))
            {
                while (!streamReaders[0].EndOfStream)
                {
                    for (int i = 0; i < nThread; i++)
                    {
                        if (i != 0)
                        {
                            writer.Write(",");
                        }
                        writer.Write(streamReaders[i].ReadLine());
                    }

                    writer.Write(Environment.NewLine);
                }
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