﻿using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;

namespace dbci.Util
{
    public class OracleSqlLoaderUtil
    {
        public void CreateControlFile(string destPath, string tableName, int commitPoint, List<string> columns, string infilePath, string badfilePath, int skipRows, bool useAbsolutePath)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            using (var writer = new StreamWriter(destPath, false, Encoding.GetEncoding("Shift_JIS")))
            {
                writer.WriteLine($"OPTIONS(SKIP={skipRows},ERRORS=0,ROWS={commitPoint})");
                writer.WriteLine($"LOAD DATA");
                writer.WriteLine($"INFILE '{((useAbsolutePath) ? infilePath : Path.GetFileName(infilePath))}'");
                writer.WriteLine($"BADFILE '{((useAbsolutePath) ? badfilePath : Path.GetFileName(badfilePath))}'");
                writer.WriteLine($"TRUNCATE");
                writer.WriteLine($"INTO TABLE {tableName}");
                writer.WriteLine($"FIELDS TERMINATED BY \",\"");
                writer.WriteLine($"OPTIONALLY ENCLOSED BY '\"'");
                writer.WriteLine($"(");
                writer.WriteLine($"{String.Join(",", columns)}");
                writer.WriteLine($")");
            }
        }

        public string BuildConnectionString(string user, string password, string host, int port, string database)
        {
            return $"{user}/{password}@{host}:{port}/{database}";
        }

        public void CreateRunnerWindowsBatchFile(string destPath, string connectionString, string controlFilePath, string logFilePath, bool useDirect)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            using (var writer = new StreamWriter(destPath, false, Encoding.GetEncoding("Shift_JIS")))
            {
                writer.WriteLine($"sqlldr userid={connectionString} control={Path.GetFileName(controlFilePath)} log={Path.GetFileName(logFilePath)} direct={useDirect}");
            }
        }

        public string CreateLoadingPackage(string csvFilePath, string tableName, string connectionString, bool useAbsolutePath = false, bool createRunner = true, int skipRows = 1, bool useDirect = true, int commitPoint = 10000)
        {
            using (var loader = new CsvLoader(csvFilePath))
            {
                var columns = loader.Table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
                var parentDirectory = Path.GetDirectoryName(csvFilePath);
                var basename = Path.GetFileNameWithoutExtension(csvFilePath);

                var controlFilePathAbs = Path.Combine(parentDirectory, basename + ".ctl");
                var batchFilePathAbs = Path.Combine(parentDirectory, basename + ".bat");
                var logFilePathAbs = Path.Combine(parentDirectory, basename + ".log");
                var badFilePath = Path.Combine(parentDirectory, basename + ".bad");

                CreateControlFile(controlFilePathAbs, tableName, commitPoint, columns, csvFilePath, badFilePath, skipRows, useAbsolutePath);

                if (createRunner)
                {
                    CreateRunnerWindowsBatchFile(batchFilePathAbs, connectionString, controlFilePathAbs, logFilePathAbs, useDirect);
                }

                return controlFilePathAbs;
            }
        }


        public void BulkCreateLoadingPackage(List<string> csvFilePaths, string connectionString, bool useAbsolutePath = false, int skipRows = 1, bool useDirect = true, int commitPoint = 10000)
        {
            var parentDirectory = Path.GetDirectoryName(csvFilePaths[0]);
            var batchFilePathAbs = Path.Combine(parentDirectory, "load-all.bat");

            foreach (var path in csvFilePaths)
            {
                CreateLoadingPackage(path, Path.GetFileNameWithoutExtension(path), connectionString, useAbsolutePath, true, skipRows, useDirect, commitPoint);
            }

            CreateBulkRunnerWindowsBatchFile(batchFilePathAbs, connectionString, csvFilePaths, useDirect);
        }

        public void CreateBulkRunnerWindowsBatchFile(string destPath, string connectionString, List<string> csvFilePaths, bool useDirect)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            using (var writer = new StreamWriter(destPath, false, Encoding.GetEncoding("Shift_JIS")))
            {
                foreach (var path in csvFilePaths)
                {
                    var parentDirectory = Path.GetDirectoryName(path);
                    var basename = Path.GetFileNameWithoutExtension(path);
                    var controlFilePath = Path.Combine(parentDirectory, basename + ".ctl");
                    var logFilePath = Path.Combine(parentDirectory, basename + ".log");
                    writer.WriteLine($"sqlldr userid={connectionString} control={Path.GetFileName(controlFilePath)} log={Path.GetFileName(logFilePath)} direct={useDirect}");
                }
            }
        }

        public void CreateMultipleRunnerWindowsBatchFile(string destPath, string connectionString, List<string> ctlFilePaths, bool useDirect)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            using (var writer = new StreamWriter(destPath, false, Encoding.GetEncoding("Shift_JIS")))
            {
                foreach (var path in ctlFilePaths)
                {
                    var parentDirectory = Path.GetDirectoryName(path);
                    var basename = Path.GetFileNameWithoutExtension(path);
                    var logFilePath = Path.Combine(parentDirectory, basename + ".log");
                    writer.WriteLine($"sqlldr userid={connectionString} control={Path.GetFileName(path)} log={Path.GetFileName(logFilePath)} direct={useDirect}");
                }
            }
        }

    }
}
