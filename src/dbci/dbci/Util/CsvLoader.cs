using CsvHelper;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;

namespace dbci
{
    public class CsvLoader : IDisposable
    {
        private StreamReader _streamReader;

        private CsvReader _csvReader;

        public DataTable Table { get; set; }

        public CsvLoader(string path /*, string encode = "UTF-8"*/)
        {
            _streamReader = new StreamReader(path);
            _csvReader = new CsvReader(_streamReader, System.Globalization.CultureInfo.InvariantCulture);
            Table = new DataTable();
        }

        public bool GetRecords(int rows = 0)
        {
            int cnt = 0;
            Table.Clear();
            while (((rows > 0)? cnt < rows : true) && _csvReader.Read())
            {
                var record = _csvReader.GetRecord<dynamic>() as IDictionary<string, object>;

                if (Table.Columns.Count == 0)
                {
                    foreach (var property in record)
                    {
                        Table.Columns.Add(property.Key);
                    }
                }

                var dataRow = Table.NewRow();
                foreach (var property in record)
                {
                    dataRow[property.Key] = property.Value;
                }

                Table.Rows.Add(dataRow);

                cnt++;
            }

            return Table.Rows.Count > 0;
        }

        public void Dispose() {
            _csvReader.Dispose();
        }
    }
}
