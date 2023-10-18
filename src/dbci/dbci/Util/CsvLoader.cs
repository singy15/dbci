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
            Table = new DataTable();
            _streamReader = new StreamReader(path);
            _csvReader = new CsvReader(_streamReader, System.Globalization.CultureInfo.InvariantCulture);

            _csvReader.Read();
            _csvReader.ReadHeader();
            foreach (var column in _csvReader.HeaderRecord)
            {
                Table.Columns.Add(column);
            }
        }

        public bool GetRecords(int rows = 0)
        {
            int cnt = 0;
            Table.Clear();
            while (((rows > 0) ? cnt < rows : true) && _csvReader.Read())
            {
                var record = _csvReader.GetRecord<dynamic>() as IDictionary<string, object>;

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
