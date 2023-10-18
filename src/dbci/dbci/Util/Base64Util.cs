using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace dbci.Util
{
    public class Base64Util
    {
        public static string ReadWithEncode(string filePath)
        {
            return Convert.ToBase64String(File.ReadAllBytes(filePath));
        }

        public static void SaveWithDecode(string base64Str, string savePath)
        {
            byte[] barray = Convert.FromBase64String(base64Str);
            using (var fs = new FileStream(savePath, FileMode.Create))
            {
                fs.Write(barray, 0, barray.Length);
            }
        }
    }
}
