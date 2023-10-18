/*
 * Oracle SQL*Loader Driver App
 */

using System;
using System.Reflection;
using System.IO;
using System.Net;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Runtime.Serialization.Json;

namespace OracleSqlLoaderDriverApp
{
    using dbci.Util;
    using Elem;
    using System.Diagnostics;

    //class Program {
    //  public static void Main(string[] args) {
    //    WebContext ctx = new WebContext("./context.xml");
    //    ((Server)ctx.GetBean(typeof(Server))).Start(/* PORT */8080);
    //  }
    //}

    [Controller]
    class SqlldrController
    {
        //[Autowired]
        //public IItemService Svc { set; get; }

        [Routing("/sqlldr/hello")]
        public void List(HttpListenerContext context)
        {
            ServerUtil.WriteResponseText(context, "It works!");
        }

        public class Base64EncodedUploadFile
        {
            public string Name { get; set; }

            public string Content { get; set; }
            public string Connstr { get; set; }
        }

        public class ProcResult
        {
            public bool Success { get; set; }

            public string Message { get; set; }

            public int ErrorCode { get; set; }

            public object AdditionalInfo { get; set; }
        }

        public class SqlLoaderResult
        {
            public string Log { get; set; }

            public string BadRecords { get; set; }
        }

        [Routing("/sqlldr/upload", RouteMethod.POST)]
        public void Upload(HttpListenerContext context, [RequestJson] Base64EncodedUploadFile file)
        {
            var base64 = Regex.Replace(file.Content, "^.*,", "");
            var tmpPath = Path.GetTempPath();
            var savePath = Path.Combine(tmpPath, file.Name);

            Base64Util.SaveWithDecode(base64, savePath);

            var ldrutil = new OracleSqlLoaderUtil();
            var controlFilePath = ldrutil.CreateLoadingPackage(
                Path.GetFullPath(savePath),
                Path.GetFileNameWithoutExtension(savePath),
                "",
                false,
                false);
            var parent = Path.GetDirectoryName(controlFilePath);
            var basename = Path.GetFileNameWithoutExtension(controlFilePath);
            var logFilePath = Path.Combine(parent, basename + ".log");
            var badFilePath = Path.Combine(parent, basename + ".bad");

            var connStr = file.Connstr;
            ProcessStartInfo psi = new ProcessStartInfo();
            psi.WorkingDirectory = tmpPath;
            psi.FileName = @"sqlldr";
            psi.Arguments = $"{connStr} control={Path.GetFileName(controlFilePath)} log={Path.GetFileName(logFilePath)} direct=true";
            psi.RedirectStandardOutput = true;
            var p = Process.Start(psi);
            p.WaitForExit();

            string log = "";
            if (File.Exists(logFilePath))
            {
                Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
                using (var reader = new StreamReader(logFilePath, Encoding.GetEncoding("Shift_JIS")))
                {
                    log = reader.ReadToEnd();
                }
            }

            string bad = "";
            if (File.Exists(badFilePath))
            {
                Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
                using (var reader = new StreamReader(badFilePath, Encoding.GetEncoding("Shift_JIS")))
                {
                    bad = reader.ReadToEnd();
                }
            }

            if (File.Exists(savePath)) File.Delete(savePath);
            if (File.Exists(controlFilePath)) File.Delete(controlFilePath);
            if (File.Exists(logFilePath)) File.Delete(logFilePath);
            if (File.Exists(badFilePath)) File.Delete(badFilePath);

            ServerUtil.WriteResponseText(context, ServerUtil.ToJson(
                new ProcResult() { Success = true, Message = "", ErrorCode = 0, AdditionalInfo = new SqlLoaderResult() { Log = log, BadRecords = bad } }));
        }
    }

    //[Controller]
    //class PersonController {
    //  [Autowired]
    //  public IPersonService Svc { set; get; }

    //  [Routing("/person/list")]
    //  public void List(HttpListenerContext context) {
    //    ServerUtil.WriteResponseText(context, ServerUtil.ToJson(Svc.GetList()));
    //  }
    //}

    //interface IItemService {
    //  List<string> GetList();
    //}

    //[Service]
    //class ItemService : IItemService { 
    //  [Autowired]
    //  public RandomUtil Util { set; get; }

    //  public List<string> GetList() {
    //    return Util.CreateRandomItemList("item", 10);
    //  }
    //}

    //interface IPersonService {
    //  List<string> GetList();
    //}

    //[Service]
    //class PersonService : IPersonService { 
    //  [Autowired]
    //  public RandomUtil Util { set; get; }

    //  public List<string> GetList() {
    //    throw new Exception("not implemented");
    //  }
    //}

    //[Service]
    //class MockPersonService : IPersonService { 
    //  public List<string> GetList() {
    //    List<string> ls = new List<string>();
    //    ls.Add("dummy person 1");
    //    ls.Add("dummy person 2");
    //    return ls;
    //  }
    //}

    //[Component]
    //class RandomUtil {
    //  public List<string> CreateRandomItemList(string prefix, int n) {
    //    List<string> itemList = new List<string>();
    //    System.Random rnd = new System.Random();
    //    for(int i = 0; i < n; i++) {
    //      itemList.Add(String.Format("{0}{1}", prefix, rnd.Next(100)));
    //    }

    //    return  itemList;
    //  }
    //}
}

