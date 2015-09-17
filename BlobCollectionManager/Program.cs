using ImageBlobData;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobCollectionManager
{
    class Program
    {
        static void Main(string[] args)
        {
            Trace.Listeners.Add(new ConsoleTraceListener());

            var uploader = new BlobUploader(AzureUtilities.GetImagesBlobContainerAsync("ImageDataConnectionString").Result);
            var imgSetTable = AzureUtilities.GetImageSetTable("ImageDataConnectionString").Result;
            var crawler = new ImageDirectoryCrawler(args[0])
            {
                TagExtractor = x => x.Split('\\'),
                BlobUploader = (f, b) =>
                {
                    var fileinfo = new FileInfo(f);
                    var blobPath = "noodlefrenzy/" + (b.Length > 0 ? (b + "/") : "") + fileinfo.Name;
                    Trace.TraceInformation("Uploading '{0}' to '{1}'", f, blobPath);
                    return uploader.Upload(f, blobPath);
                },
                ImageSetUpserter = imgSet =>
                {
                    var op = TableOperation.InsertOrReplace(imgSet);
                    return imgSetTable.ExecuteAsync(op);
                }
            };
            crawler.WalkTree().Wait();
        }
    }
}
