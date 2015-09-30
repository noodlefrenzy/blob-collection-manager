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
        /// <summary>
        /// These are environment variables you must set - first is the full path to convert.exe (e.g. c:\imgmgk\convert.exe)
        /// </summary>
        private const string ImageMagickPathEnv = "ImageMagickPath";
        /// <summary>
        /// ... and second is the connection string to table/blob storage where you want the results.
        /// </summary>
        private const string ImageDataConnectionEnv = "ImageDataConnectionString";

        static void Main(string[] args)
        {
            Trace.Listeners.Add(new ConsoleTraceListener());

            if (args.Length != 2)
            {
                throw new ArgumentException("Must provide root directory and transform output directory");
            }

            UploadAndTransform(args[0], args[1]).Wait();
        }

        private static async Task UploadAndTransform(string rootDir, string transformDir)
        {
            var uploader = new BlobUploader(await AzureUtilities.GetImagesBlobContainerAsync(ImageDataConnectionEnv));
            var imgSetTable = await AzureUtilities.GetImageSetTable(ImageDataConnectionEnv);
            var imgTransformTable = await AzureUtilities.GetImageTransformTable(ImageDataConnectionEnv);
            var crawler = new ImageDirectoryCrawler()
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

            var imageMagickPath = Environment.GetEnvironmentVariable(ImageMagickPathEnv);
            try
            {
                // Transform to charcoal...
                var transform = new ImageTransform("Charcoal", "0")
                {
                    CommandLineArguments = "-charcoal 2 {infile} {outfile}"
                };
                var upsert = TableOperation.InsertOrReplace(transform);
                await imgTransformTable.ExecuteAsync(upsert);

                await crawler.TransformTree(rootDir, "0", transform, imageMagickPath, transformDir);

                // Upload initial original images.
                await crawler.WalkTree(rootDir, "0");
            }
            catch (AggregateException e)
            {
                Console.WriteLine("Crawling failed:");
                foreach (var ex in e.InnerExceptions)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }
    }
}
