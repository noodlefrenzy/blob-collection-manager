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
    public delegate IEnumerable<string> PathToTags(string pathSuffix);
    public delegate Task ImageSetUpserter(ImageSet imageSet);
    public delegate Task ImageUploader(string imagePath, string blobLocation);

    /// <summary>
    /// The goal is to walk all subdirectories of a given root, and for each:
    /// - Get the list of "tags" from the directory name (e.g. all/trees/coniferous/my/sub/paths might turn into "trees", "coniferous")
    /// </summary>
    public class ImageDirectoryCrawler
    {
        public const int MaxParallelUploads = 20;
        public const int MaxParallelUpserts = 5;

        public static readonly string[] DefaultExtensions = new[] { ".png", ".gif", ".jpg" };

        public PathToTags TagExtractor { get; set; }

        public ImageSetUpserter ImageSetUpserter { get; set; }

        public ImageUploader BlobUploader { get; set; }

        public ISet<string> Extensions { get; set; }

        public ImageDirectoryCrawler()
        {
            this.Extensions = new HashSet<string>(DefaultExtensions, StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Walk the given directory (and all children) looking for images, upload to blob storage, and store metadata in table storage.
        /// </summary>
        /// <param name="rootDirectory">Directory to walk (recursively).</param>
        /// <param name="imagesVersion">Version to tag to uploaded image sets.</param>
        /// <returns></returns>
        public async Task WalkTree(string rootDirectory, string imagesVersion)
        {
            TestPreconditions(rootDirectory);

            var images = from file in Directory.EnumerateFiles(rootDirectory, "*.*", SearchOption.AllDirectories).Select(x => new FileInfo(x))
                         where this.Extensions.Contains(file.Extension)
                         select file;

            var byDirectory = from img in images
                              group img by img.DirectoryName;

            var uploadTasks = Enumerable.Empty<Tuple<FileInfo, Task>>();
            var imgSets = new List<ImageSet>();
            foreach (var dir in byDirectory)
            {
                var suffix = dir.Key.Length == rootDirectory.Length ? "" : dir.Key.Substring(rootDirectory.Length + 1);
                suffix = suffix.Trim();
                var imgSet = new ImageSet(suffix, imagesVersion)
                {
                    Tags = this.TagExtractor(suffix).Select(x => x.Trim()).Where(x => !string.IsNullOrEmpty(x)).ToList()
                };

                Trace.TraceInformation("New Image Set {0} w/ tags ('{1}')", imgSet.PartitionKey, string.Join("', '", imgSet.Tags));
                uploadTasks = uploadTasks.Concat(dir.Select(file => 
                    Tuple.Create(file, this.BlobUploader(file.FullName, imgSet.BlobPath))));
                imgSets.Add(imgSet);
            }

            var failedUpserts = await Utilities.ThrottleWork(MaxParallelUpserts, imgSets.Select(imgSet => this.ImageSetUpserter(imgSet)));
            var failedUploads = await Utilities.ThrottleWork(MaxParallelUploads, uploadTasks.Select(x => x.Item2));
            if (failedUpserts.Any() || failedUploads.Any())
            {
                var filesByTask = uploadTasks.ToDictionary(x => x.Item2, x => x.Item1);
                throw Utilities.AsAggregateException(failedUploads.Concat(failedUpserts), 
                    t => filesByTask.ContainsKey(t) ? string.Format("Failed upload for '{0}'", filesByTask[t]) : null);
            }
        }

        /// <summary>
        /// Walk the given directory (and all children) looking for images, transform, upload to blob storage, and store metadata in table storage.
        /// </summary>
        /// <param name="rootDirectory">Directory to walk (recursively).</param>
        /// <param name="imagesVersion">Version to tag to uploaded image sets.</param>
        /// <returns></returns>
        /// <remarks>
        /// Side-effect: Transformed images are left on disk in transformedRoot. For us, this is a feature :)
        /// NOTE: Does NOT upsert ImageTransform - it's assumed this is a known transform that already exists.
        /// </remarks>
        public async Task TransformTree(string rootDirectory, string imagesVersion, ImageTransform transform, string imageMagickPath, string transformedRoot)
        {
            TestPreconditions(rootDirectory);

            var images = from file in Directory.EnumerateFiles(rootDirectory, "*.*", SearchOption.AllDirectories).Select(x => new FileInfo(x))
                         where this.Extensions.Contains(file.Extension)
                         select file;

            var byDirectory = from img in images
                              group img by img.DirectoryName;

            var uploadTasks = Enumerable.Empty<Tuple<FileInfo, Task>>();
            var imgSets = new List<ImageSet>();
            foreach (var dir in byDirectory)
            {
                var suffix = dir.Key.Length == rootDirectory.Length ? "" : dir.Key.Substring(rootDirectory.Length + 1);
                suffix = suffix.Trim();
                var transformDir = Path.Combine(transformedRoot, suffix);
                Directory.CreateDirectory(transformDir);

                var imgSet = new ImageSet(suffix, imagesVersion, transform)
                {
                    Tags = this.TagExtractor(suffix).Select(x => x.Trim()).Where(x => !string.IsNullOrEmpty(x)).ToList()
                };

                foreach (var file in dir)
                {
                    var infile = file.FullName;
                    var outfile = Path.Combine(transformDir, file.Name);
                    var cmdline = transform.GetCommandLineArguments(infile, outfile);
                    Trace.TraceInformation("Transforming: '{0} {1}'", imageMagickPath, cmdline);
                    var proc = Process.Start(new ProcessStartInfo()
                    {
                        FileName = imageMagickPath,
                        Arguments = cmdline,
                        UseShellExecute = false,
                        RedirectStandardError = true
                    });
                    proc.WaitForExit();
                    var exitCode = proc.ExitCode;
                    proc.Close();
                    if (exitCode != 0)
                    {
                        Trace.TraceWarning("Failed to execute '{0} {1}': Code {2}", imageMagickPath, cmdline, exitCode);
                    }
                }

                var transformedImages = from file in Directory.EnumerateFiles(transformDir, "*.*", SearchOption.TopDirectoryOnly).Select(x => new FileInfo(x))
                                        where this.Extensions.Contains(file.Extension)
                                        select file;
                if (transformedImages.Any())
                {
                    Trace.TraceInformation("New Image Set {0} w/ tags ('{1}')", imgSet.PartitionKey, string.Join("', '", imgSet.Tags));
                    uploadTasks = uploadTasks.Concat(transformedImages.Select(file =>
                        Tuple.Create(file, this.BlobUploader(file.FullName, imgSet.BlobPath))));
                    imgSets.Add(imgSet);
                }
                else
                {
                    Trace.TraceWarning("No transformed images found for '{0}'", transformDir);
                }
            }

            var failedUpserts = await Utilities.ThrottleWork(MaxParallelUpserts, imgSets.Select(imgSet => this.ImageSetUpserter(imgSet)));
            var failedUploads = await Utilities.ThrottleWork(MaxParallelUploads, uploadTasks.Select(x => x.Item2));
            if (failedUpserts.Any() || failedUploads.Any())
            {
                var filesByTask = uploadTasks.ToDictionary(x => x.Item2, x => x.Item1);
                throw Utilities.AsAggregateException(failedUploads.Concat(failedUpserts),
                    t => filesByTask.ContainsKey(t) ? string.Format("Failed upload for '{0}'", filesByTask[t]) : null);
            }
        }

        private void TestPreconditions(string rootDirectory)
        {
            if (string.IsNullOrWhiteSpace(rootDirectory))
                throw new ArgumentNullException("rootDirectory");
            if (!Directory.Exists(rootDirectory))
                throw new ArgumentException("Directory '" + rootDirectory + "' does not exist.");

            if (string.IsNullOrWhiteSpace(rootDirectory) || !Directory.Exists(rootDirectory)
                || this.Extensions == null || !this.Extensions.Any()
                || this.BlobUploader == null || this.ImageSetUpserter == null || this.TagExtractor == null)
                throw new ArgumentException("Failed to meet preconditions for walking tree.");
        }

    }
}
