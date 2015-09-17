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

        public string RootDirectory { get; private set; }

        public PathToTags TagExtractor { get; set; }

        public ImageSetUpserter ImageSetUpserter { get; set; }

        public ImageUploader BlobUploader { get; set; }

        public ISet<string> Extensions { get; set; }

        public ImageDirectoryCrawler(string directory)
        {
            if (string.IsNullOrWhiteSpace(directory))
                throw new ArgumentNullException("directory");
            if (!Directory.Exists(directory))
                throw new ArgumentException("Directory '" + directory + "' does not exist.");

            this.RootDirectory = directory;
            this.Extensions = new HashSet<string>(DefaultExtensions, StringComparer.OrdinalIgnoreCase);
        }

        public async Task WalkTree()
        {
            if (string.IsNullOrWhiteSpace(this.RootDirectory) || !Directory.Exists(this.RootDirectory)
                || this.Extensions == null || !this.Extensions.Any()
                || this.BlobUploader == null || this.ImageSetUpserter == null || this.TagExtractor == null)
                throw new ArgumentException("Failed to meet preconditions for walking tree.");

            var images = from file in Directory.EnumerateFiles(this.RootDirectory, "*.*", SearchOption.AllDirectories).Select(x => new FileInfo(x))
                         where this.Extensions.Contains(file.Extension)
                         select file;

            var byDirectory = from img in images
                              group img by img.DirectoryName;

            var uploadTasks = Enumerable.Empty<Task>();
            var imgSets = new List<ImageSet>();
            foreach (var dir in byDirectory)
            {
                var suffix = dir.Key.Length == this.RootDirectory.Length ? "" : dir.Key.Substring(this.RootDirectory.Length + 1);
                suffix = suffix.Trim();
                var imgSet = new ImageSet(suffix)
                {
                    Tags = this.TagExtractor(suffix).Select(x => x.Trim()).Where(x => !string.IsNullOrEmpty(x)).ToList()
                };

                Trace.TraceInformation("New Image Set {0} w/ tags ('{1}')", imgSet.PartitionKey, string.Join("', '", imgSet.Tags));
                uploadTasks = uploadTasks.Concat(dir.Select(file => this.BlobUploader(file.FullName, imgSet.BlobPath)));
                imgSets.Add(imgSet);
            }

            await Utilities.ThrottleWork(MaxParallelUpserts, imgSets.Select(imgSet => this.ImageSetUpserter(imgSet)));
            await Utilities.ThrottleWork(MaxParallelUploads, uploadTasks);
        }

    }
}
