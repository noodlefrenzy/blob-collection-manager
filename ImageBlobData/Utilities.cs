using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ImageBlobData
{
    public static class Utilities
    {
        /// <summary>
        /// Work on all tasks, but only a max of maxWork at any one time. Complete when all tasks finish. Any errors are aggregated.
        /// </summary>
        /// <param name="maxWork"></param>
        /// <param name="tasks"></param>
        /// <returns>Failed tasks.</returns>
        public static async Task<IEnumerable<Task>> ThrottleWork(int maxWork, IEnumerable<Task> tasks)
        {
            var working = new List<Task>(maxWork);
            var failures = new List<Task>();
            foreach (var task in tasks)
            {
                if (working.Count == maxWork)
                {
                    var completed = await Task.WhenAny(working);
                    working.Remove(completed);
                    if (completed.Status != TaskStatus.RanToCompletion)
                        failures.Add(completed);
                }
                working.Add(task);
            }
            await Task.WhenAll(working);
            foreach (var task in working)
            {
                if (task.IsFaulted) failures.Add(task);
            }

            return failures;
        }

        public static AggregateException AsAggregateException(IEnumerable<Task> failedTasks, Func<Task, string> getMessage)
        {
            return new AggregateException(failedTasks.Select(x => AsException(x, getMessage)));
        }

        public static Exception AsException(Task failedTask, Func<Task, string> getMessage)
        {
            var msg = getMessage == null ? null : getMessage(failedTask);
            if (failedTask.Exception != null) return msg == null ? failedTask.Exception : new Exception(msg, failedTask.Exception);
            if (failedTask.IsCanceled) return msg == null ? new TaskCanceledException() : new TaskCanceledException(msg);

            throw new ArgumentException("Task not failed.");
        }
    }

    public class BlobUploader
    {
        public BlobUploader(CloudBlobContainer container)
        {
            if (container == null) throw new ArgumentNullException("container");
            this.Container = container;
        }

        public CloudBlobContainer Container { get; private set; }

        public Task Upload(string filePath, string blobPath)
        {
            if (string.IsNullOrWhiteSpace(blobPath)) throw new ArgumentNullException("blobPath");
            if (string.IsNullOrWhiteSpace(filePath)) throw new ArgumentNullException("filePath");
            if (!File.Exists(filePath)) throw new ArgumentException("file " + filePath + " must exist");

            var blob = this.Container.GetBlockBlobReference(blobPath);
            return blob.UploadFromFileAsync(filePath, System.IO.FileMode.Open);
        }
    }

    public static class AzureUtilities
    {
        public const string ImageBlobContainerName = "images";
        public const string ImageSetTableName = "imagesets";
        public const string ImageTransformTableName = "imagetransforms";

        // Cache the configuration data.
        private static ConcurrentDictionary<string, string> _ConfigurationEntries = new ConcurrentDictionary<string, string>();

        /// <summary>
        /// Pulls configuration entries from either the CloudConfigurationManager (app/web.config or the cscfg if deployed) or the environment variable of the same name.
        /// </summary>
        /// <param name="name"></param>
        /// <returns>The found value, or null.</returns>
        /// <remarks>Side-effect: Stores the result in the dictionary cache.</remarks>
        public static string FromConfiguration(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentNullException("name");

            return _ConfigurationEntries.GetOrAdd(name, x => CloudConfigurationManager.GetSetting(x) ?? Environment.GetEnvironmentVariable(name));
        }

        public static async Task<CloudBlobContainer> GetImagesBlobContainerAsync(string connectionStringOrKey, string containerName = ImageBlobContainerName)
        {
            if (string.IsNullOrWhiteSpace(containerName)) throw new ArgumentNullException("containerName");

            var blobClient = GetStorageAccount(connectionStringOrKey).CreateCloudBlobClient();

            var container = blobClient.GetContainerReference(containerName);

            // Create the container if it doesn't already exist
            await container.CreateIfNotExistsAsync();

            // Enable public access to blobs but not the full container
            var permissions = await container.GetPermissionsAsync();
            if (permissions.PublicAccess == BlobContainerPublicAccessType.Off)
            {
                permissions.PublicAccess = BlobContainerPublicAccessType.Blob;
                await container.SetPermissionsAsync(permissions);
            }

            return container;
        }

        /// <summary>
        /// Get/create the Azure Table Storage table for image sets.
        /// </summary>
        /// <param name="connectionStringOrKey"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static Task<CloudTable> GetImageSetTable(string connectionStringOrKey, string tableName = ImageSetTableName)
        {
            return GetTable(connectionStringOrKey, tableName);
        }

        public static Task<CloudTable> GetImageTransformTable(string connectionStringOrKey, string tableName = ImageTransformTableName)
        {
            return GetTable(connectionStringOrKey, tableName);
        }

        private static async Task<CloudTable> GetTable(string connectionStringOrKey, string tableName)
        { 
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");

            var tableClient = GetStorageAccount(connectionStringOrKey).CreateCloudTableClient();
            var table = tableClient.GetTableReference(tableName);
            await table.CreateIfNotExistsAsync();

            return table;
        }

        public static CloudStorageAccount GetStorageAccount(string connectionStringOrKey)
        {
            if (string.IsNullOrWhiteSpace(connectionStringOrKey)) throw new ArgumentNullException("connectionStringOrKey");

            var key = AzureUtilities.FromConfiguration(connectionStringOrKey ?? "StorageConnectionString");
            if (key == null)
            {
                // NOTE: In the real world, you'd want to remove this, so you didn't log keys.
                Trace.TraceInformation("Couldn't find '{0}' as setting, assuming it's the actual key.", connectionStringOrKey);
                key = connectionStringOrKey;
            }

            return CloudStorageAccount.Parse(key);
        }
    }

    public static class AzureExtensions
    {
        /// <summary>
        /// For our use here, we're ok ignoring some of these (no files/directories contain crlf), 
        ///  and we assume people haven't created crazy names that'll cause collisions.
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="dirtyKey"></param>
        /// <returns></returns>
        /// <remarks>
        /// https://msdn.microsoft.com/library/azure/dd179338.aspx
        /// Done:
        ///     The forward slash (/) character
        ///     The backslash(\) character
        ///     The number sign(#) character
        ///     The question mark (?) character
        /// 
        /// 
        /// TODO:
        ///     Control characters from U+0000 to U+001F, including:
        ///         The horizontal tab(\t) character
        ///         The linefeed(\n) character
        ///         The carriage return (\r) character
        ///     Control characters from U+007F to U+009F
        /// </remarks>
        public static string CleanPartitionKey(this TableEntity entity, string dirtyKey)
        {
            return dirtyKey.Replace('/', '_').Replace('\\', '_').Replace('#', '_').Replace('?', '_');
        }

        public static string CleanRowKey(this TableEntity entity, string dirtyKey)
        {
            return entity.CleanPartitionKey(dirtyKey);
        }
    }
}
