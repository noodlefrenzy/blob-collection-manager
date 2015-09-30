using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ImageBlobData
{
    public class ImageSet :TableEntity
    {
        public ImageSet() { }

        /// <summary>
        /// Set of (possibly transformed) images.
        /// </summary>
        /// <remarks>
        /// Blob path is original/_dir_/_image version_, or if transformed it's transform/_transform name_/_transform version_/_dir_/_image version_
        /// </remarks>
        public ImageSet(string pathSuffix, string version, ImageTransform transform = null)
        {
            if (pathSuffix == null) throw new ArgumentNullException("pathSuffix");
            var prefix = transform == null ? "original/" : string.Format("transform/{0}/{1}/", transform.Name, transform.Version);
            this.BlobPath = prefix + pathSuffix.Replace('\\', '/') + "/" + version;

            this.Path = pathSuffix == "" ? "<root>" : pathSuffix;
            this.Version = version;
            this.Tags = new List<string>();

            this.PartitionKey = this.CleanPartitionKey(this.Path);
            this.RowKey = this.CleanRowKey(this.Version);

            if (transform != null)
            {
                this.TransformPartitionKey = transform.PartitionKey;
                this.TransformRowKey = transform.RowKey;
            }
        }

        public string Path { get; set; }

        public string Version { get; set; }

        public List<string> Tags { get; set; }

        public string BlobPath { get; set; }

        /// <summary>
        /// If the images have been transformed, store the PK/RK of the transform pipeline.
        /// </summary>
        public string TransformPartitionKey { get; set; }
        public string TransformRowKey { get; set; }
    }
}
