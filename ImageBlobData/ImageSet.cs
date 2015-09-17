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
        public ImageSet(string pathSuffix)
        {
            if (pathSuffix == null) throw new ArgumentNullException("pathSuffix");
            this.BlobPath = pathSuffix.Replace('\\', '/');

            this.Path = pathSuffix == "" ? "<root>" : pathSuffix; 
            this.PartitionKey = this.CleanPartitionKey(this.Path);
            this.RowKey = "0";
            this.Tags = new List<string>();
        }

        public string Path { get; set; }

        public List<string> Tags { get; set; }

        public string BlobPath { get; set; }
    }
}
