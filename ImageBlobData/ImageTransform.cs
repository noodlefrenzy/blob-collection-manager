using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ImageBlobData
{
    public class ImageTransform : TableEntity
    {
        public ImageTransform() { }

        public ImageTransform(string name, string version)
        {
            this.Name = name;
            this.Version = version;

            this.PartitionKey = this.CleanPartitionKey(this.Name);
            this.RowKey = this.CleanRowKey(this.Version);
        }

        public string Name { get; set; }

        public string Version { get; set; }

        public const string InputFile = "{infile}";
        public const string OutputFile = "{outfile}";

        /// <summary>
        /// Command-line arguments format string. {infile} will be replaced with the input file, {outfile} will be replaced with the output file.
        /// </summary>
        public string CommandLineArguments { get; set; }

        public string GetCommandLineArguments(string inputFile, string outputFile)
        {
            return this.CommandLineArguments.Replace(InputFile, inputFile).Replace(OutputFile, outputFile);
        }
    }
}
