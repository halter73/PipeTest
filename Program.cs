using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace PipeTest
{
    class Program
    {
        const int MinimumBufferSize = ushort.MaxValue;

        enum OperationMode
        {
            Pipelines = 1,
            PipelinesSE = 2,
            Custom = 3
        }

        static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine($"Usage: {Process.GetCurrentProcess().MainModule.FileName} [source] [destination] [mode]");
                Environment.Exit(1);

                return;
            }

            if (!File.Exists(args[0]))
            {
                Console.WriteLine($"No such file {args[0]}!");
                Environment.Exit(2);

                return;
            }

            if (!Enum.TryParse<OperationMode>(args[2], true, out var mode))
            {
                Console.WriteLine($"Unknown mode \"{args[2]}\" specified. Please specify either \"pipelines\" or \"custom\".");
                Environment.Exit(3);

                return;
            }

            Console.Write($"Copying {args[0]} to {args[1]} with method {mode}...");

            var sourceStream = File.OpenRead(args[0]);
            var destinationStream = File.OpenWrite(args[1]);
            var stopWatch = new Stopwatch();

            Pipe pipe = null;
            Task readTask = null, writeTask = null;

            stopWatch.Start();
            switch (mode)
            {
                case OperationMode.Pipelines:
                    pipe = new Pipe();

                    stopWatch.Start();
                    writeTask = FillPipeAsync(sourceStream, pipe.Writer);
                    readTask = ReadPipeAsync(destinationStream, pipe.Reader);

                    break;
                case OperationMode.PipelinesSE:
                    pipe = new Pipe();

                    stopWatch.Start();
                    writeTask = FillPipeSeAsync(sourceStream, pipe.Writer);
                    readTask = ReadPipeAsync(destinationStream, pipe.Reader);
                    break;
                case OperationMode.Custom:
                    var col = new BlockingCollection<byte[]>(10);

                    stopWatch.Start();
                    readTask = ReadStreamAsync(col, sourceStream);
                    writeTask = WriteStreamAsync(col, destinationStream);
                    break;
                default:
                    throw new InvalidOperationException();
            }

            Task.WhenAll(readTask, writeTask).Wait();
            stopWatch.Stop();

            Console.WriteLine("done!");
            Console.WriteLine();
            Console.WriteLine($"GetTotalAllocatedBytes(true): [{GC.GetTotalAllocatedBytes(true):n0}] bytes");
            Console.WriteLine($"GetTotalMemory(false): [{GC.GetTotalMemory(false):n0}] bytes");
            Console.WriteLine($"Executed for {stopWatch.ElapsedMilliseconds}ms.");
        }

        static async Task ReadStreamAsync(BlockingCollection<byte[]> col, Stream strm)
        {
            while (true)
            {
                var memory = ArrayPool<byte>.Shared.Rent(MinimumBufferSize);

                var bytesRead = await strm.ReadAsync(memory);

                if (bytesRead == 0)
                    break;

                col.Add(memory);
            }

            col.CompleteAdding();
        }

        static async Task WriteStreamAsync(BlockingCollection<byte[]> col, Stream strm)
        {
            while (true)
            {
                if (col.IsCompleted)
                    break;

                var toWrite = col.Take();

                await strm.WriteAsync(toWrite);

                ArrayPool<byte>.Shared.Return(toWrite, true);
            }
        }

        static async Task FillPipeAsync(Stream strm, PipeWriter writer)
        {
            while (true)
            {
                var memory = writer.GetMemory(MinimumBufferSize);
                var bytesRead = await strm.ReadAsync(memory);

                if (bytesRead == 0)
                    break;

                writer.Advance(bytesRead);

                var result = await writer.FlushAsync();

                if (result.IsCompleted)
                    break;
            }

            writer.Complete();
        }

        static async Task FillPipeSeAsync(Stream strm, PipeWriter writer)
        {
            await strm.CopyToAsync(writer);

            writer.Complete();
        }

        static async Task ReadPipeAsync(Stream strm, PipeReader reader)
        {
            while (true)
            {
                var result = await reader.ReadAsync();

                foreach (var seq in result.Buffer)
                {
                    await strm.WriteAsync(seq);
                }

                reader.AdvanceTo(result.Buffer.End);

                if (result.IsCompleted)
                    break;
            }

            reader.Complete();
        }
    }
}