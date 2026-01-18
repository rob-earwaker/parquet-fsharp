namespace Parquet.FSharp

open Parquet
open System.IO

// TODO: Support for attributes
// TODO: Support for various serializer options in Parquet.Net.

type ParquetSerializer =
    /// Asynchronously serialize a sequence of records to a stream.
    static member AsyncSerialize<'Record>(records: 'Record seq, stream: Stream) =
        async {
            let shredder = Shredder.createFor<'Record>
            let parquetNetSchema = RootSchema.toParquetNet shredder.Schema
            let! cancellationToken = Async.CancellationToken
            use! fileWriter =
                ParquetWriter.CreateAsync(
                    parquetNetSchema,
                    stream,
                    cancellationToken = cancellationToken)
                |> Async.AwaitTask
            // TODO: Support multiple row groups based on configurable size.
            use rowGroupWriter = fileWriter.CreateRowGroup()
            for column in shredder.Shred(records) do
                do! rowGroupWriter.WriteColumnAsync(column, cancellationToken)
                    |> Async.AwaitTask
        }

    /// Asynchronously serialize a sequence of records as a byte array.
    static member AsyncSerialize<'Record>(records) =
        async {
            use stream = new MemoryStream()
            do! ParquetSerializer.AsyncSerialize<'Record>(records, stream)
            return stream.ToArray()
        }

    /// Serialize a sequence of records to a stream.
    static member Serialize<'Record>(records, stream) =
        ParquetSerializer.AsyncSerialize<'Record>(records, stream)
        |> Async.RunSynchronously

    /// Serialize a sequence of records as a byte array.
    static member Serialize<'Record>(records) =
        ParquetSerializer.AsyncSerialize<'Record>(records)
        |> Async.RunSynchronously

    /// Asynchronously deserialize an array of records from a stream.
    static member AsyncDeserialize<'Record>(stream: Stream) =
        async {
            let! cancellationToken = Async.CancellationToken
            use! fileReader =
                ParquetReader.CreateAsync(
                    stream,
                    cancellationToken = cancellationToken)
                |> Async.AwaitTask
            // TODO: Support reading multiple row groups.
            let fileSchema = RootSchema.ofParquetNet fileReader.Schema
            let assembler = Assembler.createFor<'Record> fileSchema
            use rowGroupReader = fileReader.OpenRowGroupReader(0)
            let! columns =
                // Choose which columns to read based on the assembler schema
                // rather than the file schema. The type we're deserializing
                // into may not include all of the columns. This is what allows
                // for efficient loading of subsets of the file.
                RootSchema.toParquetNet assembler.Schema
                |> fun schema -> schema.DataFields
                |> Array.map (fun field ->
                    rowGroupReader.ReadColumnAsync(field, cancellationToken)
                    |> Async.AwaitTask)
                |> Async.Sequential
            return assembler.Assemble(columns)
        }

    /// Asynchronously deserialize an array of records from a byte array.
    static member AsyncDeserialize<'Record>(bytes: byte[]) =
        async {
            use stream = new MemoryStream(bytes)
            return! ParquetSerializer.AsyncDeserialize<'Record>(stream)
        }

    /// Deserialize an array of records from a stream.
    static member Deserialize<'Record>(stream: Stream) =
        ParquetSerializer.AsyncDeserialize<'Record>(stream)
        |> Async.RunSynchronously

    /// Deserialize an array of records from a byte array.
    static member Deserialize<'Record>(bytes: byte[]) =
        ParquetSerializer.AsyncDeserialize<'Record>(bytes)
        |> Async.RunSynchronously
