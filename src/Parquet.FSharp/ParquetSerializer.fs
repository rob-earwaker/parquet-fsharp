namespace Parquet.FSharp

open Parquet
open System.IO

// TODO: Support for attributes
// TODO: Support for various serializer options in Parquet.Net.

type ParquetSerializer =
    static member AsyncSerialize<'Record>(records: 'Record seq, stream: Stream) =
        async {
            let shredder = Shredder.of'<'Record>
            let! cancellationToken = Async.CancellationToken
            use! fileWriter =
                ParquetWriter.CreateAsync(
                    shredder.Schema,
                    stream,
                    cancellationToken = cancellationToken)
                |> Async.AwaitTask
            // TODO: Support multiple row groups based on configurable size.
            use rowGroupWriter = fileWriter.CreateRowGroup()
            for column in shredder.Shred(records) do
                do! rowGroupWriter.WriteColumnAsync(column, cancellationToken)
                    |> Async.AwaitTask
        }

    static member AsyncSerialize<'Record>(records) =
        async {
            use stream = new MemoryStream()
            do! ParquetSerializer.AsyncSerialize<'Record>(records, stream)
            return stream.ToArray()
        }

    static member Serialize<'Record>(records, stream) =
        ParquetSerializer.AsyncSerialize<'Record>(records, stream)
        |> Async.RunSynchronously

    static member Serialize<'Record>(records) =
        ParquetSerializer.AsyncSerialize<'Record>(records)
        |> Async.RunSynchronously

    static member AsyncDeserialize<'Record>(stream: Stream) =
        async {
            let assembler = Assembler.of'<'Record>
            let! cancellationToken = Async.CancellationToken
            use! fileReader =
                ParquetReader.CreateAsync(
                    stream,
                    cancellationToken = cancellationToken)
                |> Async.AwaitTask
            // TODO: Check schema compatability.
            // TODO: Support reading multiple row groups.
            use rowGroupReader = fileReader.OpenRowGroupReader(0)
            let! columns =
                assembler.Schema.DataFields
                |> Array.map (fun field ->
                    rowGroupReader.ReadColumnAsync(field, cancellationToken)
                    |> Async.AwaitTask)
                |> Async.Sequential
            return assembler.Assemble(columns)
        }

    static member AsyncDeserialize<'Record>(bytes: byte[]) =
        async {
            use stream = new MemoryStream(bytes)
            return! ParquetSerializer.AsyncDeserialize<'Record>(stream)
        }

    static member Deserialize<'Record>(stream: Stream) =
        ParquetSerializer.AsyncDeserialize<'Record>(stream)
        |> Async.RunSynchronously

    static member Deserialize<'Record>(bytes: byte[]) =
        ParquetSerializer.AsyncDeserialize<'Record>(bytes)
        |> Async.RunSynchronously
