namespace Parquet.FSharp

open Parquet
open System.IO

// TODO: How will support for Parquet.Net attributes work?

type ParquetSerializer =
    static member Serialize<'Record>(records: 'Record seq, stream: Stream) =
        // TODO: Make Async and use cancellation tokens.
        let shredder = Shredder.of'<'Record>
        use fileWriter = ParquetWriter.CreateAsync(shredder.Schema, stream).Result
        // TODO: Support multiple row groups based on configurable size.
        use rowGroupWriter = fileWriter.CreateRowGroup()
        for column in shredder.Shred(records) do
            rowGroupWriter.WriteColumnAsync(column).Wait()

    static member Deserialize<'Record>(stream: Stream) =
        // TODO: Make Async and use cancellation tokens.
        let assembler = Assembler<'Record>()
        use fileReader = ParquetReader.CreateAsync(stream).Result
        // TODO: Check schema compatability.
        // TODO: Support reading multiple row groups.
        use rowGroupReader = fileReader.OpenRowGroupReader(0)
        let columns =
            assembler.Schema.DataFields
            |> Array.map (fun field -> rowGroupReader.ReadColumnAsync(field).Result)
        assembler.Assemble(columns)
