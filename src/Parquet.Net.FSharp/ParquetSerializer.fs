namespace Parquet.FSharp

open Parquet
open System.IO

type ParquetSerializer =
    static member Serialize<'Record>(records: 'Record seq, stream: Stream) =
        // TODO: Make Async and use cancellation tokens.
        let recordShredder = DremelExpr.Shredder<'Record>()
        use fileWriter = ParquetWriter.CreateAsync(recordShredder.Schema, stream).Result
        // TODO: Support multiple row groups based on configurable size.
        use rowGroupWriter = fileWriter.CreateRowGroup()
        for column in recordShredder.Shred(records) do
            rowGroupWriter.WriteColumnAsync(column).Wait()

    static member Deserialize<'Record>(stream: Stream) =
        // TODO: Make Async and use cancellation tokens.
        let recordAssembler = Dremel.Assembler<'Record>()
        use fileReader = ParquetReader.CreateAsync(stream).Result
        // TODO: Check schema compatability.
        // TODO: Support reading multiple row groups.
        use rowGroupReader = fileReader.OpenRowGroupReader(0)
        let columns =
            recordAssembler.Schema.DataFields
            |> Array.map (fun field -> rowGroupReader.ReadColumnAsync(field).Result)
        recordAssembler.Assemble(columns)
