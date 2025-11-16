namespace Parquet.FSharp

open Parquet
open Parquet.Data
open System.IO

type ParquetSerializer =
    static member Serialize<'Record>(records: 'Record seq, stream: Stream) =
        // TODO: Make Async and use cancellation tokens.
        let recordShredder = Dremel.RecordShredder<'Record>()
        use fileWriter = ParquetWriter.CreateAsync(recordShredder.Schema, stream).Result
        // TODO: Support multiple row groups based on configurable size.
        use rowGroupWriter = fileWriter.CreateRowGroup()
        let columns = recordShredder.Shred(records)
        for field, column in Seq.zip recordShredder.Schema.DataFields columns do
            let definitionLevels = Option.toObj column.DefinitionLevels
            let repetitionLevels = Option.toObj column.RepetitionLevels
            let column = DataColumn(field, column.Values, definitionLevels, repetitionLevels)
            rowGroupWriter.WriteColumnAsync(column).Wait()

    static member Deserialize<'Record>(stream: Stream) =
        // TODO: Make Async and use cancellation tokens.
        let recordAssembler = Dremel.RecordAssembler<'Record>()
        use fileReader = ParquetReader.CreateAsync(stream).Result
        // TODO: Check schema compatability.
        // TODO: Support reading multiple row groups.
        use rowGroupReader = fileReader.OpenRowGroupReader(0)
        let columns =
            recordAssembler.Schema.DataFields
            |> Array.map (fun field ->
                let dataColumn = rowGroupReader.ReadColumnAsync(field).Result
                let valueCount = dataColumn.NumValues
                let maxRepetitionLevel = dataColumn.Field.MaxRepetitionLevel
                let maxDefinitionLevel = dataColumn.Field.MaxDefinitionLevel
                // If there are no values in the column then the repetition and
                // definition level arrays won't be initialized and will be NULL
                // even if levels are required. In this situation, ensure the
                // levels are initialized to an empty array to represent the
                // fact that the level arrays exist but are empty.
                let repetitionLevels =
                    if valueCount = 0 && maxRepetitionLevel > 0
                    then Option.Some [||]
                    else Option.ofObj dataColumn.RepetitionLevels
                let definitionLevels =
                    if valueCount = 0 && maxDefinitionLevel > 0
                    then Option.Some [||]
                    else Option.ofObj dataColumn.DefinitionLevels
                { Column.Field = dataColumn.Field
                  Column.ValueCount = valueCount
                  Column.Values = dataColumn.DefinedData
                  Column.MaxRepetitionLevel = maxRepetitionLevel
                  Column.MaxDefinitionLevel = maxDefinitionLevel
                  Column.RepetitionLevels = repetitionLevels
                  Column.DefinitionLevels = definitionLevels })
        recordAssembler.Assemble(columns)
