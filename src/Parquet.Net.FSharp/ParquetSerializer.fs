namespace Parquet.FSharp

open Parquet
open Parquet.Data
open Parquet.Schema
open System.IO

module private rec Schema =
    let private getValueSchema fieldName valueInfo =
        match valueInfo with
        | ValueInfo.Atomic atomicInfo ->
            let dataType = atomicInfo.DataDotnetType
            let isNullable = atomicInfo.IsOptional
            DataField(fieldName, dataType, isNullable) :> Field
        | ValueInfo.List listInfo ->
            let element = getValueSchema ListField.ElementName listInfo.ElementInfo
            ListField(fieldName, element) :> Field
        | ValueInfo.Record recordInfo ->
            let fields = getRecordFields recordInfo
            StructField(fieldName, fields) :> Field

    let private getRecordFields (recordInfo: RecordInfo) =
        recordInfo.Fields
        |> Array.map (fun field -> getValueSchema field.Name field.ValueInfo)

    let ofRecordInfo recordInfo =
        ParquetSchema(getRecordFields recordInfo)

type ParquetSerializer =
    static member Serialize<'Record>(records: 'Record seq, stream: Stream) =
        // TODO: Make Async and use cancellation tokens.
        let recordInfo = RecordInfo.ofRecord typeof<'Record>
        let schema = Schema.ofRecordInfo recordInfo
        use fileWriter = ParquetWriter.CreateAsync(schema, stream).Result
        // TODO: Support multiple row groups based on configurable size.
        use rowGroupWriter = fileWriter.CreateRowGroup()
        let columns = Dremel.shred records
        for field, column in Seq.zip schema.DataFields columns do
            let definitionLevels = Option.toObj column.DefinitionLevels
            let repetitionLevels = Option.toObj column.RepetitionLevels
            let column = DataColumn(field, column.Values, definitionLevels, repetitionLevels)
            rowGroupWriter.WriteColumnAsync(column).Wait()

    static member Deserialize<'Record>(stream: Stream) =
        // TODO: Make Async and use cancellation tokens.
        let recordInfo = RecordInfo.ofRecord typeof<'Record>
        let schema = Schema.ofRecordInfo recordInfo
        use fileReader = ParquetReader.CreateAsync(stream).Result
        // TODO: Check schema compatability.
        // TODO: Support reading multiple row groups.
        use rowGroupReader = fileReader.OpenRowGroupReader(0)
        let columns =
            schema.DataFields
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
                { Column.ValueCount = valueCount
                  Column.Values = dataColumn.DefinedData
                  Column.MaxRepetitionLevel = maxRepetitionLevel
                  Column.MaxDefinitionLevel = maxDefinitionLevel
                  Column.RepetitionLevels = repetitionLevels
                  Column.DefinitionLevels = definitionLevels })
        Dremel.assemble<'Record> columns
