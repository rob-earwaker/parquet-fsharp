namespace Parquet.FSharp

open Parquet
open Parquet.Data
open Parquet.Schema
open System.IO

module private rec Schema =
    let private getValueSchema fieldName valueInfo =
        match valueInfo with
        | ValueInfo.Atomic atomicInfo ->
            // TODO: The dotnet type is not guaranteed to be a type supported by
            // Parquet.Net. We probably want to change the primitive type
            // definition to mean a type supported by Parquet.Net.
            DataField(fieldName, atomicInfo.DotnetType) :> Field
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
        use rowGroupWriter = fileWriter.CreateRowGroup()
        let columns = Dremel.shred records
        for field, column in Seq.zip schema.DataFields columns do
            let definitionLevels = Option.toObj column.DefinitionLevels
            let repetitionLevels = Option.toObj column.RepetitionLevels
            let column = DataColumn(field, column.Values, definitionLevels, repetitionLevels)
            rowGroupWriter.WriteColumnAsync(column).Wait()
