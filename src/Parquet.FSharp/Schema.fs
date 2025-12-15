module internal rec Parquet.FSharp.Schema

open Parquet.Schema

let private getValueSchema fieldName valueInfo =
    match valueInfo with
    | ValueInfo.Atomic atomicInfo ->
        // TODO: Should we use some of the custom DataField types here, e.g. DecimalDataField?
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
