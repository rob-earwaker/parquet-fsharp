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
    | ValueInfo.Option optionInfo ->
        // TODO: Is there a better way to deal with this nesting?
        let valueField = getValueSchema fieldName optionInfo.ValueInfo
        if valueField.IsNullable
        then valueField
        else
            match valueField with
            | :? DataField as dataField ->
                DataField(dataField.Name, dataField.ClrType, isNullable = true)
            | _ -> failwith "not implemented"

let private getRecordFields (recordInfo: RecordInfo) =
    recordInfo.Fields
    |> Array.map (fun field -> getValueSchema field.Name field.ValueInfo)

let ofRecordInfo recordInfo =
    ParquetSchema(getRecordFields recordInfo)
