module internal rec Parquet.FSharp.Schema

open Parquet.Schema

let private getValueSchema fieldName valueInfo =
    match valueInfo with
    | ValueInfo.Atomic atomicInfo ->
        // TODO: Should we use some of the custom DataField types here, e.g. DecimalDataField?
        let dataType = atomicInfo.DataDotnetType
        // Nullability in the schema is handled at the {OptionInfo} level, so
        // despite some atomic values being nullable, e.g. {string} and
        // {byte[]}, we specify as not nullable here. Note that if nullability
        // is not explicitly specified then it will be inferred from the data
        // type as part of the {DataField} constructor, so always specify as
        // {false} to avoid this.
        let isNullable = false
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
