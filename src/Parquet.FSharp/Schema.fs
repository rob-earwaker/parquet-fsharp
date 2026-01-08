module internal rec Parquet.FSharp.Schema

open Parquet.Schema
open System

// TODO: Is this useful?
type ValueSchema =
    | Atomic of AtomicSchema
    | List of ListSchema
    | Record of RecordSchema
    with
    member this.IsOptional =
        match this with
        | ValueSchema.Atomic atomic -> atomic.IsOptional
        | ValueSchema.List list -> list.IsOptional
        | ValueSchema.Record record -> record.IsOptional

type AtomicSchema = {
    IsOptional: bool
    PrimitiveType: Type }

type ListSchema = {
    IsOptional: bool
    ElementSchema: ValueSchema }

type FieldSchema = {
    Name: string
    ValueSchema: ValueSchema }

type RecordSchema = {
    IsOptional: bool
    FieldSchemas: FieldSchema[] }

let private getValueSchema fieldName valueConverter =
    match valueConverter with
    | ValueConverter.Atomic atomicConverter ->
        // TODO: Should we use some of the custom DataField types here, e.g. DecimalDataField?
        let dataType = atomicConverter.DataDotnetType
        // Nullability in the schema is handled at the {OptionalConverter} level, so
        // despite some atomic values being nullable, e.g. {string} and
        // {byte[]}, we specify as not nullable here. Note that if nullability
        // is not explicitly specified then it will be inferred from the data
        // type as part of the {DataField} constructor, so always specify as
        // {false} to avoid this.
        let isNullable = false
        DataField(fieldName, dataType, isNullable) :> Field
    | ValueConverter.List listConverter ->
        let element = getValueSchema ListField.ElementName listConverter.ElementConverter
        ListField(fieldName, element) :> Field
    | ValueConverter.Record recordConverter ->
        let fields = getRecordFields recordConverter
        StructField(fieldName, fields) :> Field
    | ValueConverter.Optional optionalConverter ->
        // TODO: Is there a better way to deal with this nesting?
        let valueField = getValueSchema fieldName optionalConverter.ValueConverter
        if valueField.IsNullable
        then valueField
        else
            match valueField with
            | :? DataField as dataField ->
                DataField(dataField.Name, dataField.ClrType, isNullable = true)
            | _ -> failwith "not implemented"

let private getRecordFields (recordConverter: RecordConverter) =
    recordConverter.Fields
    |> Array.map (fun field -> getValueSchema field.Name field.ValueConverter)

let ofRecordConverter recordConverter =
    ParquetSchema(getRecordFields recordConverter)
