namespace rec Parquet.FSharp

open Parquet.Schema
open System

type internal Schema = {
    Fields: FieldSchema[] }

type internal ValueSchema =
    | Atomic of AtomicSchema
    | List of ListSchema
    | Record of RecordSchema
    with
    member this.IsOptional =
        match this with
        | ValueSchema.Atomic atomic -> atomic.IsOptional
        | ValueSchema.List list -> list.IsOptional
        | ValueSchema.Record record -> record.IsOptional

type internal AtomicSchema = {
    IsOptional: bool
    DotnetType: Type }

type internal ListSchema = {
    IsOptional: bool
    Element: ValueSchema }

type internal FieldSchema = {
    Name: string
    Value: ValueSchema }

type internal RecordSchema = {
    IsOptional: bool
    Fields: FieldSchema[] }

module internal ValueSchema =
    let private ofConverter' isOptional valueConverter =
        match valueConverter with
        | ValueConverter.Atomic atomicConverter ->
            ValueSchema.Atomic {
                IsOptional = isOptional
                DotnetType = atomicConverter.DataDotnetType }
        | ValueConverter.List listConverter ->
            ValueSchema.List {
                IsOptional = isOptional
                Element = ValueSchema.ofConverter listConverter.ElementConverter }
        | ValueConverter.Record recordConverter ->
            ValueSchema.Record {
                IsOptional = isOptional
                Fields = recordConverter.Fields |> Array.map FieldSchema.ofConverter }
        | ValueConverter.Optional optionalConverter ->
            ValueSchema.ofConverter' true optionalConverter.ValueConverter

    let ofConverter valueConverter =
        ValueSchema.ofConverter' false valueConverter

    let ofParquetNet (field: Field) =
        match field with
        | :? DataField as dataField ->
            ValueSchema.Atomic {
                IsOptional = dataField.IsNullable
                DotnetType = dataField.ClrType }
        | :? ListField as listField ->
            ValueSchema.List {
                IsOptional = listField.IsNullable
                Element = ValueSchema.ofParquetNet listField.Item }
        | :? StructField as structField ->
            ValueSchema.Record {
                IsOptional = structField.IsNullable
                Fields =
                    structField.Fields
                    |> Seq.map FieldSchema.ofParquetNet
                    |> Array.ofSeq }
        | _ -> failwith $"unsupported field type '{field.GetType().FullName}'"

    let toParquetNet fieldName valueSchema =
        match valueSchema with
        | ValueSchema.Atomic atomicSchema ->
            // TODO: We should eventually need to use some of the custom
            // DataField types here, e.g. DecimalDataField
            DataField(fieldName, atomicSchema.DotnetType, atomicSchema.IsOptional)
            :> Field
        | ValueSchema.List listSchema ->
            let element = toParquetNet ListField.ElementName listSchema.Element
            ListField(fieldName, element) :> Field
        | ValueSchema.Record recordSchema ->
            let fields = recordSchema.Fields |> Array.map FieldSchema.toParquetNet
            StructField(fieldName, fields) :> Field

module internal FieldSchema =
    let ofConverter (fieldConverter: FieldConverter) =
        { FieldSchema.Name = fieldConverter.Name
          FieldSchema.Value = ValueSchema.ofConverter fieldConverter.ValueConverter }

    let ofParquetNet (field: Field) =
        { FieldSchema.Name = field.Name
          FieldSchema.Value = ValueSchema.ofParquetNet field }

    let toParquetNet (fieldSchema: FieldSchema) =
        ValueSchema.toParquetNet fieldSchema.Name fieldSchema.Value

module internal Schema =
    let ofConverter (recordConverter: RecordConverter) =
        { Schema.Fields = recordConverter.Fields |> Array.map FieldSchema.ofConverter }

    let ofParquetNet (schema: ParquetSchema) =
        { Schema.Fields =
            schema.Fields
            |> Seq.map FieldSchema.ofParquetNet
            |> Array.ofSeq }

    let toParquetNet (schema: Schema) =
        let fields = schema.Fields |> Array.map FieldSchema.toParquetNet
        ParquetSchema(fields)
