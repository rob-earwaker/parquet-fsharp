namespace rec Parquet.FSharp

open Parquet.Schema
open System

type internal Schema = {
    Fields: FieldSchema[] }

type internal FieldSchema = {
    Name: string
    Value: ValueSchema }
    with
    override this.ToString() =
        $"{this.Name}: {this.Value}"

type internal ValueSchema = {
    IsOptional: bool
    Type: ValueTypeSchema }
    with
    member this.MakeRequired() =
        { this with IsOptional = false }

    override this.ToString() =
        let optionality = if this.IsOptional then "optional" else "required"
        $"{optionality} {string this.Type}"

type internal ValueTypeSchema =
    | Atomic of AtomicTypeSchema
    | List of ListTypeSchema
    | Record of RecordTypeSchema

    override this.ToString() =
        match this with
        | ValueTypeSchema.Atomic atomic -> string atomic
        | ValueTypeSchema.List list -> string list
        | ValueTypeSchema.Record record -> string record

type internal AtomicTypeSchema = {
    DotnetType: Type }
    with
    override this.ToString() =
        // TODO: Could enumerate all primitive types here to make it nicer.
        this.DotnetType.Name

type internal ListTypeSchema = {
    Element: ValueSchema }
    with
    override this.ToString() =
        $"[ {this.Element} ]"

type internal RecordTypeSchema = {
    Fields: FieldSchema[] }
    with
    override this.ToString() =
        let fields = this.Fields |> Array.map string |> String.concat ", "
        $"{{ {fields} }}"

module internal ValueSchema =
    let ofParquetNet (field: Field) =
        let isOptional = field.IsNullable
        let valueType =
            match field with
            | :? DataField as dataField ->
                ValueTypeSchema.Atomic { DotnetType = dataField.ClrType }
            | :? ListField as listField ->
                ValueTypeSchema.List {
                    Element = ValueSchema.ofParquetNet listField.Item }
            | :? StructField as structField ->
                ValueTypeSchema.Record {
                    Fields =
                        structField.Fields
                        |> Seq.map FieldSchema.ofParquetNet
                        |> Array.ofSeq }
            | _ -> failwith $"unsupported field type '{field.GetType().FullName}'"
        { ValueSchema.IsOptional = isOptional
          ValueSchema.Type = valueType }

    let toParquetNet fieldName (valueSchema: ValueSchema) =
        match valueSchema.Type with
        | ValueTypeSchema.Atomic atomicSchema ->
            // TODO: We should eventually need to use some of the custom
            // DataField types here, e.g. DecimalDataField
            DataField(fieldName, atomicSchema.DotnetType, valueSchema.IsOptional)
            :> Field
        | ValueTypeSchema.List listSchema ->
            // Lists are always optional in Parquet.Net.
            let element = toParquetNet ListField.ElementName listSchema.Element
            ListField(fieldName, element) :> Field
        | ValueTypeSchema.Record recordSchema ->
            // Records are always optional in Parquet.Net.
            let fields = recordSchema.Fields |> Array.map FieldSchema.toParquetNet
            StructField(fieldName, fields) :> Field

module internal FieldSchema =
    let ofParquetNet (field: Field) =
        { FieldSchema.Name = field.Name
          FieldSchema.Value = ValueSchema.ofParquetNet field }

    let toParquetNet (fieldSchema: FieldSchema) =
        ValueSchema.toParquetNet fieldSchema.Name fieldSchema.Value

module internal Schema =
    let ofParquetNet (schema: ParquetSchema) =
        { Schema.Fields =
            schema.Fields
            |> Seq.map FieldSchema.ofParquetNet
            |> Array.ofSeq }

    let toParquetNet (schema: Schema) =
        let fields = schema.Fields |> Array.map FieldSchema.toParquetNet
        ParquetSchema(fields)
