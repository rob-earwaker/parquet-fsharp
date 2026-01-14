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

    member this.MakeRequired() =
        match this with
        | ValueSchema.Atomic atomic ->
            ValueSchema.Atomic { atomic with IsOptional = false }
        | ValueSchema.List list ->
            ValueSchema.List { list with IsOptional = false }
        | ValueSchema.Record record ->
            ValueSchema.Record { record with IsOptional = false }

    override this.ToString() =
        match this with
        | ValueSchema.Atomic atomic -> atomic.ToString()
        | ValueSchema.List list -> list.ToString()
        | ValueSchema.Record record -> record.ToString()

type internal AtomicSchema = {
    IsOptional: bool
    // TODO: All of the atomic deserializers should check against this type.
    DotnetType: Type }
    with
    override this.ToString() =
        let optionality = if this.IsOptional then "optional" else "required"
        // TODO: Could enumerate all primitive types here to make it nicer.
        $"{optionality} {this.DotnetType.Name}"

type internal ListSchema = {
    IsOptional: bool
    Element: ValueSchema }
    with
    override this.ToString() =
        let optionality = if this.IsOptional then "optional" else "required"
        $"{optionality} [ {this.Element} ]"

type internal FieldSchema = {
    Name: string
    Value: ValueSchema }
    with
    override this.ToString() =
        $"{this.Name}: {this.Value}"

type internal RecordSchema = {
    IsOptional: bool
    Fields: FieldSchema[] }
    with
    override this.ToString() =
        let optionality = if this.IsOptional then "optional" else "required"
        let fields = this.Fields |> Array.map string |> String.concat ", "
        $"{optionality} {{ {fields} }}"

module internal ValueSchema =
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
