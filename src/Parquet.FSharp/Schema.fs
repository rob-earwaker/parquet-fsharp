namespace rec Parquet.FSharp

open Parquet.Schema
open System

type internal RootSchema = {
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
    member this.MakeOptional() =
        { this with IsOptional = true }
        
    member this.MakeRequired() =
        { this with IsOptional = false }

    override this.ToString() =
        let optionality = if this.IsOptional then "optional" else "required"
        $"{optionality} {string this.Type}"

type internal ValueTypeSchema =
    | Primitive of PrimitiveTypeSchema
    | List of ListTypeSchema
    | Record of RecordTypeSchema

    override this.ToString() =
        match this with
        | ValueTypeSchema.Primitive primitive -> string primitive
        | ValueTypeSchema.List list -> string list
        | ValueTypeSchema.Record record -> string record

type internal PrimitiveTypeSchema = {
    DataDotnetType: Type }
    with
    override this.ToString() =
        // TODO: Could enumerate all primitive types here to make it nicer.
        this.DataDotnetType.Name

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

module internal ValueTypeSchema =
    let primitive dataDotnetType =
        ValueTypeSchema.Primitive { DataDotnetType = dataDotnetType }

    let list element =
        ValueTypeSchema.List { Element = element }

    let record fields =
        ValueTypeSchema.Record { Fields = fields }

module internal ValueSchema =
    let create isOptional valueType =
        { ValueSchema.IsOptional = isOptional
          ValueSchema.Type = valueType }

    let ofParquetNet (field: Field) =
        let isOptional = field.IsNullable
        let valueType =
            match field with
            | :? DataField as dataField ->
                let dataDotnetType = dataField.ClrType
                ValueTypeSchema.primitive dataDotnetType
            | :? ListField as listField ->
                let element = ValueSchema.ofParquetNet listField.Item
                ValueTypeSchema.list element
            | :? StructField as structField ->
                structField.Fields
                |> Seq.map FieldSchema.ofParquetNet
                |> Array.ofSeq
                |> ValueTypeSchema.record
            | _ -> failwith $"unsupported field type '{field.GetType().FullName}'"
        ValueSchema.create isOptional valueType

    let toParquetNet fieldName (valueSchema: ValueSchema) =
        match valueSchema.Type with
        | ValueTypeSchema.Primitive primitiveSchema ->
            DataField(fieldName, primitiveSchema.DataDotnetType, valueSchema.IsOptional)
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
    let create name value =
        { FieldSchema.Name = name
          FieldSchema.Value = value }

    let ofParquetNet (field: Field) =
        let value = ValueSchema.ofParquetNet field
        FieldSchema.create field.Name value

    let toParquetNet (fieldSchema: FieldSchema) =
        ValueSchema.toParquetNet fieldSchema.Name fieldSchema.Value

module internal RootSchema =
    let create fields =
        { RootSchema.Fields = fields }

    let ofValueSchema (valueSchema: ValueSchema) =
        if valueSchema.IsOptional
        then failwith "root record must not be optional"
        else
            match valueSchema.Type with
            | ValueTypeSchema.Record recordSchema ->
                RootSchema.create recordSchema.Fields
            | _ -> failwith "root value must be a record"

    let ofParquetNet (schema: ParquetSchema) =
        schema.Fields
        |> Seq.map FieldSchema.ofParquetNet
        |> Array.ofSeq
        |> RootSchema.create

    let toParquetNet (schema: RootSchema) =
        let fields = schema.Fields |> Array.map FieldSchema.toParquetNet
        ParquetSchema(fields)
