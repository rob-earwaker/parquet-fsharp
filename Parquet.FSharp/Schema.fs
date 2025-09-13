namespace rec Parquet.FSharp.Schema

open Parquet.FSharp

type Schema = {
    Fields: Field[] }

type Field = {
    Name: string
    Value: Value }

type Value = {
    Type: ValueType
    IsRequired: bool }

type ValueType =
    | Bool
    | Int32
    | ByteArray
    | String
    | Record of RecordType
    | List of ListType

type RecordType = {
    Fields: Field[] }

type ListType = {
    Element: Value }

module ListType =
    let create element =
        { ListType.Element = element }

module RecordType =
    let create fields =
        { RecordType.Fields = fields }

module ValueType =
    let record fields =
        ValueType.Record (RecordType.create fields)

    let list element =
        ValueType.List (ListType.create element)

module Value =
    let create valueType isRequired =
        { Value.Type = valueType
          Value.IsRequired = isRequired }

    let required valueType =
        create valueType true

    let optional valueType =
        create valueType false

    let toThrift name (value: Value) =
        seq {
            let repetitionType =
                if value.IsRequired
                then Thrift.FieldRepetitionType.REQUIRED
                else Thrift.FieldRepetitionType.OPTIONAL
            match value.Type with
            | ValueType.Bool ->
                let type' = Thrift.Type.BOOLEAN
                yield Thrift.SchemaElement.primitive repetitionType name type'
            | ValueType.Int32 ->
                let logicalType = Thrift.LogicalType.INT32
                yield Thrift.SchemaElement.logical repetitionType name logicalType
            | ValueType.ByteArray ->
                let type' = Thrift.Type.BYTE_ARRAY
                yield Thrift.SchemaElement.primitive repetitionType name type'
            | ValueType.String ->
                let logicalType = Thrift.LogicalType.STRING
                yield Thrift.SchemaElement.logical repetitionType name logicalType
            | ValueType.Record recordType ->
                let numFields = recordType.Fields.Length
                yield Thrift.SchemaElement.record repetitionType name numFields
                for field in recordType.Fields do
                    yield! Field.toThrift field
            | ValueType.List listType ->
                yield Thrift.SchemaElement.listOuter repetitionType name
                yield Thrift.SchemaElement.listMiddle ()
                yield! Value.toThrift "element" listType.Element
        }

module Field =
    let create name value =
        { Field.Name = name
          Field.Value = value }

    let toThrift (field: Field) =
        Value.toThrift field.Name field.Value

module Schema =
    let create fields =
        { Schema.Fields = fields }

    let toThrift (schema: Schema) =
        let numChildren = schema.Fields.Length
        let rootElement = Thrift.SchemaElement.root numChildren
        let schemaElements = ResizeArray([| rootElement |])
        for field in schema.Fields do
            schemaElements.AddRange(Field.toThrift field)
        schemaElements
