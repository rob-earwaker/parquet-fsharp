namespace rec Parquet.FSharp.Schema

open Parquet.FSharp

type Schema = {
    Fields: Field[] }

type Field = {
    Name: string
    Value: Value }

type Value = {
    Repetition: Repetition
    Type: ValueType }

type Repetition =
    | Required
    | Optional
    | Repeated

type ValueType =
    | PrimitiveType of PrimitiveType
    | LogicalType of LogicalType
    | RecordType of RecordType
    | ListType of ListType

type PrimitiveType =
    | Bool
    | Int32
    | ByteArray

type LogicalType =
    | Int32
    | String

type RecordType = {
    Fields: Field[] }

type ListType = {
    Element: Value }

module LogicalType =
    let toThrift logicalType =
        match logicalType with
        | LogicalType.Int32 -> Thrift.LogicalType.INT32
        | LogicalType.String -> Thrift.LogicalType.STRING

module PrimitiveType =
    let toThrift primitiveType =
        match primitiveType with
        | PrimitiveType.Bool -> Thrift.Type.BOOLEAN
        | PrimitiveType.Int32 -> Thrift.Type.INT32
        | PrimitiveType.ByteArray -> Thrift.Type.BYTE_ARRAY

module ValueType =
    module Primitive =
        let Bool = ValueType.PrimitiveType PrimitiveType.Bool
        let Int32 = ValueType.PrimitiveType PrimitiveType.Int32
        let ByteArray = ValueType.PrimitiveType PrimitiveType.ByteArray

    module Logical =
        let Int32 = ValueType.LogicalType LogicalType.Int32
        let String = ValueType.LogicalType LogicalType.String

    let record fields =
        ValueType.RecordType { Fields = fields }

    let list element =
        ValueType.ListType { Element = element }

module Value =
    let create repetition valueType =
        { Value.Repetition = repetition
          Value.Type = valueType }

    let required valueType =
        create Repetition.Required valueType

    let optional valueType =
        create Repetition.Optional valueType

    let repeated valueType =
        create Repetition.Repeated valueType

    let toThrift name (value: Value) =
        seq {
            let repetitionType = Repetition.toThrift value.Repetition
            match value.Type with
            | ValueType.PrimitiveType primitiveType ->
                let primitiveType = PrimitiveType.toThrift primitiveType
                yield Thrift.SchemaElement.primitiveValue repetitionType name primitiveType
            | ValueType.LogicalType logicalType ->
                let logicalType = LogicalType.toThrift logicalType
                yield Thrift.SchemaElement.logicalValue repetitionType name logicalType
            | ValueType.RecordType recordType ->
                yield Thrift.SchemaElement.record repetitionType name recordType.Fields.Length
                for field in recordType.Fields do
                    yield! Field.toThrift field
            | ValueType.ListType listType ->
                yield Thrift.SchemaElement.listOuter repetitionType name
                yield Thrift.SchemaElement.listMiddle ()
                yield! Value.toThrift "element" listType.Element
        }

module Repetition =
    let toThrift repetition =
        match repetition with
        | Repetition.Required -> Thrift.FieldRepetitionType.REQUIRED
        | Repetition.Optional -> Thrift.FieldRepetitionType.OPTIONAL
        | Repetition.Repeated -> Thrift.FieldRepetitionType.REPEATED

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
