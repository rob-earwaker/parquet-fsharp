namespace rec Parquet.FSharp

open Parquet.FSharp

type Schema = {
    Fields: Field[] }

type Field = {
    Name: string
    Value: Value }

type Value = {
    Type: ValueType
    IsOptional: bool }

type ValueType =
    | Bool
    | Int32
    | Float64
    | ByteArray
    | Timestamp of TimestampType
    | String
    | Record of Record
    | List of List

type Record = {
    Fields: Field[] }

type List = {
    Element: Value }

module List =
    let create element =
        { List.Element = element }

    let ofListInfo (listInfo: ListInfo) =
        let element = Value.ofValueInfo listInfo.ElementInfo
        create element

module Record =
    let create fields =
        { Record.Fields = fields }

    let ofRecordInfo (recordInfo: RecordInfo) =
        recordInfo.Fields
        |> Array.map Field.ofFieldInfo
        |> create

module ValueType =
    let record fields =
        ValueType.Record (Record.create fields)

    let list element =
        ValueType.List (List.create element)

    let ofAtomicInfo (atomicInfo: AtomicInfo) =
        match atomicInfo.LogicalType with
        | LogicalType.Bool -> ValueType.Bool
        | LogicalType.Int32 -> ValueType.Int32
        | LogicalType.Float64 -> ValueType.Float64
        | LogicalType.Timestamp timestamp -> ValueType.Timestamp timestamp
        | LogicalType.String -> ValueType.String

    let ofListInfo listInfo =
        let list = List.ofListInfo listInfo
        ValueType.List list

    let ofRecordInfo recordInfo =
        let record = Record.ofRecordInfo recordInfo
        ValueType.Record record

module Value =
    let create valueType isOptional =
        { Value.Type = valueType
          Value.IsOptional = isOptional }

    let ofValueInfo (valueInfo: ValueInfo) =
        match valueInfo with
        | ValueInfo.Atomic atomicInfo ->
            let valueType = ValueType.ofAtomicInfo atomicInfo
            create valueType atomicInfo.IsOptional
        | ValueInfo.List listInfo ->
            let valueType = ValueType.ofListInfo listInfo
            create valueType listInfo.IsOptional
        | ValueInfo.Record recordInfo ->
            let valueType = ValueType.ofRecordInfo recordInfo
            create valueType recordInfo.IsOptional

    let toThrift name (value: Value) =
        seq {
            let repetitionType =
                if value.IsOptional
                then Thrift.FieldRepetitionType.OPTIONAL
                else Thrift.FieldRepetitionType.REQUIRED
            match value.Type with
            | ValueType.Bool ->
                let type' = Thrift.Type.BOOLEAN
                yield Thrift.SchemaElement.primitive type' repetitionType name
            | ValueType.Int32 ->
                let logicalType = Thrift.LogicalType.INT32
                yield Thrift.SchemaElement.logical repetitionType name logicalType
            | ValueType.Float64 ->
                let type' = Thrift.Type.DOUBLE
                yield Thrift.SchemaElement.primitive type' repetitionType name
            | ValueType.ByteArray ->
                let type' = Thrift.Type.BYTE_ARRAY
                yield Thrift.SchemaElement.primitive type' repetitionType name
            | ValueType.Timestamp timestamp ->
                let unit =
                    match timestamp.Unit with
                    | TimeUnit.Milliseconds -> Thrift.TimeUnit.MILLIS
                    | TimeUnit.Microseconds -> Thrift.TimeUnit.MICROS
                    | TimeUnit.Nanoseconds -> Thrift.TimeUnit.NANOS
                let logicalType = Thrift.LogicalType.TIMESTAMP timestamp.IsUtc unit
                yield Thrift.SchemaElement.logical repetitionType name logicalType
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

    let ofFieldInfo (fieldInfo: FieldInfo) =
        let value = Value.ofValueInfo fieldInfo.ValueInfo
        create fieldInfo.Name value

    let toThrift (field: Field) =
        Value.toThrift field.Name field.Value

module Schema =
    let create fields =
        { Schema.Fields = fields }

    let ofRecordInfo recordInfo =
        let record = Record.ofRecordInfo recordInfo
        create record.Fields

    let toThrift (schema: Schema) =
        let numChildren = schema.Fields.Length
        let rootElement = Thrift.SchemaElement.root numChildren
        let schemaElements = ResizeArray([| rootElement |])
        for field in schema.Fields do
            schemaElements.AddRange(Field.toThrift field)
        schemaElements
