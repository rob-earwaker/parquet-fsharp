namespace rec Parquet.FSharp

open FSharp.Reflection
open System

type ValueType =
    | Bool
    | Int32
    | Float64
    | DateTimeOffset
    | String
    | Array of ArrayTypeInfo
    | Record of RecordTypeInfo

type NestedTypeInfo = {
    Type: ValueType
    IsRequired: bool }

type ArrayTypeInfo = {
    Element: NestedTypeInfo }

type FieldTypeInfo = {
    Name: string
    Value: NestedTypeInfo
    GetValue: obj -> objnull }

type RecordTypeInfo = {
    Fields: FieldTypeInfo[] }

module private ValueType =
    let ofType dotnetType =
        match dotnetType with
        | dotnetType when dotnetType = typeof<bool> -> ValueType.Bool
        | dotnetType when dotnetType = typeof<int> -> ValueType.Int32
        | dotnetType when dotnetType = typeof<float> -> ValueType.Float64
        | dotnetType when dotnetType = typeof<DateTimeOffset> -> ValueType.DateTimeOffset
        | dotnetType when dotnetType = typeof<string> -> ValueType.String
        | dotnetType when dotnetType.IsArray ->
            if dotnetType.GetArrayRank() <> 1 then
                failwith "multi-dimensional arrays are not supported"
            let elementDotnetType = dotnetType.GetElementType()
            let elementTypeInfo = NestedTypeInfo.ofType elementDotnetType
            ValueType.Array { Element = elementTypeInfo }
        | dotnetType when FSharpType.IsRecord(dotnetType) ->
            let fieldTypeInfo =
                FSharpType.GetRecordFields(dotnetType)
                |> Array.map (fun field ->
                    let valueTypeInfo = NestedTypeInfo.ofType field.PropertyType
                    let getValue = FSharpValue.PreComputeRecordFieldReader(field)
                    FieldTypeInfo.create field.Name valueTypeInfo getValue)
            ValueType.Record { Fields = fieldTypeInfo }
        | dotnetType ->
            failwith $"type '{dotnetType.FullName}' is not supported"

module private NestedTypeInfo =
    let create valueType isRequired =
        { NestedTypeInfo.Type = valueType
          NestedTypeInfo.IsRequired = isRequired }

    let private isOptionType (dotnetType: Type) =
        dotnetType.IsGenericType
        && dotnetType.GetGenericTypeDefinition() = typedefof<Option<_>>

    let ofType dotnetType =
        let isOptionType' = isOptionType dotnetType
        let dotnetType =
            if isOptionType'
            then dotnetType.GetGenericArguments()[0]
            else dotnetType
        if isOptionType dotnetType then
            failwith "multi-level option types are not supported"
        let valueType = ValueType.ofType dotnetType
        let isRequired = not isOptionType'
        create valueType isRequired

    let rec toThriftSchema name (nestedTypeInfo: NestedTypeInfo) =
        seq {
            let repetitionType =
                if nestedTypeInfo.IsRequired
                then FieldRepetitionType.REQUIRED
                else FieldRepetitionType.OPTIONAL
            match nestedTypeInfo.Type with
            | ValueType.Bool ->
                yield SchemaElement.bool repetitionType name
            | ValueType.Int32 ->
                let intType = IntType(BitWidth = 32y, IsSigned = true)
                let logicalType = LogicalType(INTEGER = intType)
                yield SchemaElement.logical repetitionType name logicalType
            | ValueType.Float64 ->
                yield SchemaElement.double repetitionType name
            | ValueType.DateTimeOffset ->
                let timestampType =
                    TimestampType(
                        IsAdjustedToUTC = true,
                        Unit = TimeUnit(MICROS = MicroSeconds()))
                let logicalType = LogicalType(TIMESTAMP = timestampType)
                yield SchemaElement.logical repetitionType name logicalType
            | ValueType.String ->
                let logicalType = LogicalType(STRING = StringType())
                yield SchemaElement.logical repetitionType name logicalType
            | ValueType.Array arrayTypeInfo ->
                yield SchemaElement.listOuter repetitionType name
                yield SchemaElement.listMiddle ()
                yield! toThriftSchema "element" arrayTypeInfo.Element
            | ValueType.Record recordType ->
                yield SchemaElement.recordGroup repetitionType name recordType.Fields.Length
                for fieldTypeInfo in recordType.Fields do
                    yield! FieldTypeInfo.toThriftSchema fieldTypeInfo
        }

module FieldTypeInfo =
    let create name valueType getValue =
        { FieldTypeInfo.Name = name
          FieldTypeInfo.Value = valueType
          FieldTypeInfo.GetValue = getValue }

    let toThriftSchema (fieldTypeInfo: FieldTypeInfo) =
        NestedTypeInfo.toThriftSchema fieldTypeInfo.Name fieldTypeInfo.Value

module RecordTypeInfo =
    let private create fieldTypeInfo =
        { RecordTypeInfo.Fields = fieldTypeInfo }

    let ofRecordType dotnetType =
        match ValueType.ofType dotnetType with
        | ValueType.Record recordType -> create recordType.Fields
        | _ -> failwith "schema root type must be a record type"

    let ofRecord<'Record> =
        ofRecordType typeof<'Record>

module Schema =
    let toThriftSchema (recordTypeInfo: RecordTypeInfo) =
        let numChildren = recordTypeInfo.Fields.Length
        let rootElement = SchemaElement.root numChildren
        let schemaElements = ResizeArray([| rootElement |])
        for fieldTypeInfo in recordTypeInfo.Fields do
            schemaElements.AddRange(FieldTypeInfo.toThriftSchema fieldTypeInfo)
        schemaElements
