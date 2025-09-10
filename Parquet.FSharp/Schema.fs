namespace rec Parquet.FSharp

open FSharp.Reflection
open System

type Schema = {
    Fields: FieldType[] }

type FieldType = {
    Name: string
    Value: NestedType }

type NestedType = {
    Type: ValueType
    IsRequired: bool }

type ValueType =
    | Bool
    | Int32
    | Float64
    | DateTimeOffset
    | String
    | Array of ArrayType
    | Record of RecordType

type ArrayType = {
    Element: NestedType }

type RecordType = {
    Fields: FieldType[] }

module private NestedType =
    let create valueType isRequired =
        { NestedType.Type = valueType
          NestedType.IsRequired = isRequired }

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
            let elementType = NestedType.ofType elementDotnetType
            ValueType.Array { Element = elementType }
        | dotnetType when FSharpType.IsRecord(dotnetType) ->
            let fieldTypes =
                FSharpType.GetRecordFields(dotnetType)
                |> Array.map (fun fieldInfo ->
                    let valueType = NestedType.ofType fieldInfo.PropertyType
                    FieldType.create fieldInfo.Name valueType)
            ValueType.Record { Fields = fieldTypes }
        | dotnetType ->
            failwith $"type '{dotnetType.FullName}' is not supported"

module FieldType =
    let create name valueType =
        { FieldType.Name = name
          FieldType.Value = valueType }

    let rec toThriftSchema (field: FieldType) =
        seq {
            let repetitionType =
                if field.Value.IsRequired
                then FieldRepetitionType.REQUIRED
                else FieldRepetitionType.OPTIONAL
            let name = field.Name
            match field.Value.Type with
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
            | ValueType.Array arrayType ->
                yield SchemaElement.listOuter repetitionType name
                yield SchemaElement.listMiddle ()
                let elementField = FieldType.create "element" arrayType.Element
                yield! toThriftSchema elementField
            | ValueType.Record recordType ->
                yield SchemaElement.recordGroup repetitionType name recordType.Fields.Length
                for field in recordType.Fields do
                    yield! toThriftSchema field
        }

module Schema =
    let private create fieldTypes =
        { Schema.Fields = fieldTypes }

    let ofRecordType dotnetType =
        match ValueType.ofType dotnetType with
        | ValueType.Record recordType -> create recordType.Fields
        | _ -> failwith "schema root type must be a record type"

    let ofRecord<'Record> =
        ofRecordType typeof<'Record>

    let toThriftSchema (schema: Schema) =
        let numChildren = schema.Fields.Length
        let rootElement = SchemaElement.root numChildren
        let schemaElements = ResizeArray([| rootElement |])
        for field in schema.Fields do
            schemaElements.AddRange(FieldType.toThriftSchema field)
        schemaElements
