namespace rec Parquet.FSharp

open FSharp.Reflection
open System

type Schema = {
    Fields: FieldType[] }

type ValueType =
    | Bool
    | Int32
    | Float64
    | DateTimeOffset
    | String
    | Array of ArrayType
    | Record of RecordType

and ArrayType = {
    Element: NestedType }

and RecordType = {
    Fields: FieldType[] }

and FieldType = {
    Name: string
    Value: NestedType }

and NestedType = {
    Type: ValueType
    IsRequired: bool }

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

module private FieldType =
    let create name valueType =
        { FieldType.Name = name
          FieldType.Value = valueType }

module Schema =
    let private create fieldTypes =
        { Schema.Fields = fieldTypes }

    let ofRecordType dotnetType =
        match ValueType.ofType dotnetType with
        | ValueType.Record recordType -> create recordType.Fields
        | _ -> failwith "schema root type must be a record type"

    let ofRecord<'Record> =
        ofRecordType typeof<'Record>
