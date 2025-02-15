namespace rec Parquet.FSharp

open FSharp.Reflection
open System
open System.Reflection

type ValueType =
    | Bool
    | Int
    | Float
    | String
    | Record of RecordType

and RecordType = {
    FieldTypes: FieldType[] }

and FieldType = {
    Multiplicity: Multiplicity
    ValueType: ValueType
    Name: string
    Level: Level }

and Multiplicity =
    | Required
    | Optional
    | Repeated

and Level = {
    Definition: int
    MaxRepetition: int }

module private Level =
    let Root = {
        Level.Definition = 0
        Level.MaxRepetition = 0 }

    let incrementDefinition (level: Level) =
        { level with Definition = level.Definition + 1 }

    let incrementMaxRepetition (level: Level) =
        { level with MaxRepetition = level.MaxRepetition + 1 }

module private FieldType =
    let create multiplicity valueType name level =
        { FieldType.Multiplicity = multiplicity
          FieldType.ValueType = valueType
          FieldType.Name = name
          FieldType.Level = level }

    let required valueType name level =
        create Multiplicity.Required valueType name level

    let optional valueType name level =
        create Multiplicity.Optional valueType name level

    let repeated valueType name level =
        create Multiplicity.Repeated valueType name level

    let ofFieldInfo (parentLevel: Level) (fieldInfo: PropertyInfo) =
        match fieldInfo.PropertyType with
        | dotnetType when dotnetType = typeof<bool> ->
            required ValueType.Bool fieldInfo.Name parentLevel
        | dotnetType when dotnetType = typeof<int> ->
            required ValueType.Int fieldInfo.Name parentLevel
        | dotnetType when dotnetType = typeof<float> ->
            required ValueType.Float fieldInfo.Name parentLevel
        | dotnetType when dotnetType = typeof<string> ->
            let level = Level.incrementDefinition parentLevel
            required ValueType.String fieldInfo.Name level
        | dotnetType when FSharpType.IsRecord(dotnetType) ->
            let recordType = RecordType.ofType dotnetType parentLevel
            let valueType = ValueType.Record recordType
            required valueType fieldInfo.Name parentLevel
        | dotnetType ->
            failwith $"type '{dotnetType.FullName}' is not supported"

module RecordType =
    let ofType (dotnetType: Type) level =
        if not (FSharpType.IsRecord(dotnetType)) then
            failwith $"type '{dotnetType.FullName}' is not an fsharp record type"
        let fieldTypes =
            FSharpType.GetRecordFields(dotnetType)
            |> Array.map (FieldType.ofFieldInfo level)
        { RecordType.FieldTypes = fieldTypes }

    let of'<'Record> =
        ofType typeof<'Record> Level.Root
