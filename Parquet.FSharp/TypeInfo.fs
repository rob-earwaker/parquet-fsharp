namespace rec Parquet.FSharp

open FSharp.Reflection
open System
open System.Reflection

type DotnetTypeInfo = {
    SourceType: Type
    IsOptional: bool
    PrimitiveType: Type
    ConvertValueToPrimitive: objnull -> obj
    Schema: Schema.Value }

type FieldInfo = {
    Name: string
    DotnetType: DotnetTypeInfo
    GetValue: obj -> objnull
    Schema: Schema.Field }

type RecordInfo = {
    Fields: FieldInfo[]
    Schema: Schema.RecordType }

//module private NestedTypeInfo =
//    let create valueType isRequired =
//        { NestedTypeInfo.Type = valueType
//          NestedTypeInfo.IsRequired = isRequired }

//    let private isOptionType (dotnetType: Type) =
//        dotnetType.IsGenericType
//        && dotnetType.GetGenericTypeDefinition() = typedefof<Option<_>>

//    let ofType dotnetType =
//        let isOptionType' = isOptionType dotnetType
//        let dotnetType =
//            if isOptionType'
//            then dotnetType.GetGenericArguments()[0]
//            else dotnetType
//        if isOptionType dotnetType then
//            failwith "multi-level option types are not supported"
//        let valueType = ValueType.ofType dotnetType
//        let isRequired = not isOptionType'
//        create valueType isRequired

module DotnetType =
    let (|Bool|_|) dotnetType =
        if dotnetType = typeof<bool>
        then Option.Some ()
        else Option.None

    let (|Int32|_|) dotnetType =
        if dotnetType = typeof<int>
        then Option.Some ()
        else Option.None

    let (|Nullable|_|) (dotnetType: Type) =
        if dotnetType.IsGenericType
            && dotnetType.GetGenericTypeDefinition() = typedefof<Nullable<_>>
        then Option.Some ()
        else Option.None

    let (|NullableInt32|_|) dotnetType =
        if dotnetType = typeof<Nullable<int>>
        then Option.Some ()
        else Option.None

module DotnetTypeInfo =
    let create sourceType isOptional primitiveType convertValueToPrimitive schema =
        { DotnetTypeInfo.SourceType = sourceType
          DotnetTypeInfo.IsOptional = isOptional
          DotnetTypeInfo.PrimitiveType = primitiveType
          DotnetTypeInfo.ConvertValueToPrimitive = convertValueToPrimitive
          DotnetTypeInfo.Schema = schema }

    let private ofPrimitive sourceType valueType =
        let isOptional = false
        let primitiveType = sourceType
        let convertValueToPrimitive = id
        let schema = Schema.Value.create valueType isOptional
        create sourceType isOptional primitiveType convertValueToPrimitive schema

    let private ofNullable (sourceType: Type) =
        let valueDotnetType = Nullable.GetUnderlyingType(sourceType)
        let valueTypeInfo = DotnetTypeInfo.ofType valueDotnetType
        let isOptional = true
        let primitiveType = valueTypeInfo.PrimitiveType
        let getValueMethod = sourceType.GetProperty("Value").GetGetMethod()
        let convertValueToPrimitive (nullableValue: objnull) =
            if isNull nullableValue
            then null
            else
                let value = getValueMethod.Invoke(nullableValue, [||])
                valueTypeInfo.ConvertValueToPrimitive value
        let schema = Schema.Value.create valueTypeInfo.Schema.Type isOptional
        create sourceType isOptional primitiveType convertValueToPrimitive schema

    let ofType (sourceType: Type) =
        match sourceType with
        | DotnetType.Bool -> ofPrimitive sourceType Schema.ValueType.Bool
        | DotnetType.Int32 -> ofPrimitive sourceType Schema.ValueType.Int32
        | DotnetType.Nullable -> ofNullable sourceType
        | _ -> failwith $"unsupported type '{sourceType.FullName}'"

module FieldInfo =
    let create name dotnetTypeInfo getValue schema =
        { FieldInfo.Name = name
          FieldInfo.DotnetType = dotnetTypeInfo
          FieldInfo.GetValue = getValue
          FieldInfo.Schema = schema }

    let ofField (field: PropertyInfo) =
        let name = field.Name
        let dotnetTypeInfo = DotnetTypeInfo.ofType field.PropertyType
        let getValue = FSharpValue.PreComputeRecordFieldReader(field)
        let schema = Schema.Field.create name dotnetTypeInfo.Schema
        create name dotnetTypeInfo getValue schema

module RecordInfo =
    let create fields schema =
        { RecordInfo.Fields = fields
          RecordInfo.Schema = schema }

    let ofRecordType dotnetType =
        if not (FSharpType.IsRecord(dotnetType))
        then failwith "type is not an F# record type"
        else
            let fields =
                FSharpType.GetRecordFields(dotnetType)
                |> Array.map FieldInfo.ofField
            let schema =
                fields
                |> Array.map _.Schema
                |> Schema.RecordType.create
            RecordInfo.create fields schema

    let ofRecord<'Record> =
        ofRecordType typeof<'Record>
