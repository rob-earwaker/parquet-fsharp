namespace rec Parquet.FSharp

open FSharp.Reflection
open System
open System.Reflection

type PrimitiveType =
    | Bool
    | Int32

type Primitive = {
    Type: PrimitiveType
    DotnetType: Type
    Schema: Schema.ValueType }

type private DotnetTypeInfo = {
    DotnetType: Type
    IsOptional: bool
    Primitive: Primitive
    ConvertValueToPrimitive: obj -> obj
    Schema: Schema.Value }

type FieldInfo = {
    Name: string
    DotnetType: Type
    IsOptional: bool
    Primitive: Primitive
    GetPrimitiveValue: obj -> obj
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

module Primitive =
    let private create type' dotnetType schema =
        { Primitive.Type = type'
          Primitive.DotnetType = dotnetType
          Primitive.Schema = schema }

    let Bool = create PrimitiveType.Bool typeof<bool> Schema.ValueType.Bool
    let Int32 = create PrimitiveType.Int32 typeof<int> Schema.ValueType.Int32

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

    let (|Option|_|) (dotnetType: Type) =
        if dotnetType.IsGenericType
            && dotnetType.GetGenericTypeDefinition() = typedefof<Option<_>>
        then Option.Some ()
        else Option.None

module private DotnetTypeInfo =
    let create dotnetType isOptional primitive convertValueToPrimitive schema =
        { DotnetTypeInfo.DotnetType = dotnetType
          DotnetTypeInfo.IsOptional = isOptional
          DotnetTypeInfo.Primitive = primitive
          DotnetTypeInfo.ConvertValueToPrimitive = convertValueToPrimitive
          DotnetTypeInfo.Schema = schema }

    let private ofPrimitive (primitive: Primitive) =
        let dotnetType = primitive.DotnetType
        let isOptional = false
        let convertValueToPrimitive = id
        let schema = Schema.Value.create primitive.Schema isOptional
        create dotnetType isOptional primitive convertValueToPrimitive schema

    let private ofNullable (dotnetType: Type) =
        let valueTypeInfo =
            let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
            DotnetTypeInfo.ofType valueDotnetType
        let isOptional = true
        let primitive = valueTypeInfo.Primitive
        let getValueMethod = dotnetType.GetProperty("Value").GetGetMethod()
        let convertValueToPrimitive (nullableValue: obj) =
            if isNull nullableValue
            then null
            else
                let value = getValueMethod.Invoke(nullableValue, [||])
                valueTypeInfo.ConvertValueToPrimitive value
        let schema = Schema.Value.create valueTypeInfo.Schema.Type isOptional
        create dotnetType isOptional primitive convertValueToPrimitive schema

    let private ofOption (dotnetType: Type) =
        let valueTypeInfo =
            let valueDotnetType = dotnetType.GetGenericArguments()[0]
            DotnetTypeInfo.ofType valueDotnetType
        let isOptional = true
        let primitive = valueTypeInfo.Primitive
        let unionCases = FSharpType.GetUnionCases(dotnetType)
        let noneCase = unionCases |> Array.find _.Name.Equals("None")
        let someCase = unionCases |> Array.find _.Name.Equals("Some")
        let getOptionTag = FSharpValue.PreComputeUnionTagReader(dotnetType)
        let getSomeFields = FSharpValue.PreComputeUnionReader(someCase)
        let convertValueToPrimitive (valueOption: obj) =
            if getOptionTag valueOption = noneCase.Tag
            then null
            else
                let value = Array.head (getSomeFields valueOption)
                valueTypeInfo.ConvertValueToPrimitive value
        let schema = Schema.Value.create valueTypeInfo.Schema.Type isOptional
        create dotnetType isOptional primitive convertValueToPrimitive schema

    let ofType (dotnetType: Type) : DotnetTypeInfo =
        match dotnetType with
        | DotnetType.Bool -> ofPrimitive Primitive.Bool
        | DotnetType.Int32 -> ofPrimitive Primitive.Int32
        | DotnetType.Nullable -> ofNullable dotnetType
        | DotnetType.Option -> ofOption dotnetType
        | _ -> failwith $"unsupported type '{dotnetType.FullName}'"

module FieldInfo =
    let create name dotnetType isOptional primitive getPrimitiveValue schema =
        { FieldInfo.Name = name
          FieldInfo.DotnetType = dotnetType
          FieldInfo.IsOptional = isOptional
          FieldInfo.Primitive = primitive
          FieldInfo.GetPrimitiveValue = getPrimitiveValue
          FieldInfo.Schema = schema }

    let ofField (field: PropertyInfo) =
        let dotnetType = field.PropertyType
        let dotnetTypeInfo = DotnetTypeInfo.ofType dotnetType
        let name = field.Name
        let isOptional = dotnetTypeInfo.IsOptional
        let primitive = dotnetTypeInfo.Primitive
        let getValue = FSharpValue.PreComputeRecordFieldReader(field)
        let getPrimitiveValue (record: obj) =
            let value = getValue record
            dotnetTypeInfo.ConvertValueToPrimitive value
        let schema = Schema.Field.create name dotnetTypeInfo.Schema
        create name dotnetType isOptional primitive getPrimitiveValue schema

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
