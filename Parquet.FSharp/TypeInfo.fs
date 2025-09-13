namespace rec Parquet.FSharp

open FSharp.Reflection
open System
open System.Reflection

type FieldInfo = {
    DotnetType: Type
    GetValue: obj -> objnull
    Schema: Schema.Field }
    with
    member this.Name =
        this.Schema.Name

type RecordInfo = {
    Fields: FieldInfo[] }
    with
    member this.Schema =
        this.Fields
        |> Array.map _.Schema
        |> Schema.RecordType.create

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
    let (|Bool|_|) (dotnetType: Type) =
        if dotnetType = typeof<bool>
        then Option.Some ()
        else Option.None

    let (|Int32|_|) (dotnetType: Type) =
        if dotnetType = typeof<int>
        then Option.Some ()
        else Option.None

module FieldInfo =
    let create dotnetType getValue schema =
        { FieldInfo.DotnetType = dotnetType
          FieldInfo.GetValue = getValue
          FieldInfo.Schema = schema }

    let ofField (field: PropertyInfo) =
        let dotnetType = field.PropertyType
        let getValue = FSharpValue.PreComputeRecordFieldReader(field)
        let schema =
            let valueType =
                match dotnetType with
                | DotnetType.Bool -> Schema.ValueType.Bool
                | DotnetType.Int32 -> Schema.ValueType.Int32
                | _ -> failwith $"unsupported type '{dotnetType.FullName}'"
            let value = Schema.Value.required valueType
            Schema.Field.create field.Name value
        create dotnetType getValue schema

module RecordInfo =
    let create fields =
        { RecordInfo.Fields = fields }

    let ofRecordType dotnetType =
        if not (FSharpType.IsRecord(dotnetType))
        then failwith "type is not an F# record type"
        else
            FSharpType.GetRecordFields(dotnetType)
            |> Array.map FieldInfo.ofField
            |> RecordInfo.create

    let ofRecord<'Record> =
        ofRecordType typeof<'Record>
