namespace rec Parquet.FSharp

open FSharp.Reflection
open System
open System.Collections.Generic
open System.Linq.Expressions

type private TypeInfoCache<'TypeInfo>() =
    let cache = Dictionary<Type, 'TypeInfo>()

    let tryGetCached dotnetType =
        lock cache (fun () ->
            match cache.TryGetValue(dotnetType) with
            | false, _ -> Option.None
            | true, typeInfo -> Option.Some typeInfo)

    let addToCache dotnetType typeInfo =
        lock cache (fun () ->
            cache[dotnetType] <- typeInfo)

    member this.GetOrCreate (dotnetType: Type) create =
        match tryGetCached dotnetType with
        | Option.Some typeInfo -> typeInfo
        | Option.None ->
            let typeInfo = create dotnetType
            addToCache dotnetType typeInfo
            typeInfo

type internal NullableInfo = {
    Type: Type
    ValueType: Type
    IsNull: Expression -> Expression
    GetValue: Expression -> Expression
    CreateNull: Expression
    CreateFromValue: Expression -> Expression }

module internal NullableInfo =
    let private Cache = TypeInfoCache<NullableInfo>()

    let private ofType (nullableType: Type) =
        let valueType = Nullable.GetUnderlyingType(nullableType)
        let isNull (nullable: Expression) =
            Expression.Not(Expression.Property(nullable, "HasValue"))
            :> Expression
        let getValue (nullable: Expression) =
            Expression.Property(nullable, "Value")
            :> Expression
        let createNull = Expression.Null(nullableType)
        let createFromValue =
            let constructor = nullableType.GetConstructor([| valueType |])
            fun (value: Expression) ->
                Expression.New(constructor, value)
                :> Expression
        { NullableInfo.Type = nullableType
          NullableInfo.ValueType = valueType
          NullableInfo.IsNull = isNull
          NullableInfo.GetValue = getValue
          NullableInfo.CreateNull = createNull
          NullableInfo.CreateFromValue = createFromValue }

    let ofTypeCached nullableType =
        Cache.GetOrCreate nullableType ofType

module internal DotnetType =
    // TODO: Check how much of this is actually used.

    let private ActivePatternTypeMatch<'Type> dotnetType =
        if dotnetType = typeof<'Type>
        then Option.Some ()
        else Option.None

    let (|Bool|_|) = ActivePatternTypeMatch<bool>
    let (|Int8|_|) = ActivePatternTypeMatch<int8>
    let (|Int16|_|) = ActivePatternTypeMatch<int16>
    let (|Int32|_|) = ActivePatternTypeMatch<int>
    let (|Int64|_|) = ActivePatternTypeMatch<int64>
    let (|UInt8|_|) = ActivePatternTypeMatch<uint8>
    let (|UInt16|_|) = ActivePatternTypeMatch<uint16>
    let (|UInt32|_|) = ActivePatternTypeMatch<uint>
    let (|UInt64|_|) = ActivePatternTypeMatch<uint64>
    let (|Float32|_|) = ActivePatternTypeMatch<float32>
    let (|Float64|_|) = ActivePatternTypeMatch<float>
    let (|Decimal|_|) = ActivePatternTypeMatch<decimal>
    let (|Guid|_|) = ActivePatternTypeMatch<Guid>
    let (|DateTime|_|) = ActivePatternTypeMatch<DateTime>
    let (|DateTimeOffset|_|) = ActivePatternTypeMatch<DateTimeOffset>
    let (|String|_|) = ActivePatternTypeMatch<string>
    let (|ByteArray|_|) = ActivePatternTypeMatch<byte[]>

    let isGenericType<'GenericType> (dotnetType: Type) =
        dotnetType.IsGenericType
        && dotnetType.GetGenericTypeDefinition() = typedefof<'GenericType>
    
    let private ActivePatternGenericTypeMatch<'GenericType> (dotnetType: Type) =
        if isGenericType<'GenericType> dotnetType
        then Option.Some ()
        else Option.None

    let (|GenericList|_|) = ActivePatternGenericTypeMatch<ResizeArray<_>>
    let (|FSharpList|_|) = ActivePatternGenericTypeMatch<list<_>>
    let (|Option|_|) = ActivePatternGenericTypeMatch<option<_>>

    let (|Nullable|_|) (dotnetType: Type) =
        if DotnetType.isGenericType<Nullable<_>> dotnetType
        then Option.Some (NullableInfo.ofTypeCached dotnetType)
        else Option.None

    let (|Array1d|_|) (dotnetType: Type) =
        if dotnetType.IsArray
            && dotnetType.GetArrayRank() = 1
        then Option.Some ()
        else Option.None

    let (|Record|_|) dotnetType =
        if FSharpType.IsRecord(dotnetType)
        then Option.Some ()
        else Option.None

    let (|Union|_|) dotnetType =
        if FSharpType.IsUnion(dotnetType)
        then Option.Some ()
        else Option.None
