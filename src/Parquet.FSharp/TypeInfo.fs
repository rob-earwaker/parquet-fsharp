namespace rec Parquet.FSharp

open FSharp.Reflection
open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Reflection

// TODO: Should review this to move reflection and fixed expressions out
// of expression builder functions

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

type internal UnionInfo = {
    DotnetType: Type
    UnionCategory: UnionCategory
    GetTag: Expression -> Expression
    GetCaseName: Expression -> Expression
    Cases: UnionCaseInfo[] }

type internal UnionCategory =
    | Enum
    | SingleCase
    | MultiCase

type internal UnionCaseInfo = {
    Tag: Expression
    Name: string
    Fields: UnionFieldInfo[]
    CreateFromFieldValues: Expression[] -> Expression }

type UnionFieldInfo = {
    Name: string
    DotnetType: Type
    GetValue: Expression -> Expression }

module internal UnionInfo =
    let private Cache = TypeInfoCache<UnionInfo>()

    let private ofType unionType =
        let unionCases =
            FSharpType.GetUnionCases(unionType)
            |> Array.map (fun unionCase ->
                let fields =
                    unionCase.GetFields()
                    |> Array.map (fun field ->
                        let getValue =
                            // Cases with fields are defined in their own
                            // distinct types that inherit from the union type.
                            // In order to access the field, the union must
                            // first be converted to this type.
                            let unionCaseType = field.DeclaringType
                            fun (union: Expression) ->
                                let unionCase = Expression.Convert(union, unionCaseType)
                                Expression.Property(unionCase, field)
                                :> Expression
                        { UnionFieldInfo.Name = field.Name
                          UnionFieldInfo.DotnetType = field.PropertyType
                          UnionFieldInfo.GetValue = getValue })
                let createFromFieldValues =
                    let constructorMethod = FSharpValue.PreComputeUnionConstructorInfo(unionCase)
                    fun (fieldValues: Expression[]) ->
                        Expression.Call(constructorMethod, fieldValues)
                        :> Expression
                { UnionCaseInfo.Tag = Expression.Constant(unionCase.Tag)
                  UnionCaseInfo.Name = unionCase.Name
                  UnionCaseInfo.Fields = fields
                  UnionCaseInfo.CreateFromFieldValues = createFromFieldValues })
        let unionCategory =
            if unionCases |> Array.forall (fun case -> Array.isEmpty case.Fields)
            then UnionCategory.Enum
            elif unionCases.Length = 1
            then UnionCategory.SingleCase
            else UnionCategory.MultiCase
        let getTag =
            match FSharpValue.PreComputeUnionTagMemberInfo(unionType) with
            | :? MethodInfo as method ->
                if method.IsStatic
                then fun (union: Expression) -> Expression.Call(method, union) :> Expression
                else fun (union: Expression) -> Expression.Call(union, method) :> Expression
            | :? PropertyInfo as property ->
                fun (union: Expression) -> Expression.Property(union, property) :> Expression
            | memberInfo ->
                failwith $"unsupported tag member info type '{memberInfo.GetType().FullName}'"
        let getCaseName (union: Expression) =
            let tag = Expression.Variable(typeof<int>, "tag")
            let returnLabel = Expression.Label(typeof<string>, "caseName")
            Expression.Block(
                [ tag ],
                seq<Expression> {
                    yield Expression.Assign(tag, getTag union)
                    yield! unionCases
                        |> Array.map (fun caseInfo ->
                            Expression.IfThen(
                                Expression.Equal(tag, caseInfo.Tag),
                                Expression.Return(returnLabel, Expression.Constant(caseInfo.Name)))
                            :> Expression)
                    yield Expression.FailWith(
                        $"union of type '{unionType.FullName}' has invalid tag value")
                    yield Expression.Label(returnLabel, Expression.Null(returnLabel.Type))
                })
            :> Expression
        { UnionInfo.DotnetType = unionType
          UnionInfo.UnionCategory = unionCategory
          UnionInfo.GetTag = getTag
          UnionInfo.GetCaseName = getCaseName
          UnionInfo.Cases = unionCases }

    let ofTypeCached unionType =
        Cache.GetOrCreate unionType ofType

type internal OptionInfo = {
    Type: Type
    ValueType: Type
    IsNull: Expression -> Expression
    GetValue: Expression -> Expression
    CreateNull: Expression
    CreateFromValue: Expression -> Expression }

module internal OptionInfo =
    let private Cache = TypeInfoCache<OptionInfo>()

    let private OptionModuleType =
        Assembly.Load("FSharp.Core").GetTypes()
        |> Array.filter (fun type' -> type'.Name = "OptionModule")
        |> Array.exactlyOne

    let private ofType (optionType: Type) =
        let unionCases = FSharpType.GetUnionCases(optionType)
        let valueType = optionType.GetGenericArguments()[0]
        let isNull =
            let isNoneMethod =
                OptionModuleType.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                |> Array.find (fun method -> method.Name = "IsNone")
                |> _.MakeGenericMethod(valueType)
            fun (option: Expression) ->
                Expression.Call(isNoneMethod, option)
                :> Expression
        let getValue =
            let valueProperty = optionType.GetProperty("Value")
            fun option ->
                Expression.Property(option, valueProperty)
                :> Expression
        let createNull =
            let noneCase = unionCases |> Array.find _.Name.Equals("None")
            let constructorMethod = FSharpValue.PreComputeUnionConstructorInfo(noneCase)
            Expression.Call(constructorMethod, [||])
            :> Expression
        let createFromValue =
            let someCase = unionCases |> Array.find _.Name.Equals("Some")
            let constructorMethod = FSharpValue.PreComputeUnionConstructorInfo(someCase)
            fun (value: Expression) ->
                Expression.Call(constructorMethod, value)
                :> Expression
        { OptionInfo.Type = optionType
          OptionInfo.ValueType = valueType
          OptionInfo.IsNull = isNull
          OptionInfo.GetValue = getValue
          OptionInfo.CreateNull = createNull
          OptionInfo.CreateFromValue = createFromValue }

    let ofTypeCached nullableType =
        Cache.GetOrCreate nullableType ofType

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
        let isNull =
            let hasValueProperty = nullableType.GetProperty("HasValue")
            fun nullable ->
                let hasValue = Expression.Property(nullable, hasValueProperty)
                Expression.Not(hasValue) :> Expression
        let getValue =
            let valueProperty = nullableType.GetProperty("Value")
            fun nullable ->
                Expression.Property(nullable, valueProperty)
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

    let (|Option|_|) (dotnetType: Type) =
        if DotnetType.isGenericType<option<_>> dotnetType
        then Option.Some (OptionInfo.ofTypeCached dotnetType)
        else Option.None

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
        then Option.Some (UnionInfo.ofTypeCached dotnetType)
        else Option.None
