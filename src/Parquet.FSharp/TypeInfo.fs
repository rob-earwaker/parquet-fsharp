namespace rec Parquet.FSharp

open FSharp.Reflection
open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Reflection

type internal EnumInfo = {
    Type: Type
    ValueType: Type }

type internal FieldInfo = {
    Field: PropertyInfo
    Name: string
    Type: Type
    GetValue: Expression -> Expression }

type internal RecordInfo = {
    Type: Type
    Fields: FieldInfo[]
    CreateFromFieldValues: Expression[] -> Expression }

type internal UnionInfo = {
    Type: Type
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
    Fields: FieldInfo[]
    CreateFromFieldValues: Expression[] -> Expression }

type internal OptionalInfo = {
    Type: Type
    ValueType: Type
    IsNull: Expression -> Expression
    GetValue: Expression -> Expression
    CreateNull: Expression
    CreateFromValue: Expression -> Expression }

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

module internal EnumInfo =
    let private Cache = TypeInfoCache<EnumInfo>()

    let private ofType (enumType: Type) =
        let valueType = Enum.GetUnderlyingType(enumType)
        { EnumInfo.Type = enumType
          EnumInfo.ValueType = valueType }

    let ofTypeCached enumType =
        Cache.GetOrCreate enumType ofType

module internal RecordInfo =
    let private Cache = TypeInfoCache<RecordInfo>()

    let private ofType (recordType: Type) =
        let fields =
            FSharpType.GetRecordFields(recordType)
            |> Array.map (fun field ->
                let getValue (record: Expression) =
                    Expression.Property(record, field)
                    :> Expression
                { FieldInfo.Field = field
                  FieldInfo.Name = field.Name
                  FieldInfo.Type = field.PropertyType
                  FieldInfo.GetValue = getValue })
        let createFromFieldValues =
            let constructor = FSharpValue.PreComputeRecordConstructorInfo(recordType)
            fun (fieldValues: Expression[]) ->
                Expression.New(constructor, fieldValues)
                :> Expression
        { RecordInfo.Type = recordType
          RecordInfo.Fields = fields
          RecordInfo.CreateFromFieldValues = createFromFieldValues }

    let ofTypeCached recordType =
        Cache.GetOrCreate recordType ofType

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
                        { FieldInfo.Field = field
                          FieldInfo.Name = field.Name
                          FieldInfo.Type = field.PropertyType
                          FieldInfo.GetValue = getValue })
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
                failwith $"unsupported tag member info type '{memberInfo.GetType()}'"
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
                        $"union of type '{unionType}' has invalid tag value")
                    yield Expression.Label(returnLabel, Expression.Null(returnLabel.Type))
                })
            :> Expression
        { UnionInfo.Type = unionType
          UnionInfo.UnionCategory = unionCategory
          UnionInfo.GetTag = getTag
          UnionInfo.GetCaseName = getCaseName
          UnionInfo.Cases = unionCases }

    let ofTypeCached unionType =
        Cache.GetOrCreate unionType ofType

module internal OptionalInfo =
    let private Cache = TypeInfoCache<OptionalInfo>()

    let private OptionModuleType =
        Assembly.Load("FSharp.Core").GetTypes()
        |> Array.filter (fun type' -> type'.Name = "OptionModule")
        |> Array.exactlyOne

    let private ofOptionType (optionType: Type) =
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
        { OptionalInfo.Type = optionType
          OptionalInfo.ValueType = valueType
          OptionalInfo.IsNull = isNull
          OptionalInfo.GetValue = getValue
          OptionalInfo.CreateNull = createNull
          OptionalInfo.CreateFromValue = createFromValue }

    let private ofValueOptionType (valueOptionType: Type) =
        let valueType = valueOptionType.GetGenericArguments()[0]
        let isNull =
            let isNoneProperty = valueOptionType.GetProperty("IsNone")
            fun (valueOption: Expression) ->
                Expression.Property(valueOption, isNoneProperty)
                :> Expression
        let getValue =
            let valueProperty = valueOptionType.GetProperty("Value")
            fun valueOption ->
                Expression.Property(valueOption, valueProperty)
                :> Expression
        let createNull = Expression.Property(null, valueOptionType, "None")
        let createFromValue =
            let createSomeMethod = valueOptionType.GetMethod("Some")
            fun (value: Expression) ->
                Expression.Call(createSomeMethod, value)
                :> Expression
        { OptionalInfo.Type = valueOptionType
          OptionalInfo.ValueType = valueType
          OptionalInfo.IsNull = isNull
          OptionalInfo.GetValue = getValue
          OptionalInfo.CreateNull = createNull
          OptionalInfo.CreateFromValue = createFromValue }

    let private ofNullableType (nullableType: Type) =
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
        { OptionalInfo.Type = nullableType
          OptionalInfo.ValueType = valueType
          OptionalInfo.IsNull = isNull
          OptionalInfo.GetValue = getValue
          OptionalInfo.CreateNull = createNull
          OptionalInfo.CreateFromValue = createFromValue }

    let ofOptionTypeCached optionType =
        Cache.GetOrCreate optionType ofOptionType

    let ofValueOptionTypeCached valueOptionType =
        Cache.GetOrCreate valueOptionType ofValueOptionType

    let ofNullableTypeCached nullableType =
        Cache.GetOrCreate nullableType ofNullableType

module internal DotnetType =
    let (|Enum|_|) (dotnetType: Type) =
        if not dotnetType.IsEnum
        then Option.None
        else
            let enumInfo = EnumInfo.ofTypeCached dotnetType
            if enumInfo.ValueType = typeof<int8>
                || enumInfo.ValueType = typeof<int16>
                || enumInfo.ValueType = typeof<int32>
                || enumInfo.ValueType = typeof<int64>
                || enumInfo.ValueType = typeof<uint8>
                || enumInfo.ValueType = typeof<uint16>
                || enumInfo.ValueType = typeof<uint32>
                || enumInfo.ValueType = typeof<uint64>
            then Option.Some enumInfo
            else Option.None

    let isGenericType<'GenericType> (dotnetType: Type) =
        dotnetType.IsGenericType
        && dotnetType.GetGenericTypeDefinition() = typedefof<'GenericType>

    let (|Option|_|) (dotnetType: Type) =
        if DotnetType.isGenericType<option<_>> dotnetType
        then Option.Some (OptionalInfo.ofOptionTypeCached dotnetType)
        else Option.None

    let (|ValueOption|_|) (dotnetType: Type) =
        if DotnetType.isGenericType<voption<_>> dotnetType
        then Option.Some (OptionalInfo.ofValueOptionTypeCached dotnetType)
        else Option.None

    let (|Nullable|_|) (dotnetType: Type) =
        if DotnetType.isGenericType<Nullable<_>> dotnetType
        then Option.Some (OptionalInfo.ofNullableTypeCached dotnetType)
        else Option.None

    let (|Record|_|) dotnetType =
        if FSharpType.IsRecord(dotnetType)
        then Option.Some (RecordInfo.ofTypeCached dotnetType)
        else Option.None

    let (|Union|_|) dotnetType =
        // Explicitly exclude union types that are handled in a special way.
        // This ultimately means that these union types can't be serailized by
        // the default union converter, so if converters associated with these
        // types cannot be used we'll get an exception rather than silently
        // succeeding and producing an overly verbose serialization schema.
        if FSharpType.IsUnion(dotnetType)
            && not (DotnetType.isGenericType<list<_>> dotnetType)
            && not (DotnetType.isGenericType<option<_>> dotnetType)
            && not (DotnetType.isGenericType<voption<_>> dotnetType)
        then Option.Some (UnionInfo.ofTypeCached dotnetType)
        else Option.None
