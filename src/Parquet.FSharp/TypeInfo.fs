namespace rec Parquet.FSharp

open FSharp.Reflection
open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Reflection

type ValueInfo =
    | Atomic of AtomicInfo
    | List of ListInfo
    | Record of RecordInfo
    with
    member this.DotnetType =
        match this with
        | ValueInfo.Atomic atomicInfo -> atomicInfo.DotnetType
        | ValueInfo.List listInfo -> listInfo.DotnetType
        | ValueInfo.Record recordInfo -> recordInfo.DotnetType

    member this.IsOptional =
        match this with
        | ValueInfo.Atomic atomicInfo -> atomicInfo.IsOptional
        | ValueInfo.List listInfo -> listInfo.IsOptional
        | ValueInfo.Record recordInfo -> recordInfo.IsOptional

// TODO: Types supported by Parquet.Net:

//   Implemented:
//     - bool
//     - int8, int16, int32, int64
//     - uint8, uint16, uint32, uint64
//     - float32, float64
//     - decimal
//     - DateTime
//     - string
//     - Guid

//   Not implemented:
//     - BigInteger
//     - byte[]
//     - DateOnly, TimeOnly
//     - TimeSpan, Interval
//     - Enums?

type AtomicInfo = {
    DotnetType: Type
    IsOptional: bool
    DataDotnetType: Type
    IsNull: Expression -> Expression
    GetDataValue: Expression -> Expression
    CreateFromDataValue: Expression -> Expression
    CreateNull: Expression }

type ListInfo = {
    DotnetType: Type
    IsOptional: bool
    // TODO: Should this have a 'ValueDotnetType' like RecordInfo?
    // Probably useful when it's an optional list to save calling .Value all the time.
    ElementInfo: ValueInfo
    IsNull: Expression -> Expression
    GetLength: Expression -> Expression
    GetElement: Expression * Expression -> Expression
    CreateNull: Expression
    CreateEmpty: Expression
    CreateFromElementValues: Expression -> Expression }

type FieldInfo = {
    Name: string
    ValueInfo: ValueInfo
    GetValue: Expression -> Expression }

type RecordInfo = {
    DotnetType: Type
    ValueDotnetType: Type
    IsOptional: bool
    Fields: FieldInfo[]
    IsNull: Expression -> Expression
    GetValue: Expression -> Expression
    CreateFromFieldValues: Expression[] -> Expression
    CreateNull: Expression }

module private Nullable =
    let isNull (nullableValue: Expression) =
        Expression.Not(
            Expression.Property(nullableValue, "HasValue"))
        :> Expression

    let getValue (nullableValue: Expression) =
        Expression.Property(nullableValue, "Value")
        :> Expression

    let createValue dotnetType =
        let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
        let constructor = dotnetType.GetConstructor([| valueDotnetType |])
        fun (value: Expression) ->
            Expression.New(constructor, value)
            :> Expression

    let createNull dotnetType =
        Expression.Null(dotnetType)

module private Option =
    let private getSomeCase dotnetType =
        let unionCases = FSharpType.GetUnionCases(dotnetType)
        unionCases |> Array.find _.Name.Equals("Some")
        
    let private getNoneCase dotnetType =
        let unionCases = FSharpType.GetUnionCases(dotnetType)
        unionCases |> Array.find _.Name.Equals("None")

    let isNone valueDotnetType =
        let optionModuleType =
            Assembly.Load("FSharp.Core").GetTypes()
            |> Array.filter (fun type' -> type'.Name = "OptionModule")
            |> Array.exactlyOne
        fun (valueOption: Expression) ->
            Expression.Call(optionModuleType, "IsNone", [| valueDotnetType |], valueOption)
            :> Expression

    let getValue (valueOption: Expression) =
        Expression.Property(valueOption, "Value")
        :> Expression

    let createSome dotnetType =
        let someCase = getSomeCase dotnetType
        let constructorMethod = FSharpValue.PreComputeUnionConstructorInfo(someCase)
        fun (value: Expression) ->
            Expression.Call(constructorMethod, value)
            :> Expression

    let createNone dotnetType =
        let noneCase = getNoneCase dotnetType
        let constructorMethod = FSharpValue.PreComputeUnionConstructorInfo(noneCase)
        Expression.Call(constructorMethod, [||])
        :> Expression

module private DotnetType =
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
    let (|String|_|) = ActivePatternTypeMatch<string>
    let (|Guid|_|) = ActivePatternTypeMatch<Guid>
    let (|DateTime|_|) = ActivePatternTypeMatch<DateTime>
    let (|DateTimeOffset|_|) = ActivePatternTypeMatch<DateTimeOffset>
    
    let private ActivePatternGenericTypeMatch<'GenericType> (dotnetType: Type) =
        if dotnetType.IsGenericType
            && dotnetType.GetGenericTypeDefinition() = typedefof<'GenericType>
        then Option.Some ()
        else Option.None

    let (|GenericList|_|) = ActivePatternGenericTypeMatch<ResizeArray<_>>
    let (|FSharpList|_|) = ActivePatternGenericTypeMatch<list<_>>
    let (|Nullable|_|) = ActivePatternGenericTypeMatch<Nullable<_>>
    let (|Option|_|) = ActivePatternGenericTypeMatch<option<_>>

    let (|Array1d|_|) (dotnetType: Type) =
        if dotnetType.IsArray
            && dotnetType.GetArrayRank() = 1
        then Option.Some ()
        else Option.None

    // TODO: Might need to rename this if ambiguous with C# record types.
    let (|Record|_|) dotnetType =
        if FSharpType.IsRecord(dotnetType)
        then Option.Some ()
        else Option.None

    let (|Union|_|) dotnetType =
        if FSharpType.IsUnion(dotnetType)
        then Option.Some ()
        else Option.None

module ValueInfo =
    let private Cache = Dictionary<Type, ValueInfo>()

    let private tryGetCached dotnetType =
        lock Cache (fun () ->
            match Cache.TryGetValue(dotnetType) with
            | false, _ -> Option.None
            | true, valueInfo -> Option.Some valueInfo)

    let private addToCache dotnetType valueInfo =
        lock Cache (fun () ->
            Cache[dotnetType] <- valueInfo)

    let private ofNullable (dotnetType: Type) =
        let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
        match ValueInfo.ofType valueDotnetType with
        | ValueInfo.Atomic atomicInfo ->
            let nullableAtomicInfo = AtomicInfo.ofNullable dotnetType atomicInfo
            ValueInfo.Atomic nullableAtomicInfo
        | ValueInfo.List listInfo ->
            let nullableListInfo = ListInfo.ofNullable dotnetType listInfo
            ValueInfo.List nullableListInfo
        | ValueInfo.Record recordInfo ->
            let nullableRecordInfo = RecordInfo.ofNullable dotnetType recordInfo
            ValueInfo.Record nullableRecordInfo

    let private ofOptionOuter (dotnetType: Type) (valueInfo: ValueInfo) =
        let dotnetType = dotnetType
        let valueDotnetType = dotnetType
        let isOptional = true
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                let getValue = Option.getValue
                FieldInfo.create name valueInfo getValue
            [| valueField |]
        let isNull = Option.isNone valueInfo.DotnetType
        let getValue = id
        let createFromFieldValues =
            let createSome = Option.createSome dotnetType
            fun (fieldValues: Expression[]) ->
                createSome fieldValues[0]
        let createNull = Option.createNone dotnetType
        RecordInfo.create dotnetType valueDotnetType isOptional fields
            isNull getValue createFromFieldValues createNull
        |> ValueInfo.Record

    let private ofOptionInner dotnetType valueInfo =
        match valueInfo with
        | ValueInfo.Atomic atomicInfo ->
            AtomicInfo.ofOptionInner dotnetType atomicInfo
            |> ValueInfo.Atomic
        | ValueInfo.List listInfo ->
            ListInfo.ofOptionInner dotnetType listInfo
            |> ValueInfo.List
        | ValueInfo.Record recordInfo ->
            RecordInfo.ofOptionInner dotnetType recordInfo
            |> ValueInfo.Record

    let private ofOption (dotnetType: Type) =
        let valueDotnetType = dotnetType.GetGenericArguments()[0]
        let valueInfo = ValueInfo.ofType valueDotnetType
        if valueInfo.IsOptional
        then ValueInfo.ofOptionOuter dotnetType valueInfo
        else ValueInfo.ofOptionInner dotnetType valueInfo

    type private UnionInfo = {
        DotnetType: Type
        GetTag: Expression -> Expression
        GetCaseName: Expression -> Expression
        UnionCases: UnionCaseInfo[] }

    type private UnionCaseInfo = {
        DotnetType: Type
        Tag: Expression
        Name: string
        Fields: PropertyInfo[]
        CreateFromFieldValues: Expression[] -> Expression  }

    module private UnionInfo =
        let ofUnion dotnetType =
            let unionCases =
                FSharpType.GetUnionCases(dotnetType)
                |> Array.map (fun unionCase ->
                    let createFromFieldValues =
                        let constructorMethod = FSharpValue.PreComputeUnionConstructorInfo(unionCase)
                        fun (fieldValues: Expression[]) ->
                            Expression.Call(constructorMethod, fieldValues)
                            :> Expression
                    let dotnetType =
                        match unionCase.GetFields() with
                        | [||] -> dotnetType
                        | fields -> fields[0].DeclaringType
                    { UnionCaseInfo.DotnetType = dotnetType
                      UnionCaseInfo.Tag = Expression.Constant(unionCase.Tag)
                      UnionCaseInfo.Name = unionCase.Name
                      UnionCaseInfo.Fields = unionCase.GetFields()
                      UnionCaseInfo.CreateFromFieldValues = createFromFieldValues })
            let getTag =
                // TODO: Can some of these options be removed in practice? Do we even
                // need to precompute if there's only one option?
                match FSharpValue.PreComputeUnionTagMemberInfo(dotnetType) with
                | :? MethodInfo as methodInfo ->
                    if methodInfo.IsStatic
                    then
                        fun (union: Expression) ->
                            Expression.Call(methodInfo, union)
                            :> Expression
                    else
                        fun (union: Expression) ->
                            Expression.Call(union, methodInfo)
                            :> Expression
                | :? PropertyInfo as propertyInfo ->
                    fun (union: Expression) ->
                        Expression.Property(union, propertyInfo)
                        :> Expression
                | memberInfo ->
                    failwith $"unsupported tag member info type '{memberInfo.GetType().FullName}'"
            let getCaseName (union: Expression) =
                let tag = Expression.Variable(typeof<int>, "tag")
                let failWithInvalidTag =
                    Expression.FailWith(
                        $"union of type '{dotnetType.FullName}' has invalid tag value")
                let returnLabel = Expression.Label(typeof<string>, "caseName")
                Expression.Block(
                    [ tag ],
                    Expression.Assign(tag, getTag union),
                    unionCases
                    |> Array.rev
                    |> Array.fold
                        (fun (ifFalse: Expression) caseInfo ->
                            let test = Expression.Equal(tag, caseInfo.Tag)
                            let ifTrue = Expression.Return(returnLabel, Expression.Constant(caseInfo.Name))
                            Expression.IfThenElse(test, ifTrue, ifFalse))
                        failWithInvalidTag,
                    Expression.Label(returnLabel, Expression.Constant(null, typeof<string>)))
                :> Expression
            { UnionInfo.DotnetType = dotnetType
              UnionInfo.GetTag = getTag
              UnionInfo.GetCaseName = getCaseName
              UnionInfo.UnionCases = unionCases }

    let private ofUnionEnum (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.DotnetType
        // We represent unions that are equivalent to enums, i.e. all cases have
        // zero fields, as a string value containing the case name. Since a
        // union value can't be null and must be one of the possible cases, this
        // value is not optional despite being a string value.
        let isOptional = false
        let dataDotnetType = typeof<string>
        let isNull = fun (union: Expression) -> Expression.False
        let getDataValue = unionInfo.GetCaseName
        let createFromDataValue (caseName: Expression) =
            let failWithInvalidName =
                Expression.FailWith(
                    $"union of type '{dotnetType.FullName}' has invalid name value")
            let returnLabel = Expression.Label(dotnetType, "union")
            Expression.Block(
                unionInfo.UnionCases
                |> Array.rev
                |> Array.fold
                    (fun (ifFalse: Expression) caseInfo ->
                        let test = Expression.Equal(caseName, Expression.Constant(caseInfo.Name))
                        let ifTrue = Expression.Return(returnLabel, caseInfo.CreateFromFieldValues [||])
                        Expression.IfThenElse(test, ifTrue, ifFalse))
                    failWithInvalidName,
                Expression.Label(returnLabel, Expression.Default(dotnetType)))
            :> Expression
        let createNull =
            Expression.Block(
                Expression.FailWith($"type '{dotnetType.FullName}' is not optional"),
                Expression.Default(dotnetType))
            :> Expression
        AtomicInfo.create dotnetType isOptional dataDotnetType
            isNull getDataValue createFromDataValue createNull
        |> ValueInfo.Atomic

    let private ofUnionCaseData (unionInfo: UnionInfo) (unionCase: UnionCaseInfo) =
        let dotnetType = unionInfo.DotnetType
        let valueDotnetType = unionCase.DotnetType
        // Each union case is treated as optional. Only one case from the union
        // can be set, so the others will be NULL.
        let isOptional = true
        let fields = unionCase.Fields |> Array.map FieldInfo.ofProperty
        let isNull (union: Expression) =
            Expression.NotEqual(unionInfo.GetTag union, unionCase.Tag)
            :> Expression
        let getValue (union: Expression) =
            Expression.Convert(union, unionCase.DotnetType)
            :> Expression
        let createFromFieldValues = unionCase.CreateFromFieldValues
        let createNull = Expression.Default(dotnetType)
        RecordInfo.create dotnetType valueDotnetType isOptional fields
            isNull getValue createFromFieldValues createNull
        |> ValueInfo.Record

    let private ofUnionData (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.DotnetType
        let valueDotnetType = unionInfo.DotnetType
        // Some union cases might not have any associated data fields. When
        // representing these cases we want to be able to set the data to NULL,
        // so this virtual record type must be optional.
        let isOptional = true
        let unionCasesWithFields =
            unionInfo.UnionCases
            |> Array.filter (fun unionCase -> unionCase.Fields.Length > 0)
        let fields =
            unionCasesWithFields
            |> Array.map (fun unionCase ->
                let name = unionCase.Name
                let valueInfo = ValueInfo.ofUnionCaseData unionInfo unionCase
                let getValue = id
                FieldInfo.create name valueInfo getValue)
        let isNull' (union: Expression) =
            // TODO: This whole thing could almost certainly be made more elegant
            // and performant by implementing a UnionShredder and UnionAssembler
            // but this would require big changes!
            let tag = Expression.Variable(typeof<int>, "tag")
            Expression.Block(
                [ tag ],
                Expression.Assign(tag, unionInfo.GetTag union),
                unionCasesWithFields
                |> Array.map (fun unionCase ->
                    Expression.NotEqual(tag, unionCase.Tag)
                    :> Expression)
                |> Expression.AndAlso)
            :> Expression
        let getValue = id
        let createFromFieldValues (fieldValues: Expression[]) =
            let failWithAllCaseDataNull =
                Expression.FailWith(
                    $"all case data fields are null for union of type '{dotnetType.FullName}'")
            let returnLabel = Expression.Label(dotnetType, "union")
            Expression.Block(
                Array.zip unionCasesWithFields fieldValues
                |> Array.rev
                |> Array.fold
                    (fun (ifFalse: Expression) (caseInfo, fieldValue) ->
                        // TODO: Should technically use the 'IsNull' function from the
                        // relevant field here instead checking directly.
                        // TODO: Slightly hacky to box before checking for null. This gets around
                        // the fact that null is not a proper value for a union.
                        let test =
                            Expression.Not(
                                Expression.IsNull(
                                    Expression.Convert(fieldValue, typeof<obj>)))
                        let ifTrue = Expression.Return(returnLabel, fieldValue)
                        Expression.IfThenElse(test, ifTrue, ifFalse))
                    failWithAllCaseDataNull,
                Expression.Label(returnLabel, Expression.Default(returnLabel.Type)))
            :> Expression
        let createNull = Expression.Default(dotnetType)
        RecordInfo.create dotnetType valueDotnetType isOptional fields
            isNull' getValue createFromFieldValues createNull
        |> ValueInfo.Record

    let private ofUnionComplex (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.DotnetType
        let valueDotnetType = unionInfo.DotnetType
        // TODO: F# unions are not nullable by default, however we are mapping
        // to a struct and Parquet.Net does not support struct fields that
        // aren't nullable.
        let isOptional = true
        let fields =
            let unionCaseField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "UnionCase"
                // TODO: This should really be a non-optional string like for the union enum
                let valueInfo = AtomicInfo.String |> ValueInfo.Atomic
                let getValue = unionInfo.GetCaseName
                FieldInfo.create name valueInfo getValue
            let dataField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Data"
                let valueInfo = ValueInfo.ofUnionData unionInfo
                let getValue = id
                FieldInfo.create name valueInfo getValue
            [| unionCaseField; dataField |]
        let isNull = fun (union: Expression) -> Expression.False
        let getValue = id
        let createFromFieldValues (fieldValues: Expression[]) =
            let caseName = Expression.Variable(typeof<string>, "caseName")
            let caseData = Expression.Variable(dotnetType, "caseData")
            let failWithInvalidName =
                Expression.FailWith(
                    $"union of type '{dotnetType.FullName}' has invalid name value")
            let returnLabel = Expression.Label(dotnetType, "union")
            Expression.Block(
                [ caseName; caseData ],
                Expression.Assign(caseName, fieldValues[0]),
                Expression.Assign(caseData, fieldValues[1]),
                Expression.IfThenElse(
                    // TODO: Should technically use the 'IsNull' function from the
                    // relevant field here instead checking directly.
                    // TODO: Slightly hacky to box before checking for null. This gets around
                    // the fact that null is not a proper value for a union.
                    Expression.Not(Expression.IsNull(Expression.Convert(caseData, typeof<obj>))),
                    Expression.Return(returnLabel, caseData),
                    unionInfo.UnionCases
                    |> Array.filter (fun caseInfo -> caseInfo.Fields.Length = 0)
                    |> Array.rev
                    |> Array.fold
                        (fun (ifFalse: Expression) caseInfo ->
                            let test = Expression.Equal(caseName, Expression.Constant(caseInfo.Name))
                            let union = caseInfo.CreateFromFieldValues [||]
                            let ifTrue = Expression.Return(returnLabel, union)
                            Expression.IfThenElse(test, ifTrue, ifFalse))
                        failWithInvalidName),
                Expression.Label(returnLabel, Expression.Default(returnLabel.Type)))
            :> Expression
        let createNull =
            Expression.Block(
                Expression.FailWith($"type '{dotnetType.FullName}' is not optional"),
                Expression.Default(dotnetType))
            :> Expression
        RecordInfo.create dotnetType valueDotnetType isOptional fields
            isNull getValue createFromFieldValues createNull
        |> ValueInfo.Record

    let private ofUnion dotnetType =
        // Unions are represented in one of two ways. For unions where none of
        // the cases have any associated fields this is similar to an enum and
        // we represent it as a string value where the value is the case name.
        // For unions where at least one of the cases has at least one
        // associated field we need a more complex data structure. We represent
        // this as a record with two fields, one containing the case name and
        // another containing the case data.
        let unionInfo = UnionInfo.ofUnion dotnetType
        if unionInfo.UnionCases
            |> Array.forall (fun unionCase -> unionCase.Fields.Length = 0)
        then ValueInfo.ofUnionEnum unionInfo
        else ValueInfo.ofUnionComplex unionInfo

    let ofType (dotnetType: Type) =
        match tryGetCached dotnetType with
        | Option.Some valueInfo -> valueInfo
        | Option.None ->
            let valueInfo =
                match dotnetType with
                | DotnetType.Bool -> ValueInfo.Atomic AtomicInfo.Bool
                | DotnetType.Int8 -> ValueInfo.Atomic AtomicInfo.Int8
                | DotnetType.Int16 -> ValueInfo.Atomic AtomicInfo.Int16
                | DotnetType.Int32 -> ValueInfo.Atomic AtomicInfo.Int32
                | DotnetType.Int64 -> ValueInfo.Atomic AtomicInfo.Int64
                | DotnetType.UInt8 -> ValueInfo.Atomic AtomicInfo.UInt8
                | DotnetType.UInt16 -> ValueInfo.Atomic AtomicInfo.UInt16
                | DotnetType.UInt32 -> ValueInfo.Atomic AtomicInfo.UInt32
                | DotnetType.UInt64 -> ValueInfo.Atomic AtomicInfo.UInt64
                | DotnetType.Float32 -> ValueInfo.Atomic AtomicInfo.Float32
                | DotnetType.Float64 -> ValueInfo.Atomic AtomicInfo.Float64
                | DotnetType.Decimal -> ValueInfo.Atomic AtomicInfo.Decimal
                | DotnetType.DateTime -> ValueInfo.Atomic AtomicInfo.DateTime
                | DotnetType.DateTimeOffset -> ValueInfo.Atomic AtomicInfo.DateTimeOffset
                | DotnetType.String -> ValueInfo.Atomic AtomicInfo.String
                | DotnetType.Guid -> ValueInfo.Atomic AtomicInfo.Guid
                | DotnetType.Array1d -> ValueInfo.List (ListInfo.ofArray1d dotnetType)
                | DotnetType.GenericList -> ValueInfo.List (ListInfo.ofGenericList dotnetType)
                | DotnetType.FSharpList -> ValueInfo.List (ListInfo.ofFSharpList dotnetType)
                | DotnetType.Record -> ValueInfo.Record (RecordInfo.ofRecord dotnetType)
                | DotnetType.Nullable -> ValueInfo.ofNullable dotnetType
                | DotnetType.Option -> ValueInfo.ofOption dotnetType
                // This must come after the option type since option types are
                // handled in a special way.
                | DotnetType.Union -> ValueInfo.ofUnion dotnetType
                | _ -> failwith $"unsupported type '{dotnetType.FullName}'"
            addToCache dotnetType valueInfo
            valueInfo

    let of'<'Value> =
        ofType typeof<'Value>

module private AtomicInfo =
    let create
        dotnetType isOptional dataDotnetType
        isNull getDataValue createFromDataValue createNull =
        { AtomicInfo.DotnetType = dotnetType
          AtomicInfo.IsOptional = isOptional
          AtomicInfo.DataDotnetType = dataDotnetType
          AtomicInfo.IsNull = isNull
          AtomicInfo.GetDataValue = getDataValue
          AtomicInfo.CreateFromDataValue = createFromDataValue
          AtomicInfo.CreateNull = createNull }

    let private ofPrimitive<'Value> =
        let dotnetType = typeof<'Value>
        let isOptional = false
        let dataDotnetType = dotnetType
        let isNull = fun (value: Expression) -> Expression.False
        let getDataValue = id
        let createFromDataValue = id
        let createNull =
            Expression.Block(
                Expression.FailWith($"type '{dotnetType.FullName}' is not optional"),
                Expression.Default(dotnetType))
            :> Expression
        create dotnetType isOptional dataDotnetType
            isNull getDataValue createFromDataValue createNull

    let Bool = ofPrimitive<bool>
    let Int8 = ofPrimitive<int8>
    let Int16 = ofPrimitive<int16>
    let Int32 = ofPrimitive<int>
    let Int64 = ofPrimitive<int64>
    let UInt8 = ofPrimitive<uint8>
    let UInt16 = ofPrimitive<uint16>
    let UInt32 = ofPrimitive<uint32>
    let UInt64 = ofPrimitive<uint64>
    let Float32 = ofPrimitive<float32>
    let Float64 = ofPrimitive<float>
    let Decimal = ofPrimitive<decimal>
    let DateTime = ofPrimitive<DateTime>
    let Guid = ofPrimitive<Guid>

    let DateTimeOffset =
        let dotnetType = typeof<DateTimeOffset>
        let isOptional = false
        let dataDotnetType = typeof<DateTime>
        let isNull = fun (value: Expression) -> Expression.False
        let getDataValue (value: Expression) =
            Expression.Property(value, "UtcDateTime")
            :> Expression
        let createFromDataValue (dataValue: Expression) =
            Expression.New(
                typeof<DateTimeOffset>.GetConstructor([| typeof<DateTime> |]),
                Expression.Call(dataValue, "ToUniversalTime", [||], [||]))
            :> Expression
        let createNull =
            Expression.Block(
                Expression.FailWith($"type '{dotnetType.FullName}' is not optional"),
                Expression.Default(dotnetType))
            :> Expression
        create dotnetType isOptional dataDotnetType
            isNull getDataValue createFromDataValue createNull

    let String =
        let dotnetType = typeof<string>
        let isOptional = true
        let dataDotnetType = dotnetType
        let isNull = Expression.IsNull
        let getDataValue = id
        let createFromDataValue = id
        let createNull = Expression.Null(dotnetType)
        create dotnetType isOptional dataDotnetType
            isNull getDataValue createFromDataValue createNull

    let ofNullable (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let dataDotnetType = valueInfo.DataDotnetType
        let isNull = Nullable.isNull
        let getDataValue (nullableValue: Expression) =
            let value = Nullable.getValue nullableValue
            valueInfo.GetDataValue value
        let createFromDataValue =
            let createValue = Nullable.createValue dotnetType
            fun (dataValue: Expression) ->
                let value = valueInfo.CreateFromDataValue dataValue
                createValue value
        let createNull = Nullable.createNull dotnetType
        create dotnetType isOptional dataDotnetType
            isNull getDataValue createFromDataValue createNull

    let ofOptionInner (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let dataDotnetType = valueInfo.DataDotnetType
        let isNull = Option.isNone valueInfo.DotnetType
        let getDataValue (valueOption: Expression) =
            let value = Option.getValue valueOption
            valueInfo.GetDataValue value
        let createFromDataValue =
            let createSome = Option.createSome dotnetType
            fun (dataValue: Expression) ->
                let value = valueInfo.CreateFromDataValue dataValue
                createSome value
        let createNull = Option.createNone dotnetType
        create dotnetType isOptional dataDotnetType
            isNull getDataValue createFromDataValue createNull

module private ListInfo =
    let private create
        dotnetType isOptional elementInfo
        isNull getLength getElement
        createNull createEmpty createFromElementValues =
        { ListInfo.DotnetType = dotnetType
          ListInfo.IsOptional = isOptional
          ListInfo.ElementInfo = elementInfo
          ListInfo.IsNull = isNull
          ListInfo.GetLength = getLength
          ListInfo.GetElement = getElement
          ListInfo.CreateNull = createNull
          ListInfo.CreateEmpty = createEmpty
          ListInfo.CreateFromElementValues = createFromElementValues }

    let ofArray1d (dotnetType: Type) =
        let isOptional = true
        let elementDotnetType = dotnetType.GetElementType()
        let elementInfo = ValueInfo.ofType elementDotnetType
        let isNull = Expression.IsNull
        let getLength (array: Expression) =
            Expression.Property(array, "Length")
            :> Expression
        let getElement (array: Expression, index: Expression) =
            Expression.ArrayIndex(array, [ index ])
            :> Expression
        let createNull = Expression.Null(dotnetType)
        let createEmpty =
            Expression.NewArrayBounds(elementDotnetType, Expression.Constant(0))
        let createFromElementValues (elementValues: Expression) =
            Expression.Call(elementValues, "ToArray", [||], [||])
            :> Expression
        create dotnetType isOptional elementInfo
            isNull getLength getElement
            createNull createEmpty createFromElementValues

    let ofGenericList (dotnetType: Type) =
        let isOptional = true
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementInfo = ValueInfo.ofType elementDotnetType
        let isNull = Expression.IsNull
        let getLength (list: Expression) =
            Expression.Property(list, "Count")
            :> Expression
        let getElement (list: Expression, index: Expression) =
            Expression.MakeIndex(list, dotnetType.GetProperty("Item"), [ index ])
            :> Expression
        let createNull = Expression.Null(dotnetType)
        let createEmpty = Expression.New(dotnetType)
        let createFromElementValues = id
        create dotnetType isOptional elementInfo
            isNull getLength getElement
            createNull createEmpty createFromElementValues

    let ofFSharpList (dotnetType: Type) =
        // TODO: F# lists are not nullable by default, however Parquet.Net
        // does not support list fields that aren't nullable and a nullable
        // list field class can't easily be created since some of the relevant
        // properties are internal. For now, treat all lists as optional, even
        // though in practice they will never be null.
        let isOptional = true
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementInfo = ValueInfo.ofType elementDotnetType
        let isNull = fun (list: Expression) -> Expression.False
        let getLength (list: Expression) =
            Expression.Property(list, "Length")
            :> Expression
        let getElement =
            let itemProperty = dotnetType.GetProperty("Item")
            fun (list: Expression, index: Expression) ->
                Expression.MakeIndex(list, itemProperty, [ index ])
                :> Expression
        let createNull =
            Expression.Block(
                Expression.FailWith($"type '{dotnetType.FullName}' is not optional"),
                Expression.Default(dotnetType))
            :> Expression
        let createEmpty =
            Expression.Property(null, dotnetType.GetProperty("Empty"))
        let createFromElementValues =
            let seqModuleType =
                Assembly.Load("FSharp.Core").GetTypes()
                |> Array.filter (fun type' -> type'.Name = "SeqModule")
                |> Array.exactlyOne
            fun (elementValues: Expression) ->
                Expression.Call(seqModuleType, "ToList", [| elementDotnetType |], elementValues)
                :> Expression
        create dotnetType isOptional elementInfo
            isNull getLength getElement
            createNull createEmpty createFromElementValues

    let ofNullable (dotnetType: Type) (listInfo: ListInfo) =
        let isOptional = true
        let elementInfo = listInfo.ElementInfo
        let isNull = Nullable.isNull
        let getLength (nullableList: Expression) =
            let list = Nullable.getValue nullableList
            listInfo.GetLength list
        let getElement (nullableList: Expression, index: Expression) =
            let list = Nullable.getValue nullableList
            listInfo.GetElement(list, index)
        let createNull = Nullable.createNull dotnetType
        let createEmpty =
            let createValue = Nullable.createValue dotnetType
            createValue listInfo.CreateEmpty
        let createFromElementValues =
            let createValue = Nullable.createValue dotnetType
            fun (elementValues: Expression) ->
                let list = listInfo.CreateFromElementValues elementValues
                createValue list
        create dotnetType isOptional elementInfo
            isNull getLength getElement
            createNull createEmpty createFromElementValues

    let ofOptionInner (dotnetType: Type) (listInfo: ListInfo) =
        let isOptional = true
        let elementInfo = listInfo.ElementInfo
        let isNull = Option.isNone listInfo.DotnetType
        let getLength (listOption: Expression) =
            let list = Option.getValue listOption
            listInfo.GetLength list
        let getElement (listOption: Expression, index: Expression) =
            let list = Option.getValue listOption
            listInfo.GetElement(list, index)
        let createNull = Option.createNone dotnetType
        let createEmpty =
            let createSome = Option.createSome dotnetType
            createSome listInfo.CreateEmpty
        let createFromElementValues =
            let createSome = Option.createSome dotnetType
            fun (elementValues: Expression) ->
                let list = listInfo.CreateFromElementValues elementValues
                createSome list
        create dotnetType isOptional elementInfo
            isNull getLength getElement
            createNull createEmpty createFromElementValues

module private FieldInfo =
    let create name valueInfo getValue =
        { FieldInfo.Name = name
          FieldInfo.ValueInfo = valueInfo
          FieldInfo.GetValue = getValue }

    let ofProperty (field: PropertyInfo) =
        let name = field.Name
        let valueInfo = ValueInfo.ofType field.PropertyType
        let getValue (record: Expression) =
            Expression.Property(record, field)
            :> Expression
        create name valueInfo getValue

module private RecordInfo =
    let create
        dotnetType valueDotnetType isOptional fields
        isNull getValue createFromFieldValues createNull =
        { RecordInfo.DotnetType = dotnetType
          RecordInfo.ValueDotnetType = valueDotnetType
          RecordInfo.IsOptional = isOptional
          RecordInfo.Fields = fields
          RecordInfo.IsNull = isNull
          RecordInfo.GetValue = getValue
          RecordInfo.CreateFromFieldValues = createFromFieldValues
          RecordInfo.CreateNull = createNull }

    let ofRecord dotnetType =
        let valueDotnetType = dotnetType
        // TODO: F# records are not nullable by default, however Parquet.Net
        // does not support struct fields that aren't nullable and a nullable
        // struct field class can't easily be created since some of the relevant
        // properties are internal. For now, treat all records as optional, even
        // though in practice they will never be null.
        let isOptional = true
        let fields =
            FSharpType.GetRecordFields(dotnetType)
            |> Array.map FieldInfo.ofProperty
        let isNull = fun (record: Expression) -> Expression.False
        let getValue = id
        let createFromFieldValues =
            let constructor = FSharpValue.PreComputeRecordConstructorInfo(dotnetType)
            fun (fieldValues: Expression[]) ->
                Expression.New(constructor, fieldValues)
                :> Expression
        let createNull =
            Expression.Block(
                Expression.FailWith($"type '{dotnetType.FullName}' is not optional"),
                Expression.Default(dotnetType))
            :> Expression
        create dotnetType valueDotnetType isOptional fields
            isNull getValue createFromFieldValues createNull

    let ofNullable dotnetType (recordInfo: RecordInfo) =
        let valueDotnetType = recordInfo.DotnetType
        let isOptional = true
        let fields = recordInfo.Fields
        let isNull = Nullable.isNull
        let getValue = Nullable.getValue
        let createFromFieldValues =
            let createValue = Nullable.createValue dotnetType
            fun (fieldValues: Expression[]) ->
                let record = recordInfo.CreateFromFieldValues fieldValues
                createValue record
        let createNull = Nullable.createNull dotnetType
        create dotnetType valueDotnetType isOptional fields
            isNull getValue createFromFieldValues createNull

    let ofOptionInner dotnetType (recordInfo: RecordInfo) =
        let valueDotnetType = recordInfo.DotnetType
        let isOptional = true
        let fields = recordInfo.Fields
        let isNull = Option.isNone valueDotnetType
        let getValue = Option.getValue
        let createFromFieldValues =
            let createSome = Option.createSome dotnetType
            fun (fieldValues: Expression[]) ->
                let record = recordInfo.CreateFromFieldValues fieldValues
                createSome record
        let createNull = Option.createNone dotnetType
        create dotnetType valueDotnetType isOptional fields
            isNull getValue createFromFieldValues createNull
