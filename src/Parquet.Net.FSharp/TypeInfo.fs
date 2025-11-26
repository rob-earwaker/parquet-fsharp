namespace rec Parquet.FSharp
// Ignore warning about recursive field definitions.
#nowarn 40

open FSharp.Reflection
open System
open System.Collections
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

type AtomicInfo = {
    DotnetType: Type
    IsOptional: bool
    DataDotnetType: Type
    IsPrimitive: bool
    IsNullExpr: Expression -> Expression
    GetDataValueExpr: Expression -> Expression
    CreateFromDataValue: obj -> obj
    CreateNull: unit -> obj }

type ListInfo = {
    DotnetType: Type
    IsOptional: bool
    ElementInfo: ValueInfo
    IsNullExpr: Expression -> Expression
    GetLengthExpr: Expression -> Expression
    GetElementExpr: Expression * Expression -> Expression
    CreateFromElementValues: IList -> obj
    CreateNull: unit -> obj }

type FieldInfo = {
    Name: string
    ValueInfo: ValueInfo
    GetValueExpr: Expression -> Expression }

type RecordInfo = {
    DotnetType: Type
    ValueDotnetType: Type
    IsOptional: bool
    Fields: FieldInfo[]
    IsNullExpr: Expression -> Expression
    GetValueExpr: Expression -> Expression
    CreateFromFieldValues: obj[] -> obj
    CreateNull: unit -> obj }

[<AutoOpen>]
module ExpressionExtensions =
    type Expression with
        static member Null = Expression.Constant(null) :> Expression

        static member False = Expression.Constant(false) :> Expression
        static member True = Expression.Constant(true) :> Expression

        static member EqualsNull(value: Expression) =
            Expression.Equal(value, Expression.Null)
            :> Expression

        static member AndAlso([<ParamArray>] values: Expression[]) =
            values
            |> Array.reduce (fun combined value ->
                Expression.AndAlso(combined, value))

module private Nullable =
    let preComputeCreateValue (dotnetType: Type) =
        let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
        let constructor = dotnetType.GetConstructor([| valueDotnetType |])
        fun (valueObj: obj) -> constructor.Invoke([| valueObj |])

    let createNull = fun () -> null :> obj

    let isNullExpr (nullableValue: Expression) =
        Expression.Not(
            Expression.Property(nullableValue, "HasValue"))
        :> Expression

    let getValueExpr (nullableValue: Expression) =
        Expression.Property(nullableValue, "Value")
        :> Expression

module private Option =
    let private getSomeCase dotnetType =
        let unionCases = FSharpType.GetUnionCases(dotnetType)
        unionCases |> Array.find _.Name.Equals("Some")
        
    let private getNoneCase dotnetType =
        let unionCases = FSharpType.GetUnionCases(dotnetType)
        unionCases |> Array.find _.Name.Equals("None")

    let preComputeCreateSome dotnetType =
        let someCase = getSomeCase dotnetType
        let createSome = FSharpValue.PreComputeUnionConstructor(someCase)
        fun (valueObj: obj) -> createSome [| valueObj |]

    let preComputeCreateNone dotnetType =
        let noneCase = getNoneCase dotnetType
        let createNone = FSharpValue.PreComputeUnionConstructor(noneCase)
        fun () -> createNone [||]

    let isNoneExpr valueDotnetType =
        let optionModuleType =
            Assembly.Load("FSharp.Core").GetTypes()
            |> Array.filter (fun type' -> type'.Name = "OptionModule")
            |> Array.exactlyOne
        fun (valueOption: Expression) ->
            Expression.Call(optionModuleType, "IsNone", [| valueDotnetType |], valueOption)
            :> Expression

    let getValueExpr (valueOption: Expression) =
        Expression.Property(valueOption, "Value")
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

    let (|Array1d|_|) (dotnetType: Type) =
        if dotnetType.IsArray
            && dotnetType.GetArrayRank() = 1
        then Option.Some ()
        else Option.None

    let (|List|_|) (dotnetType: Type) =
        if dotnetType.IsGenericType
            && dotnetType.GetGenericTypeDefinition() = typedefof<list<_>>
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
                let name = "Value"
                let getValueExpr = Option.getValueExpr
                FieldInfo.create name valueInfo getValueExpr
            [| valueField |]
        let isNullExpr = Option.isNoneExpr valueInfo.DotnetType
        let getValueExpr = id
        let createFromFieldValues =
            let createSome = Option.preComputeCreateSome dotnetType
            fun (fieldValues: obj[]) ->
                createSome fieldValues[0]
        let createNull = Option.preComputeCreateNone dotnetType
        RecordInfo.create dotnetType valueDotnetType isOptional fields
            isNullExpr getValueExpr createFromFieldValues createNull
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
        GetTag: obj -> int
        GetTagExpr: Expression -> Expression
        GetCaseNameExpr: Expression -> Expression
        UnionCases: UnionCaseInfo[] }

    type private UnionCaseInfo = {
        UnionDotnetType: Type
        Tag: int
        TagExpr: Expression
        Name: string
        Fields: PropertyInfo[]
        GetFieldValues: obj -> obj[]
        CreateFromFieldValues: obj[] -> obj }

    module private UnionInfo =
        let ofUnion dotnetType =
            let unionCases =
                FSharpType.GetUnionCases(dotnetType)
                |> Array.map (fun unionCase ->
                    let getFieldValues = FSharpValue.PreComputeUnionReader(unionCase)
                    let createFromFieldValues = FSharpValue.PreComputeUnionConstructor(unionCase)
                    { UnionCaseInfo.UnionDotnetType = dotnetType
                      UnionCaseInfo.Tag = unionCase.Tag
                      UnionCaseInfo.TagExpr = Expression.Constant(unionCase.Tag)
                      UnionCaseInfo.Name = unionCase.Name
                      UnionCaseInfo.Fields = unionCase.GetFields()
                      UnionCaseInfo.GetFieldValues = getFieldValues
                      UnionCaseInfo.CreateFromFieldValues = createFromFieldValues })
            let getTag = FSharpValue.PreComputeUnionTagReader(dotnetType)
            let getTagExpr =
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
            let getCaseNameExpr (union: Expression) =
                let tag = Expression.Variable(typeof<int>, "tag")
                let throwInvalidTagExn =
                    Expression.Throw(
                        Expression.Constant(
                            exn($"union of type '{dotnetType.FullName}' has invalid tag value")))
                let returnLabel = Expression.Label(typeof<string>, "caseName")
                Expression.Block(
                    [ tag ],
                    Expression.Assign(tag, getTagExpr union),
                    unionCases
                    |> Array.rev
                    |> Array.fold
                        (fun (ifFalse: Expression) caseInfo ->
                            let test = Expression.Equal(tag, Expression.Constant(caseInfo.Tag))
                            let ifTrue = Expression.Return(returnLabel, Expression.Constant(caseInfo.Name))
                            Expression.IfThenElse(test, ifTrue, ifFalse))
                        throwInvalidTagExn,
                    Expression.Label(returnLabel, Expression.Constant(null, typeof<string>)))
                :> Expression
            { UnionInfo.DotnetType = dotnetType
              UnionInfo.GetTag = getTag
              UnionInfo.GetTagExpr = getTagExpr
              UnionInfo.GetCaseNameExpr = getCaseNameExpr
              UnionInfo.UnionCases = unionCases }

    let private ofUnionEnum (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.DotnetType
        // We represent unions that are equivalent to enums, i.e. all cases have
        // zero fields, as a string value containing the case name. Since a
        // union value can't be null and must be one of the possible cases, this
        // value is not optional despite being a string value.
        let isOptional = false
        let dataDotnetType = typeof<string>
        let isPrimitive = false
        let isNullExpr = fun (union: Expression) -> Expression.False
        let getDataValueExpr = unionInfo.GetCaseNameExpr
        let createFromDataValue (unionCaseNameObj: obj) =
            let unionCaseName = unionCaseNameObj :?> string
            unionInfo.UnionCases
            |> Array.pick (fun unionCase ->
                if unionCase.Name = unionCaseName
                then Option.Some (unionCase.CreateFromFieldValues [||])
                else Option.None)
        let createNull () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        AtomicInfo.create dotnetType isOptional dataDotnetType isPrimitive
            isNullExpr getDataValueExpr createFromDataValue createNull
        |> ValueInfo.Atomic

    let private ofUnionCaseData (unionInfo: UnionInfo) (unionCase: UnionCaseInfo) =
        let dotnetType = unionCase.UnionDotnetType
        let valueDotnetType = unionCase.UnionDotnetType
        // Each union case is treated as optional. Only one case from the union
        // can be set, so the others will be NULL.
        let isOptional = true
        let fields = unionCase.Fields |> Array.map FieldInfo.ofProperty
        let isNullExpr (union: Expression) =
            Expression.NotEqual(unionInfo.GetTagExpr union, unionCase.TagExpr)
            :> Expression
        let getValueExpr = id
        // The union object is created by the root union record, so just pass
        // the field values up to the parent union data record so they can be
        // passed through to the root.
        let createFromFieldValues (fieldValues: obj[]) =
            fieldValues :> obj
        let createNull = fun () -> null
        RecordInfo.create dotnetType valueDotnetType isOptional fields
            isNullExpr getValueExpr createFromFieldValues createNull
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
                let getValueExpr = id
                FieldInfo.create name valueInfo getValueExpr)
        let isNullExpr (union: Expression) =
            // TODO: This whole thing could almost certainly be made more elegant
            // and performant by implementing a UnionShredder and UnionAssembler
            // but this would require big changes!
            let tag = Expression.Variable(typeof<int>, "tag")
            Expression.Block(
                [ tag ],
                Expression.Assign(tag, unionInfo.GetTagExpr union),
                unionCasesWithFields
                |> Array.map (fun unionCase ->
                    Expression.NotEqual(tag, unionCase.TagExpr)
                    :> Expression)
                |> Expression.AndAlso)
            :> Expression
        let getValueExpr = id
        // Only one of the union cases should have fields available, so find the
        // field value that is NOTNULL. The union object is created by the root
        // union record so pass the field values up to the parent union data
        // record so they can be passed through to the root. There's no need to
        // pass the tag up since the root record stores the case name.
        let createFromFieldValues (fieldValues: obj[]) =
            fieldValues
            // TODO: Should technically use the 'IsNull' function from the
            // relevant field here instead of the built-in isNull function.
            |> Array.filter (fun fieldValue -> not (isNull fieldValue))
            |> Array.exactlyOne
        let createNull = fun () -> null
        RecordInfo.create dotnetType valueDotnetType isOptional fields
            isNullExpr getValueExpr createFromFieldValues createNull
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
                let name = "UnionCase"
                // TODO: This should really be a non-optional string like for the union enum
                let valueInfo = AtomicInfo.String |> ValueInfo.Atomic
                let getValueExpr = unionInfo.GetCaseNameExpr
                FieldInfo.create name valueInfo getValueExpr
            let dataField =
                let name = "Data"
                let valueInfo = ValueInfo.ofUnionData unionInfo
                let getValueExpr = id
                FieldInfo.create name valueInfo getValueExpr
            [| unionCaseField; dataField |]
        let isNullExpr = fun (union: Expression) -> Expression.False
        let getValueExpr = id
        let createFromFieldValues (fieldValues: obj[]) =
            let unionCaseName = fieldValues[0] :?> string
            unionInfo.UnionCases
            |> Array.pick (fun unionCase ->
                if unionCase.Name <> unionCaseName
                then Option.None
                else
                    let fieldValues =
                        if unionCase.Fields.Length = 0
                        then [||]
                        else fieldValues[1] :?> obj[]
                    Option.Some (unionCase.CreateFromFieldValues fieldValues))
        let createNull () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        RecordInfo.create dotnetType valueDotnetType isOptional fields
            isNullExpr getValueExpr createFromFieldValues createNull
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
                | DotnetType.List -> ValueInfo.List (ListInfo.ofList dotnetType)
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
        dotnetType isOptional dataDotnetType isPrimitive
        isNullExpr getDataValueExpr createFromDataValue createNull =
        { AtomicInfo.DotnetType = dotnetType
          AtomicInfo.IsOptional = isOptional
          AtomicInfo.DataDotnetType = dataDotnetType
          AtomicInfo.IsPrimitive = isPrimitive
          AtomicInfo.IsNullExpr = isNullExpr
          AtomicInfo.GetDataValueExpr = getDataValueExpr
          AtomicInfo.CreateFromDataValue = createFromDataValue
          AtomicInfo.CreateNull = createNull }

    let private ofPrimitive<'Value> =
        let dotnetType = typeof<'Value>
        let isOptional = false
        let dataDotnetType = dotnetType
        let isPrimitive = true
        let isNullExpr = fun (value: Expression) -> Expression.False
        let getDataValueExpr = id
        let createFromDataValue = id
        let createNull () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional dataDotnetType isPrimitive
            isNullExpr getDataValueExpr createFromDataValue createNull

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
        let isPrimitive = false
        let isNullExpr = fun (value: Expression) -> Expression.False
        let getDataValueExpr (value: Expression) =
            Expression.Property(value, "UtcDateTime")
            :> Expression
        let createFromDataValue (dataValueObj: obj) =
            let dateTime = dataValueObj :?> DateTime
            System.DateTimeOffset(dateTime.ToUniversalTime()) :> obj
        let createNull () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional dataDotnetType isPrimitive
            isNullExpr getDataValueExpr createFromDataValue createNull

    let String =
        let dotnetType = typeof<string>
        let isOptional = true
        let dataDotnetType = dotnetType
        let isPrimitive = true
        let isNullExpr = Expression.EqualsNull
        let getDataValueExpr = id
        let createFromDataValue = id
        let createNull = fun () -> null
        create dotnetType isOptional dataDotnetType isPrimitive
            isNullExpr getDataValueExpr createFromDataValue createNull

    let ofNullable (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let dataDotnetType = valueInfo.DataDotnetType
        let isPrimitive = false
        let isNullExpr = Nullable.isNullExpr
        let getDataValueExpr (nullableValue: Expression) =
            let value = Nullable.getValueExpr nullableValue
            valueInfo.GetDataValueExpr value
        let createFromDataValue =
            let createValue = Nullable.preComputeCreateValue dotnetType
            fun dataValueObj ->
                let valueObj = valueInfo.CreateFromDataValue dataValueObj
                createValue valueObj
        let createNull = Nullable.createNull
        create dotnetType isOptional dataDotnetType isPrimitive
            isNullExpr getDataValueExpr createFromDataValue createNull

    let ofOptionInner (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let dataDotnetType = valueInfo.DataDotnetType
        let isPrimitive = false
        let isNullExpr = Option.isNoneExpr valueInfo.DotnetType
        let getDataValueExpr (valueOption: Expression) =
            let value = Option.getValueExpr valueOption
            valueInfo.GetDataValueExpr value
        let createFromDataValue =
            let createSome = Option.preComputeCreateSome dotnetType
            fun dataValueObj ->
                let valueObj = valueInfo.CreateFromDataValue dataValueObj
                createSome valueObj
        let createNull = Option.preComputeCreateNone dotnetType
        create dotnetType isOptional dataDotnetType isPrimitive
            isNullExpr getDataValueExpr createFromDataValue createNull

module private ListInfo =
    let private create
        dotnetType isOptional elementInfo
        isNullExpr getLengthExpr getElementExpr
        createFromElementValues createNull =
        { ListInfo.DotnetType = dotnetType
          ListInfo.IsOptional = isOptional
          ListInfo.ElementInfo = elementInfo
          ListInfo.IsNullExpr = isNullExpr
          ListInfo.GetLengthExpr = getLengthExpr
          ListInfo.GetElementExpr = getElementExpr
          ListInfo.CreateFromElementValues = createFromElementValues
          ListInfo.CreateNull = createNull }

    let ofArray1d (dotnetType: Type) =
        let isOptional = true
        let elementDotnetType = dotnetType.GetElementType()
        let elementInfo = ValueInfo.ofType elementDotnetType
        let isNullExpr = Expression.EqualsNull
        let getLengthExpr (array: Expression) =
            Expression.Property(array, "Length")
            :> Expression
        let getElementExpr (array: Expression, index: Expression) =
            Expression.ArrayIndex(array, [ index ])
            :> Expression
        let createFromElementValues (elementValues: IList) =
            let array = Array.CreateInstance(elementDotnetType, elementValues.Count)
            elementValues.CopyTo(array, 0)
            array :> obj
        let createNull = fun () -> null
        create dotnetType isOptional elementInfo
            isNullExpr getLengthExpr getElementExpr
            createFromElementValues createNull

    let ofList (dotnetType: Type) =
        // TODO: F# lists are not nullable by default, however Parquet.Net
        // does not support list fields that aren't nullable and a nullable
        // list field class can't easily be created since some of the relevant
        // properties are internal. For now, treat all lists as optional, even
        // though in practice they will never be null.
        let isOptional = true
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementInfo = ValueInfo.ofType elementDotnetType
        let isNullExpr = fun (list: Expression) -> Expression.False
        let getLengthExpr (list: Expression) =
            Expression.Property(list, "Length")
            :> Expression
        let getElementExpr =
            let itemProperty = dotnetType.GetProperty("Item")
            fun (list: Expression, index: Expression) ->
                Expression.MakeIndex(list, itemProperty, [ index ])
                :> Expression
        let createFromElementValues =
            let Empty = dotnetType.GetProperty("Empty").GetValue(null)
            let cons = dotnetType.GetMethod("Cons")
            fun (elementValues: IList) ->
                let mutable list = Empty
                for index in [ elementValues.Count - 1 .. -1 .. 0 ] do
                    list <- cons.Invoke(null, [| elementValues[index]; list |])
                list
        let createNull () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional elementInfo
            isNullExpr getLengthExpr getElementExpr
            createFromElementValues createNull

    let ofNullable (dotnetType: Type) (listInfo: ListInfo) =
        let isOptional = true
        let elementInfo = listInfo.ElementInfo
        let isNullExpr = Nullable.isNullExpr
        let getLengthExpr (nullableList: Expression) =
            let list = Nullable.getValueExpr nullableList
            listInfo.GetLengthExpr list
        let getElementExpr (nullableList: Expression, index: Expression) =
            let list = Nullable.getValueExpr nullableList
            listInfo.GetElementExpr(list, index)
        let createFromElementValues =
            let createValue = Nullable.preComputeCreateValue dotnetType
            fun elementValues ->
                let listObj = listInfo.CreateFromElementValues elementValues
                createValue listObj
        let createNull = Nullable.createNull
        create dotnetType isOptional elementInfo
            isNullExpr getLengthExpr getElementExpr
            createFromElementValues createNull

    let ofOptionInner (dotnetType: Type) (listInfo: ListInfo) =
        let isOptional = true
        let elementInfo = listInfo.ElementInfo
        let isNullExpr = Option.isNoneExpr listInfo.DotnetType
        let getLengthExpr (listOption: Expression) =
            let list = Option.getValueExpr listOption
            listInfo.GetLengthExpr list
        let getElementExpr (listOption: Expression, index: Expression) =
            let list = Option.getValueExpr listOption
            listInfo.GetElementExpr(list, index)
        let createFromElementValues =
            let createSome = Option.preComputeCreateSome dotnetType
            fun elementValues ->
                let listObj = listInfo.CreateFromElementValues elementValues
                createSome listObj
        let createNull = Option.preComputeCreateNone dotnetType
        create dotnetType isOptional elementInfo
            isNullExpr getLengthExpr getElementExpr
            createFromElementValues createNull

module private FieldInfo =
    let create name valueInfo getValueExpr =
        { FieldInfo.Name = name
          FieldInfo.ValueInfo = valueInfo
          FieldInfo.GetValueExpr = getValueExpr }

    let ofProperty (field: PropertyInfo) =
        let name = field.Name
        let valueInfo = ValueInfo.ofType field.PropertyType
        let getValueExpr (record: Expression) =
            Expression.Property(record, field)
            :> Expression
        create name valueInfo getValueExpr

module private RecordInfo =
    let create
        dotnetType valueDotnetType isOptional fields
        isNullExpr getValueExpr createFromFieldValues createNull =
        { RecordInfo.DotnetType = dotnetType
          RecordInfo.ValueDotnetType = valueDotnetType
          RecordInfo.IsOptional = isOptional
          RecordInfo.Fields = fields
          RecordInfo.IsNullExpr = isNullExpr
          RecordInfo.GetValueExpr = getValueExpr
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
        let isNullExpr = fun (record: Expression) -> Expression.False
        let getValueExpr = id
        let createFromFieldValues = FSharpValue.PreComputeRecordConstructor(dotnetType)
        let createNull () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType valueDotnetType isOptional fields
            isNullExpr getValueExpr createFromFieldValues createNull

    let ofNullable dotnetType (recordInfo: RecordInfo) =
        let valueDotnetType = recordInfo.DotnetType
        let isOptional = true
        let fields = recordInfo.Fields
        let isNullExpr = Nullable.isNullExpr
        let getValueExpr = Nullable.getValueExpr
        let createFromFieldValues =
            let createValue = Nullable.preComputeCreateValue dotnetType
            fun (fieldValues: obj[]) ->
                let record = recordInfo.CreateFromFieldValues fieldValues
                createValue record
        let createNull = Nullable.createNull
        create dotnetType valueDotnetType isOptional fields
            isNullExpr getValueExpr createFromFieldValues createNull

    let ofOptionInner dotnetType (recordInfo: RecordInfo) =
        let valueDotnetType = recordInfo.DotnetType
        let isOptional = true
        let fields = recordInfo.Fields
        let isNullExpr = Option.isNoneExpr valueDotnetType
        let getValueExpr = Option.getValueExpr
        let createFromFieldValues =
            let createSome = Option.preComputeCreateSome dotnetType
            fun (fieldValues: obj[]) ->
                let record = recordInfo.CreateFromFieldValues fieldValues
                createSome record
        let createNull = Option.preComputeCreateNone dotnetType
        create dotnetType valueDotnetType isOptional fields
            isNullExpr getValueExpr createFromFieldValues createNull
