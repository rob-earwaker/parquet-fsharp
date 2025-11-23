namespace rec Parquet.FSharp

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
    IsNull: obj -> bool
    BuildIsNullExpr: Expression -> Expression
    GetDataValue: obj -> obj
    BuildGetDataValueExpr: Expression -> Expression
    CreateFromDataValue: obj -> obj
    CreateNull: unit -> obj }

type ListInfo = {
    DotnetType: Type
    IsOptional: bool
    ElementInfo: ValueInfo
    IsNull: obj -> bool
    GetElementValues: obj -> IList
    CreateFromElementValues: IList -> obj
    CreateNull: unit -> obj }

type FieldInfo = {
    Name: string
    ValueInfo: ValueInfo }

type RecordInfo = {
    DotnetType: Type
    IsOptional: bool
    Fields: FieldInfo[]
    IsNull: obj -> bool
    GetFieldValues: obj -> obj[]
    CreateFromFieldValues: obj[] -> obj
    CreateNull: unit -> obj }

module private Nullable =
    let preComputeGetValue (dotnetType: Type) =
        let getValueMethod = dotnetType.GetProperty("Value").GetGetMethod()
        fun (nullableObj: obj) -> getValueMethod.Invoke(nullableObj, [||])

    let preComputeCreateValue (dotnetType: Type) =
        let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
        let constructor = dotnetType.GetConstructor([| valueDotnetType |])
        fun (valueObj: obj) -> constructor.Invoke([| valueObj |])

    let isNull' = isNull

    let createNull = fun () -> null :> obj

module private Option =
    let private getSomeCase dotnetType =
        let unionCases = FSharpType.GetUnionCases(dotnetType)
        unionCases |> Array.find _.Name.Equals("Some")
        
    let private getNoneCase dotnetType =
        let unionCases = FSharpType.GetUnionCases(dotnetType)
        unionCases |> Array.find _.Name.Equals("None")

    let preComputeIsNone dotnetType =
        let noneCase = getNoneCase dotnetType
        let getOptionTag = FSharpValue.PreComputeUnionTagReader(dotnetType)
        fun (optionObj: obj) -> getOptionTag optionObj = noneCase.Tag

    let preComputeGetValue dotnetType =
        let someCase = getSomeCase dotnetType
        let getSomeFields = FSharpValue.PreComputeUnionReader(someCase)
        fun (optionObj: obj) -> Array.head (getSomeFields optionObj)

    let preComputeCreateSome dotnetType =
        let someCase = getSomeCase dotnetType
        let createSome = FSharpValue.PreComputeUnionConstructor(someCase)
        fun (valueObj: obj) -> createSome [| valueObj |]

    let preComputeCreateNone dotnetType =
        let noneCase = getNoneCase dotnetType
        let createNone = FSharpValue.PreComputeUnionConstructor(noneCase)
        fun () -> createNone [||]

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

    let private ofOption (dotnetType: Type) =
        let valueDotnetType = dotnetType.GetGenericArguments()[0]
        match ValueInfo.ofType valueDotnetType with
        | ValueInfo.Atomic atomicInfo ->
            let atomicOptionInfo = AtomicInfo.ofOption dotnetType atomicInfo
            ValueInfo.Atomic atomicOptionInfo
        | ValueInfo.List listInfo ->
            let listOptionInfo = ListInfo.ofOption dotnetType listInfo
            ValueInfo.List listOptionInfo
        | ValueInfo.Record recordInfo ->
            let recordOptionInfo = RecordInfo.ofOption dotnetType recordInfo
            ValueInfo.Record recordOptionInfo

    type private UnionInfo = {
        DotnetType: Type
        GetTag: obj -> int
        BuildGetTagExpr: Expression -> Expression
        UnionCases: UnionCaseInfo[] }

    type private UnionCaseInfo = {
        UnionDotnetType: Type
        Tag: int
        Name: string
        Fields: PropertyInfo[]
        GetFieldValues: obj -> obj[]
        CreateFromFieldValues: obj[] -> obj }

    module private UnionInfo =
        let ofUnion dotnetType =
            let getTag = FSharpValue.PreComputeUnionTagReader(dotnetType)
            let buildGetTagExpr =
                match FSharpValue.PreComputeUnionTagMemberInfo(dotnetType) with
                | :? MethodInfo as methodInfo ->
                    fun (union: Expression) ->
                        Expression.Call(union, methodInfo)
                        :> Expression
                | :? PropertyInfo as propertyInfo ->
                    fun (union: Expression) ->
                        Expression.Property(union, propertyInfo)
                        :> Expression
                | memberInfo ->
                    failwith $"unsupported tag member info type '{memberInfo.GetType().FullName}'"
            let unionCases =
                FSharpType.GetUnionCases(dotnetType)
                |> Array.map (fun unionCase ->
                    let getFieldValues = FSharpValue.PreComputeUnionReader(unionCase)
                    let createFromFieldValues = FSharpValue.PreComputeUnionConstructor(unionCase)
                    { UnionCaseInfo.UnionDotnetType = dotnetType
                      UnionCaseInfo.Tag = unionCase.Tag
                      UnionCaseInfo.Name = unionCase.Name
                      UnionCaseInfo.Fields = unionCase.GetFields()
                      UnionCaseInfo.GetFieldValues = getFieldValues
                      UnionCaseInfo.CreateFromFieldValues = createFromFieldValues })
            { UnionInfo.DotnetType = dotnetType
              UnionInfo.GetTag = getTag
              UnionInfo.BuildGetTagExpr = buildGetTagExpr
              UnionInfo.UnionCases = unionCases }

    let private ofUnionEnum (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.DotnetType
        // We represent unions that are equivalent to enums, i.e. all cases have
        // zero fields, as a string value containing the case name. Since a
        // union value can't be null and must be one of the possible cases, this
        // value is not optional despite being a string value.
        let isOptional = false
        let dataDotnetType = typeof<string>
        let isNull = fun _ -> false
        let buildIsNullExpr (union: Expression) =
            Expression.Constant(false) :> Expression
        let getDataValue (unionObj: obj) =
            let tag = unionInfo.GetTag unionObj
            unionInfo.UnionCases
            |> Array.pick (fun unionCase ->
                if unionCase.Tag = tag
                then Option.Some (unionCase.Name :> obj)
                else Option.None)
        let buildGetDataValueExpr (union: Expression) =
            let tag = Expression.Variable(typeof<int>, "tag")
            let throwInvalidTagExn =
                Expression.Throw(
                    Expression.Constant(
                        exn($"union of type '{dotnetType.FullName}' has invalid tag value")))
            let returnLabel = Expression.Label(typeof<string>, "caseName")
            Expression.Block(
                [ tag ],
                Expression.Assign(tag, unionInfo.BuildGetTagExpr union),
                unionInfo.UnionCases
                |> Array.rev
                |> Array.fold
                    (fun (ifFalse: Expression) caseInfo ->
                        let test = Expression.Equal(tag, Expression.Constant(caseInfo.Tag))
                        let ifTrue = Expression.Return(returnLabel, Expression.Constant(caseInfo.Name))
                        Expression.IfThenElse(test, ifTrue, ifFalse))
                    throwInvalidTagExn,
                Expression.Label(returnLabel, Expression.Constant(null, typeof<string>)))
            :> Expression
        let createFromDataValue (unionCaseNameObj: obj) =
            let unionCaseName = unionCaseNameObj :?> string
            unionInfo.UnionCases
            |> Array.pick (fun unionCase ->
                if unionCase.Name = unionCaseName
                then Option.Some (unionCase.CreateFromFieldValues [||])
                else Option.None)
        let createNull () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        AtomicInfo.create dotnetType isOptional dataDotnetType
            isNull buildIsNullExpr getDataValue buildGetDataValueExpr createFromDataValue createNull
        |> ValueInfo.Atomic

    let private ofUnionCaseData (unionCase: UnionCaseInfo) =
        let dotnetType = unionCase.UnionDotnetType
        // Each union case is treated as optional. Only one case from the union
        // can be set, so the others will be NULL.
        let isOptional = true
        let fields = unionCase.Fields |> Array.map FieldInfo.ofProperty
        let isNull = isNull
        // Field values for this union case are extracted in the root union
        // record when the union is decomposed and are passed down through the
        // union data record, so all we need to do is cast the object.
        let getFieldValues (fieldValuesObj: obj) =
            fieldValuesObj :?> obj[]
        // The union object is created by the root union record, so just pass
        // the field values up to the parent union data record so they can be
        // passed through to the root.
        let createFromFieldValues (fieldValues: obj[]) =
            fieldValues :> obj
        let createNull = fun () -> null
        RecordInfo.create dotnetType isOptional fields
            isNull getFieldValues createFromFieldValues createNull
        |> ValueInfo.Record

    let private ofUnionData (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.DotnetType
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
                let valueInfo = ValueInfo.ofUnionCaseData unionCase
                FieldInfo.create name valueInfo)
        let isNull' = isNull
        // The field values are already extracted by the root union record and
        // are passed through along with the tag. We need to ensure we only pass
        // the field values through to the correct union case based on the tag.
        let getFieldValues (valueObj: obj) =
            let tag, fieldValues = valueObj :?> (int * obj[])
            unionCasesWithFields
            |> Array.map (fun unionCase ->
                if unionCase.Tag = tag
                then fieldValues :> obj
                else null)
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
        RecordInfo.create dotnetType isOptional fields
            isNull' getFieldValues createFromFieldValues createNull
        |> ValueInfo.Record

    let private ofUnionComplex (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.DotnetType
        // TODO: F# unions are not nullable by default, however we are mapping
        // to a struct and Parquet.Net does not support struct fields that
        // aren't nullable.
        let isOptional = true
        let fields =
            let unionCaseField =
                let name = "UnionCase"
                let valueInfo = AtomicInfo.String |> ValueInfo.Atomic
                FieldInfo.create name valueInfo
            let dataField =
                let name = "Data"
                let valueInfo = ValueInfo.ofUnionData unionInfo
                FieldInfo.create name valueInfo
            [| unionCaseField; dataField |]
        let isNull' = fun _ -> false
        let getFieldValues (unionObj: obj) =
            let tag = unionInfo.GetTag unionObj
            unionInfo.UnionCases
            |> Array.pick (fun unionCase ->
                if unionCase.Tag <> tag
                then Option.None
                else
                    let nameFieldValue = unionCase.Name :> obj
                    let dataFieldValue =
                        if unionCase.Fields.Length = 0
                        // TODO: Should technically use the 'CreateNull' function
                        // from the relevant field here instead of the null literal.
                        then null
                        else (tag, unionCase.GetFieldValues unionObj) :> obj
                    Option.Some [| nameFieldValue; dataFieldValue |])
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
        RecordInfo.create dotnetType isOptional fields
            isNull' getFieldValues createFromFieldValues createNull
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
                | DotnetType.Union -> ValueInfo.ofUnion dotnetType
                | DotnetType.Nullable -> ValueInfo.ofNullable dotnetType
                // TODO: Implement special handling for option types. Currently
                // they are treated like any other union type.
                | DotnetType.Option -> ValueInfo.ofOption dotnetType
                | _ -> failwith $"unsupported type '{dotnetType.FullName}'"
            addToCache dotnetType valueInfo
            valueInfo

    let of'<'Value> =
        ofType typeof<'Value>

module private AtomicInfo =
    let create
        dotnetType isOptional dataDotnetType
        isNull buildIsNullExpr getDataValue buildGetDataValueExpr createFromDataValue createNull =
        { AtomicInfo.DotnetType = dotnetType
          AtomicInfo.IsOptional = isOptional
          AtomicInfo.DataDotnetType = dataDotnetType
          AtomicInfo.IsNull = isNull
          AtomicInfo.BuildIsNullExpr = buildIsNullExpr
          AtomicInfo.GetDataValue = getDataValue
          AtomicInfo.BuildGetDataValueExpr = buildGetDataValueExpr
          AtomicInfo.CreateFromDataValue = createFromDataValue
          AtomicInfo.CreateNull = createNull }

    let private ofDataType<'Value> =
        let dotnetType = typeof<'Value>
        let isOptional = false
        let dataDotnetType = dotnetType
        let isNull = fun _ -> false
        let buildIsNullExpr (value: Expression) =
            Expression.Constant(false) :> Expression
        let getDataValue = id
        let buildGetDataValueExpr = id
        let createFromDataValue = id
        let createNull () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional dataDotnetType
            isNull buildIsNullExpr getDataValue buildGetDataValueExpr createFromDataValue createNull

    let Bool = ofDataType<bool>
    let Int8 = ofDataType<int8>
    let Int16 = ofDataType<int16>
    let Int32 = ofDataType<int>
    let Int64 = ofDataType<int64>
    let UInt8 = ofDataType<uint8>
    let UInt16 = ofDataType<uint16>
    let UInt32 = ofDataType<uint32>
    let UInt64 = ofDataType<uint64>
    let Float32 = ofDataType<float32>
    let Float64 = ofDataType<float>
    let Decimal = ofDataType<decimal>
    let DateTime = ofDataType<DateTime>
    let Guid = ofDataType<Guid>

    let DateTimeOffset =
        let dotnetType = typeof<DateTimeOffset>
        let isOptional = false
        let dataDotnetType = typeof<DateTime>
        let isNull = fun _ -> false
        let buildIsNullExpr (value: Expression) =
            Expression.Constant(false) :> Expression
        let getDataValue (valueObj: obj) =
            let dateTimeOffset = valueObj :?> DateTimeOffset
            dateTimeOffset.UtcDateTime :> obj
        let buildGetDataValueExpr (value: Expression) =
            Expression.Property(value, "UtcDateTime")
            :> Expression
        let createFromDataValue (dataValueObj: obj) =
            let dateTime = dataValueObj :?> DateTime
            System.DateTimeOffset(dateTime.ToUniversalTime()) :> obj
        let createNull () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional dataDotnetType
            isNull buildIsNullExpr getDataValue buildGetDataValueExpr createFromDataValue createNull

    let String =
        let dotnetType = typeof<string>
        let isOptional = true
        let dataDotnetType = dotnetType
        let isNull = isNull
        let buildIsNullExpr (value: Expression) =
            Expression.Equal(value, Expression.Constant(null)) :> Expression
        let getDataValue = id
        let buildGetDataValueExpr = id
        let createFromDataValue = id
        let createNull = fun () -> null
        create dotnetType isOptional dataDotnetType
            isNull buildIsNullExpr getDataValue buildGetDataValueExpr createFromDataValue createNull

    let ofNullable (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let dataDotnetType = valueInfo.DataDotnetType
        let isNull = Nullable.isNull'
        let buildIsNullExpr (nullableValue: Expression) =
            Expression.Not(
                Expression.Property(nullableValue, "HasValue"))
            :> Expression
        let getDataValue =
            let getValue = Nullable.preComputeGetValue dotnetType
            fun nullableValueObj ->
                let valueObj = getValue nullableValueObj
                valueInfo.GetDataValue valueObj
        let buildGetDataValueExpr (nullableValue: Expression) =
            let value = Expression.Property(nullableValue, "Value")
            valueInfo.BuildGetDataValueExpr value
        let createFromDataValue =
            let createValue = Nullable.preComputeCreateValue dotnetType
            fun dataValueObj ->
                let valueObj = valueInfo.CreateFromDataValue dataValueObj
                createValue valueObj
        let createNull = Nullable.createNull
        create dotnetType isOptional dataDotnetType
            isNull buildIsNullExpr getDataValue buildGetDataValueExpr createFromDataValue createNull

    let ofOption (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let dataDotnetType = valueInfo.DataDotnetType
        let isNull = Option.preComputeIsNone dotnetType
        let buildIsNullExpr (valueOption: Expression) =
            Expression.Property(valueOption, "IsNone")
            :> Expression
        let getDataValue =
            let getValue = Option.preComputeGetValue dotnetType
            fun optionValueObj ->
                let valueObj = getValue optionValueObj
                valueInfo.GetDataValue valueObj
        let buildGetDataValueExpr (valueOption: Expression) =
            let value = Expression.Property(valueOption, "Value")
            valueInfo.BuildGetDataValueExpr value
        let createFromDataValue =
            let createSome = Option.preComputeCreateSome dotnetType
            fun dataValueObj ->
                let valueObj = valueInfo.CreateFromDataValue dataValueObj
                createSome valueObj
        let createNull = Option.preComputeCreateNone dotnetType
        create dotnetType isOptional dataDotnetType
            isNull buildIsNullExpr getDataValue buildGetDataValueExpr createFromDataValue createNull

module private ListInfo =
    let private create
        dotnetType isOptional elementInfo
        isNull getElementValues createFromElementValues createNull =
        { ListInfo.DotnetType = dotnetType
          ListInfo.IsOptional = isOptional
          ListInfo.ElementInfo = elementInfo
          ListInfo.IsNull = isNull
          ListInfo.GetElementValues = getElementValues
          ListInfo.CreateFromElementValues = createFromElementValues
          ListInfo.CreateNull = createNull }

    let ofArray1d (dotnetType: Type) =
        let isOptional = true
        let elementDotnetType = dotnetType.GetElementType()
        let elementInfo = ValueInfo.ofType elementDotnetType
        let isNull = isNull
        let getElementValues (arrayObj: obj) =
            arrayObj :?> IList
        let createFromElementValues (elementValues: IList) =
            let array = Array.CreateInstance(elementDotnetType, elementValues.Count)
            elementValues.CopyTo(array, 0)
            array :> obj
        let createNull = fun () -> null
        create dotnetType isOptional elementInfo
            isNull getElementValues createFromElementValues createNull

    let ofList (dotnetType: Type) =
        // TODO: F# lists are not nullable by default, however Parquet.Net
        // does not support list fields that aren't nullable and a nullable
        // list field class can't easily be created since some of the relevant
        // properties are internal. For now, treat all lists as optional, even
        // though in practice they will never be null.
        let isOptional = true
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementInfo = ValueInfo.ofType elementDotnetType
        let isNull = fun _ -> false
        let getElementValues (listObj: obj) =
            let arrayList = ArrayList()
            for element in listObj :?> IEnumerable do
                arrayList.Add(element) |> ignore
            arrayList :> IList
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
            isNull getElementValues createFromElementValues createNull

    let ofNullable (dotnetType: Type) (listInfo: ListInfo) =
        let isOptional = true
        let elementInfo = listInfo.ElementInfo
        let isNull = Nullable.isNull'
        let getElementValues =
            let getValue = Nullable.preComputeGetValue dotnetType
            fun nullableObj ->
                let valueObj = getValue nullableObj
                listInfo.GetElementValues valueObj
        let createFromElementValues =
            let createValue = Nullable.preComputeCreateValue dotnetType
            fun elementValues ->
                let listObj = listInfo.CreateFromElementValues elementValues
                createValue listObj
        let createNull = Nullable.createNull
        create dotnetType isOptional elementInfo
            isNull getElementValues createFromElementValues createNull

    let ofOption (dotnetType: Type) (listInfo: ListInfo) =
        let isOptional = true
        let elementInfo = listInfo.ElementInfo
        let isNull = Option.preComputeIsNone dotnetType
        let getElementValues =
            let getValue = Option.preComputeGetValue dotnetType
            fun optionObj ->
                let valueObj = getValue optionObj
                listInfo.GetElementValues valueObj
        let createFromElementValues =
            let createSome = Option.preComputeCreateSome dotnetType
            fun elementValues ->
                let listObj = listInfo.CreateFromElementValues elementValues
                createSome listObj
        let createNull = Option.preComputeCreateNone dotnetType
        create dotnetType isOptional elementInfo
            isNull getElementValues createFromElementValues createNull

module private FieldInfo =
    let create name valueInfo =
        { FieldInfo.Name = name
          FieldInfo.ValueInfo = valueInfo }

    let ofProperty (field: PropertyInfo) =
        let name = field.Name
        let valueInfo = ValueInfo.ofType field.PropertyType
        create name valueInfo

module private RecordInfo =
    let create
        dotnetType isOptional fields
        isNull getFieldValues createFromFieldValues createNull =
        { RecordInfo.DotnetType = dotnetType
          RecordInfo.IsOptional = isOptional
          RecordInfo.Fields = fields
          RecordInfo.IsNull = isNull
          RecordInfo.GetFieldValues = getFieldValues
          RecordInfo.CreateFromFieldValues = createFromFieldValues
          RecordInfo.CreateNull = createNull }

    let ofRecord dotnetType =
        // TODO: F# records are not nullable by default, however Parquet.Net
        // does not support struct fields that aren't nullable and a nullable
        // struct field class can't easily be created since some of the relevant
        // properties are internal. For now, treat all records as optional, even
        // though in practice they will never be null.
        let isOptional = true
        let fields =
            FSharpType.GetRecordFields(dotnetType)
            |> Array.map FieldInfo.ofProperty
        let isNull = fun _ -> false
        let getFieldValues = FSharpValue.PreComputeRecordReader(dotnetType)
        let createFromFieldValues = FSharpValue.PreComputeRecordConstructor(dotnetType)
        let createNull () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional fields
            isNull getFieldValues createFromFieldValues createNull

    let ofNullable dotnetType (recordInfo: RecordInfo) =
        let isOptional = true
        let fields = recordInfo.Fields
        let isNull = Nullable.isNull'
        let getFieldValues =
            let getValue = Nullable.preComputeGetValue dotnetType
            fun nullableObj ->
                let valueObj = getValue nullableObj
                recordInfo.GetFieldValues valueObj
        let createFromFieldValues =
            let createValue = Nullable.preComputeCreateValue dotnetType
            fun (fieldValues: obj[]) ->
                let record = recordInfo.CreateFromFieldValues fieldValues
                createValue record
        let createNull = Nullable.createNull
        create dotnetType isOptional fields
            isNull getFieldValues createFromFieldValues createNull

    let ofOption dotnetType (recordInfo: RecordInfo) =
        let isOptional = true
        let fields = recordInfo.Fields
        let isNull = Option.preComputeIsNone dotnetType
        let getFieldValues =
            let getValue = Option.preComputeGetValue dotnetType
            fun optionObj ->
                let valueObj = getValue optionObj
                recordInfo.GetFieldValues valueObj
        let createFromFieldValues =
            let createSome = Option.preComputeCreateSome dotnetType
            fun (fieldValues: obj[]) ->
                let record = recordInfo.CreateFromFieldValues fieldValues
                createSome record
        let createNull = Option.preComputeCreateNone dotnetType
        create dotnetType isOptional fields
            isNull getFieldValues createFromFieldValues createNull
