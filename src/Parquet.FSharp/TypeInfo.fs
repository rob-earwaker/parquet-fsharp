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
    | Optional of OptionalInfo
    with
    member this.DotnetType =
        match this with
        | ValueInfo.Atomic atomicInfo -> atomicInfo.DotnetType
        | ValueInfo.List listInfo -> listInfo.DotnetType
        | ValueInfo.Record recordInfo -> recordInfo.DotnetType
        | ValueInfo.Optional optionalInfo -> optionalInfo.DotnetType

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
//     - byte[]

//   Not implemented:
//     - BigInteger
//     - DateOnly, TimeOnly
//     - TimeSpan, Interval
//     - Enums?

type AtomicInfo = {
    DotnetType: Type
    DataDotnetType: Type
    GetDataValue: Expression -> Expression
    CreateFromDataValue: Expression -> Expression }

type ListInfo = {
    DotnetType: Type
    ElementInfo: ValueInfo
    GetLength: Expression -> Expression
    GetElementValue: Expression * Expression -> Expression
    CreateEmpty: Expression
    CreateFromElementValues: Expression -> Expression }

type FieldInfo = {
    Name: string
    ValueInfo: ValueInfo
    GetValue: Expression -> Expression }

type RecordInfo = {
    DotnetType: Type
    Fields: FieldInfo[]
    CreateFromFieldValues: Expression[] -> Expression }

type OptionalInfo = {
    DotnetType: Type
    ValueInfo: ValueInfo
    IsNull: Expression -> Expression
    GetValue: Expression -> Expression
    CreateNull: Expression
    CreateFromValue: Expression -> Expression }

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
            match FSharpValue.PreComputeUnionTagMemberInfo(dotnetType) with
            | :? MethodInfo as methodInfo ->
                fun (union: Expression) ->
                    if methodInfo.IsStatic
                    then Expression.Call(methodInfo, union) :> Expression
                    else Expression.Call(union, methodInfo) :> Expression
            | :? PropertyInfo as propertyInfo ->
                fun (union: Expression) ->
                    Expression.Property(union, propertyInfo) :> Expression
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
                        $"union of type '{dotnetType.FullName}' has invalid tag value")
                    yield Expression.Label(returnLabel, Expression.Null(returnLabel.Type))
                })
            :> Expression
        { UnionInfo.DotnetType = dotnetType
          UnionInfo.GetTag = getTag
          UnionInfo.GetCaseName = getCaseName
          UnionInfo.UnionCases = unionCases }

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
    let (|ByteArray|_|) = ActivePatternTypeMatch<byte[]>
    
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

    let (|Record|_|) dotnetType =
        if FSharpType.IsRecord(dotnetType)
        then Option.Some ()
        else Option.None

    let (|Union|_|) dotnetType =
        if FSharpType.IsUnion(dotnetType)
        then Option.Some ()
        else Option.None

    let (|Class|_|) (dotnetType: Type) =
        if dotnetType.IsClass
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

    let private atomicInfo
        dotnetType dataDotnetType getDataValue createFromDataValue =
        ValueInfo.Atomic {
            AtomicInfo.DotnetType = dotnetType
            AtomicInfo.DataDotnetType = dataDotnetType
            AtomicInfo.GetDataValue = getDataValue
            AtomicInfo.CreateFromDataValue = createFromDataValue }

    let private recordInfo dotnetType fields createFromFieldValues =
        ValueInfo.Record {
            RecordInfo.DotnetType = dotnetType
            RecordInfo.Fields = fields
            RecordInfo.CreateFromFieldValues = createFromFieldValues }

    let private listInfo
        dotnetType elementInfo
        getLength getElementValue createEmpty createFromElementValues =
        ValueInfo.List {
            ListInfo.DotnetType = dotnetType
            ListInfo.ElementInfo = elementInfo
            ListInfo.GetLength = getLength
            ListInfo.GetElementValue = getElementValue
            ListInfo.CreateEmpty = createEmpty
            ListInfo.CreateFromElementValues = createFromElementValues }

    let private optionalInfo
        dotnetType valueInfo isNull getValue createNull createFromValue =
        ValueInfo.Optional {
            OptionalInfo.DotnetType = dotnetType
            OptionalInfo.ValueInfo = valueInfo
            OptionalInfo.IsNull = isNull
            OptionalInfo.GetValue = getValue
            OptionalInfo.CreateNull = createNull
            OptionalInfo.CreateFromValue = createFromValue }

    let private ofReferenceType (valueInfo: ValueInfo) =
        let dotnetType = valueInfo.DotnetType
        let isNull = Expression.IsNull
        let getValue = id
        let createNull = Expression.Null(dotnetType)
        let createFromValue = id
        ValueInfo.optionalInfo
            dotnetType valueInfo isNull getValue createNull createFromValue

    // TODO: This shouldn't really be necessary, but while Parquet.Net
    // treats all struct and list fields as optional it's necessary.
    let private ofNonNullableReferenceType (valueInfo: ValueInfo) =
        let dotnetType = valueInfo.DotnetType
        let isNull = fun value -> Expression.False
        let getValue = id
        let createNull =
            Expression.Block(
                Expression.FailWith($"type '{dotnetType.FullName}' is not nullable"),
                Expression.Default(dotnetType))
            :> Expression
        let createFromValue = id
        ValueInfo.optionalInfo
            dotnetType valueInfo isNull getValue createNull createFromValue

    let private ofPrimitive<'Value> =
        let dotnetType = typeof<'Value>
        let dataDotnetType = dotnetType
        let getDataValue = id
        let createFromDataValue = id
        ValueInfo.atomicInfo
            dotnetType dataDotnetType getDataValue createFromDataValue

    let private Bool = ofPrimitive<bool>
    let private Int8 = ofPrimitive<int8>
    let private Int16 = ofPrimitive<int16>
    let private Int32 = ofPrimitive<int>
    let private Int64 = ofPrimitive<int64>
    let private UInt8 = ofPrimitive<uint8>
    let private UInt16 = ofPrimitive<uint16>
    let private UInt32 = ofPrimitive<uint32>
    let private UInt64 = ofPrimitive<uint64>
    let private Float32 = ofPrimitive<float32>
    let private Float64 = ofPrimitive<float>
    let private Decimal = ofPrimitive<decimal>
    let private DateTime = ofPrimitive<DateTime>
    let private Guid = ofPrimitive<Guid>

    let private DateTimeOffset =
        let dotnetType = typeof<DateTimeOffset>
        let dataDotnetType = typeof<DateTime>
        let getDataValue (value: Expression) =
            Expression.Property(value, "UtcDateTime")
            :> Expression
        let createFromDataValue (dataValue: Expression) =
            Expression.New(
                typeof<DateTimeOffset>.GetConstructor([| typeof<DateTime> |]),
                Expression.Call(dataValue, "ToUniversalTime", []))
            :> Expression
        ValueInfo.atomicInfo
            dotnetType dataDotnetType getDataValue createFromDataValue

    let private String =
        let dotnetType = typeof<string>
        let dataDotnetType = dotnetType
        let getDataValue = id
        let createFromDataValue = id
        ValueInfo.atomicInfo
            dotnetType dataDotnetType getDataValue createFromDataValue
        |> ValueInfo.ofReferenceType

    let private ByteArray =
        let dotnetType = typeof<byte[]>
        let dataDotnetType = dotnetType
        let getDataValue = id
        let createFromDataValue = id
        ValueInfo.atomicInfo
            dotnetType dataDotnetType getDataValue createFromDataValue
        |> ValueInfo.ofReferenceType

    let private ofRecord dotnetType =
        let fields =
            FSharpType.GetRecordFields(dotnetType)
            |> Array.map FieldInfo.ofProperty
        let createFromFieldValues =
            let constructor = FSharpValue.PreComputeRecordConstructorInfo(dotnetType)
            fun (fieldValues: Expression[]) ->
                Expression.New(constructor, fieldValues)
                :> Expression
        // TODO: F# records are not nullable by default, however Parquet.Net
        // does not support struct fields that aren't nullable and a nullable
        // struct field class can't easily be created since some of the relevant
        // properties are internal. For now, wrap as a non-nullable reference
        // type.
        ValueInfo.recordInfo dotnetType fields createFromFieldValues
        |> ValueInfo.ofNonNullableReferenceType

    let private ofClass (dotnetType: Type) =
        let defaultConstructor = dotnetType.GetConstructor([||])
        if isNull defaultConstructor then
            failwith $"type '{dotnetType.FullName}' does not have a default constructor"
        let fields =
            dotnetType.GetProperties(BindingFlags.Instance ||| BindingFlags.Public)
            |> Array.map FieldInfo.ofProperty
        let createFromFieldValues =
            fun (fieldValues: Expression[]) ->
                let record = Expression.Variable(dotnetType, "record")
                Expression.Block(
                    [ record ],
                    seq<Expression> {
                        yield Expression.Assign(record, Expression.New(defaultConstructor))
                        yield! Array.zip fields fieldValues
                            |> Array.map (fun (field, fieldValue) ->
                                Expression.Assign(
                                    Expression.Property(record, field.Name),
                                    fieldValue)
                                :> Expression)
                        yield record
                    })
                :> Expression
        ValueInfo.recordInfo dotnetType fields createFromFieldValues
        |> ValueInfo.ofReferenceType

    let private ofArray1d (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetElementType()
        let elementInfo = ValueInfo.ofType elementDotnetType
        let getLength (array: Expression) =
            Expression.Property(array, "Length")
            :> Expression
        let getElementValue (array: Expression, index: Expression) =
            Expression.ArrayIndex(array, [ index ])
            :> Expression
        let createEmpty =
            Expression.NewArrayBounds(elementDotnetType, Expression.Constant(0))
        let createFromElementValues (elementValues: Expression) =
            Expression.Call(elementValues, "ToArray", [])
        ValueInfo.listInfo
            dotnetType elementInfo
            getLength getElementValue createEmpty createFromElementValues
        |> ValueInfo.ofReferenceType

    let private ofGenericList (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementInfo = ValueInfo.ofType elementDotnetType
        let getLength (list: Expression) =
            Expression.Property(list, "Count")
            :> Expression
        let getElementValue (list: Expression, index: Expression) =
            Expression.MakeIndex(list, dotnetType.GetProperty("Item"), [ index ])
            :> Expression
        let createEmpty = Expression.New(dotnetType)
        let createFromElementValues = id
        ValueInfo.listInfo
            dotnetType elementInfo
            getLength getElementValue createEmpty createFromElementValues
        |> ValueInfo.ofReferenceType

    let private ofFSharpList (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementInfo = ValueInfo.ofType elementDotnetType
        let getLength (list: Expression) =
            Expression.Property(list, "Length")
            :> Expression
        let getElementValue =
            let itemProperty = dotnetType.GetProperty("Item")
            fun (list: Expression, index: Expression) ->
                Expression.MakeIndex(list, itemProperty, [ index ])
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
        // TODO: F# lists are not nullable by default, however Parquet.Net
        // does not support list fields that aren't nullable and a nullable
        // list field class can't easily be created since some of the relevant
        // properties are internal. For now, wrap as a non-nullable reference
        // type.
        ValueInfo.listInfo
            dotnetType elementInfo
            getLength getElementValue createEmpty createFromElementValues
        |> ValueInfo.ofNonNullableReferenceType

    let private ofNullableRequired dotnetType (valueInfo: ValueInfo) =
        let isNull (nullable: Expression) =
            Expression.Not(Expression.Property(nullable, "HasValue"))
            :> Expression
        let getValue (nullable: Expression) =
            Expression.Property(nullable, "Value")
            :> Expression
        let createNull = Expression.Null(dotnetType)
        let createFromValue =
            let constructor = dotnetType.GetConstructor([| valueInfo.DotnetType |])
            fun (value: Expression) ->
                Expression.New(constructor, value)
                :> Expression
        ValueInfo.optionalInfo
            dotnetType valueInfo isNull getValue createNull createFromValue

    let private ofNullableOptional (dotnetType: Type) (valueInfo: ValueInfo) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                let getValue = id
                FieldInfo.create name valueInfo getValue
            [| valueField |]
        let createFromFieldValues (fieldValues: Expression[]) =
            fieldValues[0]
        ValueInfo.recordInfo valueInfo.DotnetType fields createFromFieldValues
        |> ValueInfo.ofNullableRequired dotnetType

    let private ofNullable (dotnetType: Type) =
        let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
        let valueInfo = ValueInfo.ofType valueDotnetType
        if valueInfo.IsOptional
        // TODO: This shouldn't really be possible as nullable values have to be
        // value types and therefore can't be null, but while we need to treat
        // all record types as optional we need to handle this case.
        then ValueInfo.ofNullableOptional dotnetType valueInfo
        else ValueInfo.ofNullableRequired dotnetType valueInfo

    let private ofOptionRequired dotnetType (valueInfo: ValueInfo) =
        let unionCases = FSharpType.GetUnionCases(dotnetType)
        let isNull =
            let optionModuleType =
                Assembly.Load("FSharp.Core").GetTypes()
                |> Array.filter (fun type' -> type'.Name = "OptionModule")
                |> Array.exactlyOne
            fun (option: Expression) ->
                Expression.Call(optionModuleType, "IsNone", [| valueInfo.DotnetType |], option)
                :> Expression
        let getValue (option: Expression) =
            Expression.Property(option, "Value")
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
        ValueInfo.optionalInfo
            dotnetType valueInfo isNull getValue createNull createFromValue

    let private ofOptionOptional (dotnetType: Type) (valueInfo: ValueInfo) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                let getValue = id
                FieldInfo.create name valueInfo getValue
            [| valueField |]
        let createFromFieldValues (fieldValues: Expression[]) =
            fieldValues[0]
        ValueInfo.recordInfo valueInfo.DotnetType fields createFromFieldValues
        |> ValueInfo.ofOptionRequired dotnetType

    let private ofOption (dotnetType: Type) =
        let valueDotnetType = dotnetType.GetGenericArguments()[0]
        let valueInfo = ValueInfo.ofType valueDotnetType
        if valueInfo.IsOptional
        then ValueInfo.ofOptionOptional dotnetType valueInfo
        else ValueInfo.ofOptionRequired dotnetType valueInfo

    let private ofUnionEnum (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.DotnetType
        // We represent unions that are equivalent to enums, i.e. all cases have
        // zero fields, as a string value containing the case name. Since a
        // union value can't be null and must be one of the possible cases, this
        // value is not optional despite being represented as a string value.
        let dataDotnetType = typeof<string>
        let getDataValue = unionInfo.GetCaseName
        let createFromDataValue (caseName: Expression) =
            let returnLabel = Expression.Label(dotnetType, "union")
            Expression.Block(
                seq<Expression> {
                    yield! unionInfo.UnionCases
                        |> Array.map (fun caseInfo ->
                            Expression.IfThen(
                                Expression.Equal(caseName, Expression.Constant(caseInfo.Name)),
                                Expression.Return(returnLabel, caseInfo.CreateFromFieldValues [||]))
                            :> Expression)
                    yield Expression.FailWith(
                        $"union of type '{dotnetType.FullName}' has invalid name value")
                    yield Expression.Label(returnLabel, Expression.Default(dotnetType))
                })
            :> Expression
        ValueInfo.atomicInfo
            dotnetType dataDotnetType getDataValue createFromDataValue

    let private ofUnionCase (unionInfo: UnionInfo) (unionCase: UnionCaseInfo) =
        // Union case data is represented as an optional record containing the
        // field values for that case. The record needs to be optional since
        // only one case from the union can be set and the others will be NULL.
        let dotnetType = unionInfo.DotnetType
        let valueInfo =
            let dotnetType = unionInfo.DotnetType
            let fields = unionCase.Fields |> Array.map (FieldInfo.ofUnionCaseField unionCase)
            let createFromFieldValues = unionCase.CreateFromFieldValues
            ValueInfo.recordInfo dotnetType fields createFromFieldValues
        // The data for this case is NULL if the union tag does not match the
        // tag for this case.
        let isNull (union: Expression) =
            Expression.NotEqual(unionInfo.GetTag union, unionCase.Tag)
            :> Expression
        let getValue = id
        // We can't use {Expression.Null} here because union types are not
        // nullable, however they do still have {null} as their default value
        // because they are reference types.
        let createNull = Expression.Default(dotnetType) :> Expression
        let createFromValue = id
        ValueInfo.optionalInfo
            dotnetType valueInfo isNull getValue createNull createFromValue

    let private ofUnionComplex (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.DotnetType
        let unionCasesWithFields =
            unionInfo.UnionCases
            |> Array.filter (fun unionCase -> unionCase.Fields.Length > 0)
        let typeField =
            // TODO: The name of this field could be configurable via an attribute.
            let name = "Type"
            // TODO: This should really be a non-optional string like for the union enum
            let valueInfo = ValueInfo.String
            let getValue = unionInfo.GetCaseName
            FieldInfo.create name valueInfo getValue
        let caseFields =
            unionCasesWithFields
            |> Array.map (fun unionCase ->
                let name = unionCase.Name
                if name = typeField.Name then
                    failwith <|
                        $"case name '{typeField.Name}' is not supported"
                        + $" for union type '{dotnetType.FullName}'"
                let valueInfo = ValueInfo.ofUnionCase unionInfo unionCase
                let getValue = id
                FieldInfo.create name valueInfo getValue)
        let fields = Array.append [| typeField |] caseFields
        let createFromFieldValues (fieldValues: Expression[]) =
            let caseName = Expression.Variable(typeof<string>, "caseName")
            let returnLabel = Expression.Label(dotnetType, "union")
            Expression.Block(
                [ caseName ],
                seq<Expression> {
                    yield Expression.Assign(caseName, fieldValues[0])
                    for caseInfo in unionInfo.UnionCases do
                        yield Expression.IfThen(
                            Expression.Equal(caseName, Expression.Constant(caseInfo.Name)),
                            if caseInfo.Fields.Length = 0
                            then
                                Expression.Return(returnLabel, caseInfo.CreateFromFieldValues [||])
                                :> Expression
                            else
                                let caseIndex =
                                    caseFields
                                    |> Array.findIndex (fun field -> field.Name = caseInfo.Name)
                                let fieldValue = fieldValues[caseIndex + 1]
                                Expression.IfThenElse(
                                    Expression.IsNull(Expression.Convert(fieldValue, typeof<obj>)),
                                    Expression.FailWith(
                                        $"no field values found for case '{caseInfo.Name}'"
                                        + " of union type '{dotnetType.FullName}'"),
                                    Expression.Return(returnLabel, fieldValue)))
                    yield Expression.FailWith(
                        $"unknown case name for union of type '{dotnetType.FullName}'")
                    yield Expression.Label(returnLabel, Expression.Default(returnLabel.Type))
                })
            :> Expression
        // TODO: F# unions are not nullable by default, however we are mapping
        // to a struct and Parquet.Net does not support struct fields that
        // aren't nullable, so wrap as a non-nullable reference type.
        ValueInfo.recordInfo dotnetType fields createFromFieldValues
        |> ValueInfo.ofNonNullableReferenceType

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

    let ofType (dotnetType: Type) : ValueInfo =
        match tryGetCached dotnetType with
        | Option.Some valueInfo -> valueInfo
        | Option.None ->
            let valueInfo =
                match dotnetType with
                | DotnetType.Bool -> ValueInfo.Bool
                | DotnetType.Int8 -> ValueInfo.Int8
                | DotnetType.Int16 -> ValueInfo.Int16
                | DotnetType.Int32 -> ValueInfo.Int32
                | DotnetType.Int64 -> ValueInfo.Int64
                | DotnetType.UInt8 -> ValueInfo.UInt8
                | DotnetType.UInt16 -> ValueInfo.UInt16
                | DotnetType.UInt32 -> ValueInfo.UInt32
                | DotnetType.UInt64 -> ValueInfo.UInt64
                | DotnetType.Float32 -> ValueInfo.Float32
                | DotnetType.Float64 -> ValueInfo.Float64
                | DotnetType.Decimal -> ValueInfo.Decimal
                | DotnetType.DateTime -> ValueInfo.DateTime
                | DotnetType.DateTimeOffset -> ValueInfo.DateTimeOffset
                | DotnetType.String -> ValueInfo.String
                | DotnetType.Guid -> ValueInfo.Guid
                // This must come before the generic array type since byte
                // arrays are supported as a primitive type in Parquet and are
                // therefore handled as atomic values rather than lists.
                | DotnetType.ByteArray -> ValueInfo.ByteArray
                | DotnetType.Array1d -> ValueInfo.ofArray1d dotnetType
                | DotnetType.GenericList -> ValueInfo.ofGenericList dotnetType
                | DotnetType.FSharpList -> ValueInfo.ofFSharpList dotnetType
                | DotnetType.Record -> ValueInfo.ofRecord dotnetType
                | DotnetType.Nullable -> ValueInfo.ofNullable dotnetType
                | DotnetType.Option -> ValueInfo.ofOption dotnetType
                // This must come after the option type since option types are
                // handled in a special way.
                | DotnetType.Union -> ValueInfo.ofUnion dotnetType
                | DotnetType.Class -> ValueInfo.ofClass dotnetType
                | _ -> failwith $"unsupported type '{dotnetType.FullName}'"
            addToCache dotnetType valueInfo
            valueInfo

    let of'<'Value> =
        ofType typeof<'Value>

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

    let ofUnionCaseField (unionCase: UnionCaseInfo) (field: PropertyInfo) =
        let name = field.Name
        let valueInfo = ValueInfo.ofType field.PropertyType
        let getValue (union: Expression) =
            Expression.Property(
                Expression.Convert(union, unionCase.DotnetType), field)
            :> Expression
        create name valueInfo getValue
