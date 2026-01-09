namespace rec Parquet.FSharp
#nowarn 40

open FSharp.Reflection
open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Reflection

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

// TODO: Attribute to select specific converter type to use? Alternatively could
// be part of the serializer configuration?

// TODO: Eventually should be able to rename to remove 'Factory'
type internal IValueConverterFactory =
    abstract member TryCreateSerializer
        : dotnetType:Type -> ValueConverter option
    abstract member TryCreateDeserializer
        : dotnetType:Type * schema:ValueSchema -> ValueConverter option

type internal ValueConverter =
    | Atomic of AtomicConverter
    | List of ListConverter
    | Record of RecordConverter
    | Optional of OptionalConverter
    with
    member this.DotnetType =
        match this with
        | ValueConverter.Atomic atomicConverter -> atomicConverter.DotnetType
        | ValueConverter.List listConverter -> listConverter.DotnetType
        | ValueConverter.Record recordConverter -> recordConverter.DotnetType
        | ValueConverter.Optional optionalConverter -> optionalConverter.DotnetType

type internal AtomicConverter = {
    DotnetType: Type
    DataDotnetType: Type
    GetDataValue: Expression -> Expression
    CreateFromDataValue: Expression -> Expression }

type internal ListConverter = {
    DotnetType: Type
    ElementConverter: ValueConverter
    GetLength: Expression -> Expression
    GetElementValue: Expression * Expression -> Expression
    CreateEmpty: Expression
    CreateFromElementValues: Expression -> Expression }

type internal FieldConverter = {
    Name: string
    ValueConverter: ValueConverter
    GetValue: Expression -> Expression }

type internal RecordConverter = {
    DotnetType: Type
    Fields: FieldConverter[]
    CreateFromFieldValues: Expression[] -> Expression }

type internal OptionalConverter = {
    DotnetType: Type
    ValueConverter: ValueConverter
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
    let isGenericType<'GenericType> (dotnetType: Type) =
        dotnetType.IsGenericType
        && dotnetType.GetGenericTypeDefinition() = typedefof<'GenericType>

module internal ValueConverter =
    let private Cache = Dictionary<Type, ValueConverter>()

    let private tryGetCached dotnetType =
        lock Cache (fun () ->
            match Cache.TryGetValue(dotnetType) with
            | false, _ -> Option.None
            | true, valueConverter -> Option.Some valueConverter)

    let private addToCache dotnetType valueConverter =
        lock Cache (fun () ->
            Cache[dotnetType] <- valueConverter)

    let createAtomic
        dotnetType dataDotnetType getDataValue createFromDataValue =
        ValueConverter.Atomic {
            AtomicConverter.DotnetType = dotnetType
            AtomicConverter.DataDotnetType = dataDotnetType
            AtomicConverter.GetDataValue = getDataValue
            AtomicConverter.CreateFromDataValue = createFromDataValue }

    let createRecord dotnetType fields createFromFieldValues =
        ValueConverter.Record {
            RecordConverter.DotnetType = dotnetType
            RecordConverter.Fields = fields
            RecordConverter.CreateFromFieldValues = createFromFieldValues }

    let createList
        dotnetType elementConverter
        getLength getElementValue createEmpty createFromElementValues =
        ValueConverter.List {
            ListConverter.DotnetType = dotnetType
            ListConverter.ElementConverter = elementConverter
            ListConverter.GetLength = getLength
            ListConverter.GetElementValue = getElementValue
            ListConverter.CreateEmpty = createEmpty
            ListConverter.CreateFromElementValues = createFromElementValues }

    let createOptional
        dotnetType valueConverter isNull getValue createNull createFromValue =
        ValueConverter.Optional {
            OptionalConverter.DotnetType = dotnetType
            OptionalConverter.ValueConverter = valueConverter
            OptionalConverter.IsNull = isNull
            OptionalConverter.GetValue = getValue
            OptionalConverter.CreateNull = createNull
            OptionalConverter.CreateFromValue = createFromValue }

    let forReferenceTypeConverter (valueConverter: ValueConverter) =
        let dotnetType = valueConverter.DotnetType
        let isNull = Expression.IsNull
        let getValue = id
        let createNull = Expression.Null(dotnetType)
        let createFromValue = id
        ValueConverter.createOptional
            dotnetType valueConverter isNull getValue createNull createFromValue

    // TODO: This shouldn't really be necessary, but while Parquet.Net
    // treats all struct and list fields as optional it's necessary.
    let forNonNullableReferenceTypeConverter (valueConverter: ValueConverter) =
        let dotnetType = valueConverter.DotnetType
        let isNull = fun value -> Expression.False
        let getValue = id
        let createNull =
            Expression.Block(
                Expression.FailWith($"type '{dotnetType.FullName}' is not nullable"),
                Expression.Default(dotnetType))
            :> Expression
        let createFromValue = id
        ValueConverter.createOptional
            dotnetType valueConverter isNull getValue createNull createFromValue

    let private getSchema' isOptional valueConverter =
        match valueConverter with
        | ValueConverter.Atomic atomicConverter ->
            ValueSchema.Atomic {
                IsOptional = isOptional
                DotnetType = atomicConverter.DataDotnetType }
        | ValueConverter.List listConverter ->
            ValueSchema.List {
                IsOptional = isOptional
                Element = ValueConverter.getSchema listConverter.ElementConverter }
        | ValueConverter.Record recordConverter ->
            ValueSchema.Record {
                IsOptional = isOptional
                Fields = recordConverter.Fields |> Array.map FieldConverter.getSchema }
        | ValueConverter.Optional optionalConverter ->
            ValueConverter.getSchema' true optionalConverter.ValueConverter

    let getSchema valueConverter =
        ValueConverter.getSchema' false valueConverter

    let ofType (dotnetType: Type) : ValueConverter =
        match tryGetCached dotnetType with
        | Option.Some valueConverter -> valueConverter
        | Option.None ->
            let valueConverter =
                ValueConverterFactory.All
                |> Array.tryPick _.TryCreateSerializer(dotnetType)
                |> Option.defaultWith (fun () ->
                    failwith $"unsupported type '{dotnetType.FullName}'")
            addToCache dotnetType valueConverter
            valueConverter

    let of'<'Value> =
        ofType typeof<'Value>

module private FieldConverter =
    let create name valueConverter getValue =
        { FieldConverter.Name = name
          FieldConverter.ValueConverter = valueConverter
          FieldConverter.GetValue = getValue }

    let ofProperty (field: PropertyInfo) : FieldConverter =
        let name = field.Name
        let valueConverter = ValueConverter.ofType field.PropertyType
        let getValue (record: Expression) =
            Expression.Property(record, field)
            :> Expression
        create name valueConverter getValue

    let ofUnionCaseField (unionCase: UnionCaseInfo) (field: PropertyInfo) =
        let name = field.Name
        let valueConverter = ValueConverter.ofType field.PropertyType
        let getValue (union: Expression) =
            Expression.Property(
                Expression.Convert(union, unionCase.DotnetType), field)
            :> Expression
        create name valueConverter getValue

    let getSchema (fieldConverter: FieldConverter) =
        { FieldSchema.Name = fieldConverter.Name
          FieldSchema.Value = ValueConverter.getSchema fieldConverter.ValueConverter }

module internal RecordConverter =
    let getRootSchema (recordConverter: RecordConverter) =
        { Schema.Fields =
            recordConverter.Fields
            |> Array.map FieldConverter.getSchema }

module internal ValueConverterFactory =
    let All : IValueConverterFactory[] = [|
        PrimitiveConverterFactory<bool>()
        PrimitiveConverterFactory<int8>()
        PrimitiveConverterFactory<int16>()
        PrimitiveConverterFactory<int>()
        PrimitiveConverterFactory<int64>()
        PrimitiveConverterFactory<uint8>()
        PrimitiveConverterFactory<uint16>()
        PrimitiveConverterFactory<uint32>()
        PrimitiveConverterFactory<uint64>()
        PrimitiveConverterFactory<float32>()
        PrimitiveConverterFactory<float>()
        PrimitiveConverterFactory<decimal>()
        PrimitiveConverterFactory<DateTime>()
        PrimitiveConverterFactory<Guid>()
        DateTimeOffsetConverterFactory()
        StringConverterFactory()
        // This must come before the generic array type since byte arrays are
        // supported as a primitive type in Parquet and are therefore handled as
        // atomic values rather than lists.
        ByteArrayConverterFactory()
        Array1dConverterFactory()
        GenericListConverterFactory()
        FSharpListConverterFactory()
        FSharpRecordConverterFactory()
        NullableConverterFactory()
        // This must come before the generic union type since option types are
        // handled in a special way.
        OptionConverterFactory()
        UnionConverterFactory()
        ClassConverterFactory()
    |]

type internal PrimitiveConverterFactory<'Value>() =
    let valueConverter =
        let dotnetType = typeof<'Value>
        let dataDotnetType = dotnetType
        let getDataValue = id
        let createFromDataValue = id
        ValueConverter.createAtomic
            dotnetType dataDotnetType getDataValue createFromDataValue

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if dotnetType = valueConverter.DotnetType
            then Option.Some valueConverter
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if dotnetType = valueConverter.DotnetType
            then Option.Some valueConverter
            else Option.None

type internal DateTimeOffsetConverterFactory() =
    let valueConverter =
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
        ValueConverter.createAtomic
            dotnetType dataDotnetType getDataValue createFromDataValue

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if dotnetType = valueConverter.DotnetType
            then Option.Some valueConverter
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if dotnetType = valueConverter.DotnetType
            then Option.Some valueConverter
            else Option.None

type internal StringConverterFactory() =
    let valueConverter =
        let dotnetType = typeof<string>
        let dataDotnetType = dotnetType
        let getDataValue = id
        let createFromDataValue = id
        ValueConverter.createAtomic
            dotnetType dataDotnetType getDataValue createFromDataValue
        |> ValueConverter.forReferenceTypeConverter

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if dotnetType = valueConverter.DotnetType
            then Option.Some valueConverter
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if dotnetType = valueConverter.DotnetType
            then Option.Some valueConverter
            else Option.None

type internal ByteArrayConverterFactory() =
    let valueConverter =
        let dotnetType = typeof<byte[]>
        let dataDotnetType = dotnetType
        let getDataValue = id
        let createFromDataValue = id
        ValueConverter.createAtomic
            dotnetType dataDotnetType getDataValue createFromDataValue
        |> ValueConverter.forReferenceTypeConverter

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if dotnetType = valueConverter.DotnetType
            then Option.Some valueConverter
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if dotnetType = valueConverter.DotnetType
            then Option.Some valueConverter
            else Option.None

type internal Array1dConverterFactory() =
    let isArray1dType (dotnetType: Type) =
        dotnetType.IsArray
        && dotnetType.GetArrayRank() = 1

    let createValueConverter (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetElementType()
        let elementConverter = ValueConverter.ofType elementDotnetType
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
        ValueConverter.createList
            dotnetType elementConverter
            getLength getElementValue createEmpty createFromElementValues
        |> ValueConverter.forReferenceTypeConverter

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isArray1dType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if isArray1dType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None

type internal GenericListConverterFactory() =
    let isGenericListType = DotnetType.isGenericType<ResizeArray<_>>

    let createValueConverter (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementConverter = ValueConverter.ofType elementDotnetType
        let getLength (list: Expression) =
            Expression.Property(list, "Count")
            :> Expression
        let getElementValue (list: Expression, index: Expression) =
            Expression.MakeIndex(list, dotnetType.GetProperty("Item"), [ index ])
            :> Expression
        let createEmpty = Expression.New(dotnetType)
        let createFromElementValues = id
        ValueConverter.createList
            dotnetType elementConverter
            getLength getElementValue createEmpty createFromElementValues
        |> ValueConverter.forReferenceTypeConverter

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isGenericListType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if isGenericListType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None

type internal FSharpListConverterFactory() =
    let isFSharpListType = DotnetType.isGenericType<list<_>>

    let createValueConverter (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementConverter = ValueConverter.ofType elementDotnetType
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
        ValueConverter.createList
            dotnetType elementConverter
            getLength getElementValue createEmpty createFromElementValues
        |> ValueConverter.forNonNullableReferenceTypeConverter

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isFSharpListType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if isFSharpListType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None

type internal FSharpRecordConverterFactory() =
    let isFSharpRecordType = FSharpType.IsRecord

    let createValueConverter (dotnetType: Type) =
        let fields =
            FSharpType.GetRecordFields(dotnetType)
            |> Array.map FieldConverter.ofProperty
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
        ValueConverter.createRecord dotnetType fields createFromFieldValues
        |> ValueConverter.forNonNullableReferenceTypeConverter

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isFSharpRecordType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if isFSharpRecordType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None

type internal UnionConverterFactory() =
    let isUnionType = FSharpType.IsUnion

    let ofUnionSimple (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.DotnetType
        // Unions in which all cases have no fields can be represented as a
        // simple string value containing the case name. Since a union value
        // can't be null and must be one of the possible cases, this value is
        // not optional.
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
        ValueConverter.createAtomic
            dotnetType dataDotnetType getDataValue createFromDataValue

    let ofUnionCase (unionInfo: UnionInfo) (unionCase: UnionCaseInfo) =
        // Union case data is represented as an optional record containing the
        // field values for that case. The record needs to be optional since
        // only one case from the union can be set and the others will be NULL.
        let dotnetType = unionInfo.DotnetType
        let valueConverter =
            let dotnetType = unionInfo.DotnetType
            let fields = unionCase.Fields |> Array.map (FieldConverter.ofUnionCaseField unionCase)
            let createFromFieldValues = unionCase.CreateFromFieldValues
            ValueConverter.createRecord dotnetType fields createFromFieldValues
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
        ValueConverter.createOptional
            dotnetType valueConverter isNull getValue createNull createFromValue

    let ofUnionComplex (unionInfo: UnionInfo) =
        // Unions that have one or more cases with one or more fields can not be
        // represented as a simple string value. Instead, we have to model the
        // union as a record with a field to capture the case name and
        // additional fields to hold any associated case data.
        let dotnetType = unionInfo.DotnetType
        let unionCasesWithFields =
            unionInfo.UnionCases
            |> Array.filter (fun unionCase -> unionCase.Fields.Length > 0)
        // The 'Type' field holds the case name. Since unions are not nullable
        // there must always be a case name present. We therefore model this
        // as a non-optional string value.
        let typeField =
            // TODO: The name of this field could be configurable via an attribute.
            let name = "Type"
            let valueConverter =
                let dotnetType = typeof<string>
                let dataDotnetType = dotnetType
                let getDataValue = id
                let createFromDataValue = id
                ValueConverter.createAtomic
                    dotnetType dataDotnetType getDataValue createFromDataValue
            let getValue = unionInfo.GetCaseName
            FieldConverter.create name valueConverter getValue
        // Each union case with one or more fields is assigned an additional
        // field within the record to hold its associated data. The name of this
        // field matches the case name and the value is a record that contains
        // the case's field values.
        let caseFields =
            unionCasesWithFields
            |> Array.map (fun unionCase ->
                let name = unionCase.Name
                // Note that there's a chance the case name is the same as the
                // field name chosen to store the union case name, in which case
                // we'd have two fields with the same name. We could add a level
                // of nesting to the object structure to avoid this potential
                // name conflict, but this adds extra complexity.
                if name = typeField.Name then
                    failwith <|
                        $"case name '{typeField.Name}' is not supported"
                        + $" for union type '{dotnetType.FullName}'"
                let valueConverter = ofUnionCase unionInfo unionCase
                let getValue = id
                FieldConverter.create name valueConverter getValue)
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
        ValueConverter.createRecord dotnetType fields createFromFieldValues
        |> ValueConverter.forNonNullableReferenceTypeConverter

    let createValueConverter dotnetType =
        // Unions are represented in one of two ways depending on whether any of
        // the cases have associated data fields.
        let unionInfo = UnionInfo.ofUnion dotnetType
        if unionInfo.UnionCases
            |> Array.forall (fun unionCase -> unionCase.Fields.Length = 0)
        then ofUnionSimple unionInfo
        else ofUnionComplex unionInfo

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isUnionType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if isUnionType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None

type internal NullableConverterFactory() =
    let isNullableType = DotnetType.isGenericType<Nullable<_>>

    let ofNullableRequired dotnetType (valueConverter: ValueConverter) =
        let isNull (nullable: Expression) =
            Expression.Not(Expression.Property(nullable, "HasValue"))
            :> Expression
        let getValue (nullable: Expression) =
            Expression.Property(nullable, "Value")
            :> Expression
        let createNull = Expression.Null(dotnetType)
        let createFromValue =
            let constructor = dotnetType.GetConstructor([| valueConverter.DotnetType |])
            fun (value: Expression) ->
                Expression.New(constructor, value)
                :> Expression
        ValueConverter.createOptional
            dotnetType valueConverter isNull getValue createNull createFromValue

    let ofNullableOptional (dotnetType: Type) (valueConverter: ValueConverter) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                let getValue = id
                FieldConverter.create name valueConverter getValue
            [| valueField |]
        let createFromFieldValues (fieldValues: Expression[]) =
            fieldValues[0]
        ValueConverter.createRecord valueConverter.DotnetType fields createFromFieldValues
        |> ofNullableRequired dotnetType

    let createValueConverter (dotnetType: Type) =
        let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
        let valueConverter = ValueConverter.ofType valueDotnetType
        if valueConverter.IsOptional
        // TODO: This shouldn't really be possible as nullable values have to be
        // value types and therefore can't be null, but while we need to treat
        // all record types as optional we need to handle this case.
        then ofNullableOptional dotnetType valueConverter
        else ofNullableRequired dotnetType valueConverter

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isNullableType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if isNullableType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None

type internal OptionConverterFactory() =
    let isOptionType = DotnetType.isGenericType<option<_>>

    let ofOptionRequired dotnetType (valueConverter: ValueConverter) =
        let unionCases = FSharpType.GetUnionCases(dotnetType)
        let isNull =
            let optionModuleType =
                Assembly.Load("FSharp.Core").GetTypes()
                |> Array.filter (fun type' -> type'.Name = "OptionModule")
                |> Array.exactlyOne
            fun (option: Expression) ->
                Expression.Call(optionModuleType, "IsNone", [| valueConverter.DotnetType |], option)
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
        ValueConverter.createOptional
            dotnetType valueConverter isNull getValue createNull createFromValue

    let ofOptionOptional (dotnetType: Type) (valueConverter: ValueConverter) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                let getValue = id
                FieldConverter.create name valueConverter getValue
            [| valueField |]
        let createFromFieldValues (fieldValues: Expression[]) =
            fieldValues[0]
        ValueConverter.createRecord valueConverter.DotnetType fields createFromFieldValues
        |> ofOptionRequired dotnetType

    let createValueConverter (dotnetType: Type) =
        let valueDotnetType = dotnetType.GetGenericArguments()[0]
        let valueConverter = ValueConverter.ofType valueDotnetType
        if valueConverter.IsOptional
        then ofOptionOptional dotnetType valueConverter
        else ofOptionRequired dotnetType valueConverter

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isOptionType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if isOptionType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None

type internal ClassConverterFactory() =
    let isClassType (dotnetType: Type) =
        dotnetType.IsClass

    let createValueConverter (dotnetType: Type) =
        let defaultConstructor = dotnetType.GetConstructor([||])
        if isNull defaultConstructor then
            failwith $"type '{dotnetType.FullName}' does not have a default constructor"
        let fields =
            dotnetType.GetProperties(BindingFlags.Instance ||| BindingFlags.Public)
            |> Array.map FieldConverter.ofProperty
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
        ValueConverter.createRecord dotnetType fields createFromFieldValues
        |> ValueConverter.forReferenceTypeConverter

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isClassType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if isClassType dotnetType
            then Option.Some (createValueConverter dotnetType)
            else Option.None
