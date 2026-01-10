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

// TODO: Attribute to select specific serializer type to use? Alternatively could
// be part of the serializer configuration?

// TODO: Eventually should be able to rename to remove 'Factory'
type internal IValueConverterFactory =
    abstract member TryCreateSerializer
        : dotnetType:Type -> ValueSerializer option
    abstract member TryCreateDeserializer
        : dotnetType:Type * schema:ValueSchema -> ValueDeserializer option

type internal ValueSerializer =
    | Atomic of AtomicSerializer
    | List of ListSerializer
    | Record of RecordSerializer
    | Optional of OptionalSerializer
    with
    member this.DotnetType =
        match this with
        | ValueSerializer.Atomic atomicSerializer -> atomicSerializer.DotnetType
        | ValueSerializer.List listSerializer -> listSerializer.DotnetType
        | ValueSerializer.Record recordSerializer -> recordSerializer.DotnetType
        | ValueSerializer.Optional optionalSerializer -> optionalSerializer.DotnetType

type internal AtomicSerializer = {
    DotnetType: Type
    DataDotnetType: Type
    GetDataValue: Expression -> Expression }

type internal ListSerializer = {
    DotnetType: Type
    ElementSerializer: ValueSerializer
    GetLength: Expression -> Expression
    GetElementValue: Expression * Expression -> Expression }

type internal FieldSerializer = {
    Name: string
    ValueSerializer: ValueSerializer
    GetValue: Expression -> Expression }

type internal RecordSerializer = {
    DotnetType: Type
    Fields: FieldSerializer[] }

type internal OptionalSerializer = {
    DotnetType: Type
    ValueSerializer: ValueSerializer
    IsNull: Expression -> Expression
    GetValue: Expression -> Expression }

type internal ValueDeserializer =
    | Atomic of AtomicDeserializer
    | List of ListDeserializer
    | Record of RecordDeserializer
    | Optional of OptionalDeserializer
    with
    member this.DotnetType =
        match this with
        | ValueDeserializer.Atomic atomicDeserializer -> atomicDeserializer.DotnetType
        | ValueDeserializer.List listDeserializer -> listDeserializer.DotnetType
        | ValueDeserializer.Record recordDeserializer -> recordDeserializer.DotnetType
        | ValueDeserializer.Optional optionalDeserializer -> optionalDeserializer.DotnetType

type internal AtomicDeserializer = {
    DotnetType: Type
    DataDotnetType: Type
    CreateFromDataValue: Expression -> Expression }

type internal ListDeserializer = {
    DotnetType: Type
    ElementDeserializer: ValueDeserializer
    CreateEmpty: Expression
    CreateFromElementValues: Expression -> Expression }

type internal FieldDeserializer = {
    Name: string
    ValueDeserializer: ValueDeserializer }

type internal RecordDeserializer = {
    DotnetType: Type
    Fields: FieldDeserializer[]
    CreateFromFieldValues: Expression[] -> Expression }

type internal OptionalDeserializer = {
    DotnetType: Type
    ValueDeserializer: ValueDeserializer
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

module internal ValueSerializer =
    // TODO: This probably isn't valid any more!
    let private Cache = Dictionary<Type, ValueSerializer>()

    let private tryGetCached dotnetType =
        lock Cache (fun () ->
            match Cache.TryGetValue(dotnetType) with
            | false, _ -> Option.None
            | true, valueSerializer -> Option.Some valueSerializer)

    let private addToCache dotnetType valueSerializer =
        lock Cache (fun () ->
            Cache[dotnetType] <- valueSerializer)

    let createAtomic dotnetType dataDotnetType getDataValue =
        ValueSerializer.Atomic {
            AtomicSerializer.DotnetType = dotnetType
            AtomicSerializer.DataDotnetType = dataDotnetType
            AtomicSerializer.GetDataValue = getDataValue }

    let createRecord dotnetType fields =
        ValueSerializer.Record {
            RecordSerializer.DotnetType = dotnetType
            RecordSerializer.Fields = fields }

    let createList dotnetType elementSerializer getLength getElementValue =
        ValueSerializer.List {
            ListSerializer.DotnetType = dotnetType
            ListSerializer.ElementSerializer = elementSerializer
            ListSerializer.GetLength = getLength
            ListSerializer.GetElementValue = getElementValue }

    let createOptional dotnetType valueSerializer isNull getValue =
        ValueSerializer.Optional {
            OptionalSerializer.DotnetType = dotnetType
            OptionalSerializer.ValueSerializer = valueSerializer
            OptionalSerializer.IsNull = isNull
            OptionalSerializer.GetValue = getValue }

    let forReferenceTypeSerializer (valueSerializer: ValueSerializer) =
        let dotnetType = valueSerializer.DotnetType
        let isNull = Expression.IsNull
        let getValue = id
        ValueSerializer.createOptional dotnetType valueSerializer isNull getValue

    // TODO: This shouldn't really be necessary, but while Parquet.Net
    // treats all struct and list fields as optional it's necessary.
    let forNonNullableReferenceTypeSerializer (valueSerializer: ValueSerializer) =
        let dotnetType = valueSerializer.DotnetType
        let isNull = fun value -> Expression.False
        let getValue = id
        ValueSerializer.createOptional dotnetType valueSerializer isNull getValue

    let private getSchema' isOptional valueSerializer =
        match valueSerializer with
        | ValueSerializer.Atomic atomicSerializer ->
            ValueSchema.Atomic {
                IsOptional = isOptional
                DotnetType = atomicSerializer.DataDotnetType }
        | ValueSerializer.List listSerializer ->
            ValueSchema.List {
                IsOptional = isOptional
                Element = ValueSerializer.getSchema listSerializer.ElementSerializer }
        | ValueSerializer.Record recordSerializer ->
            ValueSchema.Record {
                IsOptional = isOptional
                Fields = recordSerializer.Fields |> Array.map FieldSerializer.getSchema }
        | ValueSerializer.Optional optionalSerializer ->
            ValueSerializer.getSchema' true optionalSerializer.ValueSerializer

    let getSchema valueSerializer =
        ValueSerializer.getSchema' false valueSerializer

    let ofType (dotnetType: Type) : ValueSerializer =
        match tryGetCached dotnetType with
        | Option.Some valueSerializer -> valueSerializer
        | Option.None ->
            let valueSerializer =
                ValueConverterFactory.All
                |> Array.tryPick _.TryCreateSerializer(dotnetType)
                |> Option.defaultWith (fun () ->
                    failwith $"unsupported type '{dotnetType.FullName}'")
            addToCache dotnetType valueSerializer
            valueSerializer

    let of'<'Value> =
        ofType typeof<'Value>

module internal ValueDeserializer =
    // TODO: This probably isn't valid any more!
    let private Cache = Dictionary<Type, ValueDeserializer>()

    let private tryGetCached dotnetType =
        lock Cache (fun () ->
            match Cache.TryGetValue(dotnetType) with
            | false, _ -> Option.None
            | true, valueDeserializer -> Option.Some valueDeserializer)

    let private addToCache dotnetType valueDeserializer =
        lock Cache (fun () ->
            Cache[dotnetType] <- valueDeserializer)

    // TODO: Rename to just atomic, list, etc.
    let createAtomic dotnetType dataDotnetType createFromDataValue =
        ValueDeserializer.Atomic {
            AtomicDeserializer.DotnetType = dotnetType
            AtomicDeserializer.DataDotnetType = dataDotnetType
            AtomicDeserializer.CreateFromDataValue = createFromDataValue }

    let createRecord dotnetType fields createFromFieldValues =
        ValueDeserializer.Record {
            RecordDeserializer.DotnetType = dotnetType
            RecordDeserializer.Fields = fields
            RecordDeserializer.CreateFromFieldValues = createFromFieldValues }

    let createList dotnetType elementDeserializer createEmpty createFromElementValues =
        ValueDeserializer.List {
            ListDeserializer.DotnetType = dotnetType
            ListDeserializer.ElementDeserializer = elementDeserializer
            ListDeserializer.CreateEmpty = createEmpty
            ListDeserializer.CreateFromElementValues = createFromElementValues }

    let createOptional dotnetType valueDeserializer createNull createFromValue =
        ValueDeserializer.Optional {
            OptionalDeserializer.DotnetType = dotnetType
            OptionalDeserializer.ValueDeserializer = valueDeserializer
            OptionalDeserializer.CreateNull = createNull
            OptionalDeserializer.CreateFromValue = createFromValue }

    let forReferenceTypeDeserializer (valueDeserializer: ValueDeserializer) =
        let dotnetType = valueDeserializer.DotnetType
        let createNull = Expression.Null(dotnetType)
        let createFromValue = id
        ValueDeserializer.createOptional
            dotnetType valueDeserializer createNull createFromValue

    // TODO: This shouldn't really be necessary, but while Parquet.Net
    // treats all struct and list fields as optional it's necessary.
    let forNonNullableReferenceTypeDeserializer (valueDeserializer: ValueDeserializer) =
        let dotnetType = valueDeserializer.DotnetType
        let createNull =
            Expression.Block(
                Expression.FailWith($"type '{dotnetType.FullName}' is not nullable"),
                Expression.Default(dotnetType))
            :> Expression
        let createFromValue = id
        ValueDeserializer.createOptional
            dotnetType valueDeserializer createNull createFromValue

    let private getSchema' isOptional valueDeserializer =
        match valueDeserializer with
        | ValueDeserializer.Atomic atomicDeserializer ->
            ValueSchema.Atomic {
                IsOptional = isOptional
                DotnetType = atomicDeserializer.DataDotnetType }
        | ValueDeserializer.List listDeserializer ->
            ValueSchema.List {
                IsOptional = isOptional
                Element = ValueDeserializer.getSchema listDeserializer.ElementDeserializer }
        | ValueDeserializer.Record recordDeserializer ->
            ValueSchema.Record {
                IsOptional = isOptional
                Fields = recordDeserializer.Fields |> Array.map FieldDeserializer.getSchema }
        | ValueDeserializer.Optional optionalDeserializer ->
            ValueDeserializer.getSchema' true optionalDeserializer.ValueDeserializer

    let getSchema valueDeserializer =
        ValueDeserializer.getSchema' false valueDeserializer

    let ofType (dotnetType: Type) schema : ValueDeserializer =
        match tryGetCached dotnetType with
        | Option.Some valueDeserializer -> valueDeserializer
        | Option.None ->
            let valueDeserializer =
                ValueConverterFactory.All
                |> Array.tryPick _.TryCreateDeserializer(dotnetType, schema)
                |> Option.defaultWith (fun () ->
                    failwith $"unsupported type '{dotnetType.FullName}'")
            addToCache dotnetType valueDeserializer
            valueDeserializer

    let of'<'Value> schema =
        ofType typeof<'Value> schema

module private FieldSerializer =
    let create name valueSerializer getValue =
        { FieldSerializer.Name = name
          FieldSerializer.ValueSerializer = valueSerializer
          FieldSerializer.GetValue = getValue }

    let ofProperty (field: PropertyInfo) : FieldSerializer =
        let name = field.Name
        let valueSerializer = ValueSerializer.ofType field.PropertyType
        let getValue (record: Expression) =
            Expression.Property(record, field)
            :> Expression
        create name valueSerializer getValue

    let ofUnionCaseField (unionCase: UnionCaseInfo) (field: PropertyInfo) =
        let name = field.Name
        let valueSerializer = ValueSerializer.ofType field.PropertyType
        let getValue (union: Expression) =
            Expression.Property(
                Expression.Convert(union, unionCase.DotnetType), field)
            :> Expression
        create name valueSerializer getValue

    let getSchema (fieldSerializer: FieldSerializer) =
        { FieldSchema.Name = fieldSerializer.Name
          FieldSchema.Value = ValueSerializer.getSchema fieldSerializer.ValueSerializer }

module private FieldDeserializer =
    let create name valueDeserializer =
        { FieldDeserializer.Name = name
          FieldDeserializer.ValueDeserializer = valueDeserializer }

    let ofProperty (field: PropertyInfo) schema : FieldDeserializer =
        let name = field.Name
        let valueDeserializer = ValueDeserializer.ofType field.PropertyType schema
        create name valueDeserializer

    let ofUnionCaseField (unionCase: UnionCaseInfo) (field: PropertyInfo) schema =
        let name = field.Name
        let valueDeserializer = ValueDeserializer.ofType field.PropertyType schema
        create name valueDeserializer

    let getSchema (fieldDeserializer: FieldDeserializer) =
        { FieldSchema.Name = fieldDeserializer.Name
          FieldSchema.Value = ValueDeserializer.getSchema fieldDeserializer.ValueDeserializer }

module internal RecordSerializer =
    let getRootSchema (recordSerializer: RecordSerializer) =
        { Schema.Fields =
            recordSerializer.Fields
            |> Array.map FieldSerializer.getSchema }

module internal RecordDeserializer =
    let getRootSchema (recordDeserializer: RecordDeserializer) =
        { Schema.Fields =
            recordDeserializer.Fields
            |> Array.map FieldDeserializer.getSchema }

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
    let valueSerializer =
        let dotnetType = typeof<'Value>
        let dataDotnetType = dotnetType
        let getDataValue = id
        ValueSerializer.createAtomic dotnetType dataDotnetType getDataValue
            
    let valueDeserializer =
        let dotnetType = typeof<'Value>
        let dataDotnetType = dotnetType
        let createFromDataValue = id
        ValueDeserializer.createAtomic dotnetType dataDotnetType createFromDataValue

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if dotnetType = valueSerializer.DotnetType
            then Option.Some valueSerializer
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if dotnetType = valueSerializer.DotnetType
            then Option.Some valueDeserializer
            else Option.None

type internal DateTimeOffsetConverterFactory() =
    let valueSerializer =
        let dotnetType = typeof<DateTimeOffset>
        let dataDotnetType = typeof<DateTime>
        let getDataValue (value: Expression) =
            Expression.Property(value, "UtcDateTime")
            :> Expression
        ValueSerializer.createAtomic dotnetType dataDotnetType getDataValue
            
    let valueDeserializer =
        let dotnetType = typeof<DateTimeOffset>
        let dataDotnetType = typeof<DateTime>
        let createFromDataValue (dataValue: Expression) =
            Expression.New(
                typeof<DateTimeOffset>.GetConstructor([| typeof<DateTime> |]),
                Expression.Call(dataValue, "ToUniversalTime", []))
            :> Expression
        ValueDeserializer.createAtomic dotnetType dataDotnetType createFromDataValue

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if dotnetType = valueSerializer.DotnetType
            then Option.Some valueSerializer
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if dotnetType = valueSerializer.DotnetType
            then Option.Some valueDeserializer
            else Option.None

type internal StringConverterFactory() =
    let valueSerializer =
        let dotnetType = typeof<string>
        let dataDotnetType = dotnetType
        let getDataValue = id
        ValueSerializer.createAtomic dotnetType dataDotnetType getDataValue
        |> ValueSerializer.forReferenceTypeSerializer
        
    let valueDeserializer =
        let dotnetType = typeof<string>
        let dataDotnetType = dotnetType
        let createFromDataValue = id
        ValueDeserializer.createAtomic dotnetType dataDotnetType createFromDataValue
        |> ValueDeserializer.forReferenceTypeDeserializer

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if dotnetType = valueSerializer.DotnetType
            then Option.Some valueSerializer
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if dotnetType = valueSerializer.DotnetType
            then Option.Some valueDeserializer
            else Option.None

type internal ByteArrayConverterFactory() =
    let valueSerializer =
        let dotnetType = typeof<byte[]>
        let dataDotnetType = dotnetType
        let getDataValue = id
        ValueSerializer.createAtomic dotnetType dataDotnetType getDataValue
        |> ValueSerializer.forReferenceTypeSerializer
        
    let valueDeserializer =
        let dotnetType = typeof<byte[]>
        let dataDotnetType = dotnetType
        let createFromDataValue = id
        ValueDeserializer.createAtomic dotnetType dataDotnetType createFromDataValue
        |> ValueDeserializer.forReferenceTypeDeserializer

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if dotnetType = valueSerializer.DotnetType
            then Option.Some valueSerializer
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if dotnetType = valueSerializer.DotnetType
            then Option.Some valueDeserializer
            else Option.None

type internal Array1dConverterFactory() =
    let isArray1dType (dotnetType: Type) =
        dotnetType.IsArray
        && dotnetType.GetArrayRank() = 1

    let createValueSerializer (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetElementType()
        let elementSerializer = ValueSerializer.ofType elementDotnetType
        let getLength (array: Expression) =
            Expression.Property(array, "Length")
            :> Expression
        let getElementValue (array: Expression, index: Expression) =
            Expression.ArrayIndex(array, [ index ])
            :> Expression
        ValueSerializer.createList dotnetType elementSerializer getLength getElementValue
        |> ValueSerializer.forReferenceTypeSerializer

    let createValueDeserializer (dotnetType: Type) (schema: ListSchema) =
        let elementDotnetType = dotnetType.GetElementType()
        let elementDeserializer =
            ValueDeserializer.ofType elementDotnetType schema.Element
        let createEmpty =
            Expression.NewArrayBounds(elementDotnetType, Expression.Constant(0))
        let createFromElementValues (elementValues: Expression) =
            Expression.Call(elementValues, "ToArray", [])
        ValueDeserializer.createList
            dotnetType elementDeserializer createEmpty createFromElementValues
        |> ValueDeserializer.forReferenceTypeDeserializer

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isArray1dType dotnetType
            then Option.Some (createValueSerializer dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if not (isArray1dType dotnetType)
            then Option.None
            else
                match schema with
                | ValueSchema.List listSchema ->
                    Option.Some (createValueDeserializer dotnetType listSchema)
                | _ -> Option.None

type internal GenericListConverterFactory() =
    let isGenericListType = DotnetType.isGenericType<ResizeArray<_>>

    let createValueSerializer (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementSerializer = ValueSerializer.ofType elementDotnetType
        let getLength (list: Expression) =
            Expression.Property(list, "Count")
            :> Expression
        let getElementValue (list: Expression, index: Expression) =
            Expression.MakeIndex(list, dotnetType.GetProperty("Item"), [ index ])
            :> Expression
        ValueSerializer.createList dotnetType elementSerializer getLength getElementValue
        |> ValueSerializer.forReferenceTypeSerializer

    let createValueDeserializer (dotnetType: Type) (schema: ListSchema) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementDeserializer =
            ValueDeserializer.ofType elementDotnetType schema.Element
        let createEmpty = Expression.New(dotnetType)
        let createFromElementValues = id
        ValueDeserializer.createList
            dotnetType elementDeserializer createEmpty createFromElementValues
        |> ValueDeserializer.forReferenceTypeDeserializer

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isGenericListType dotnetType
            then Option.Some (createValueSerializer dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if not (isGenericListType dotnetType)
            then Option.None
            else
                match schema with
                | ValueSchema.List listSchema ->
                    Option.Some (createValueDeserializer dotnetType listSchema)
                | _ -> Option.None

type internal FSharpListConverterFactory() =
    let isFSharpListType = DotnetType.isGenericType<list<_>>

    let createValueSerializer (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementSerializer = ValueSerializer.ofType elementDotnetType
        let getLength (list: Expression) =
            Expression.Property(list, "Length")
            :> Expression
        let getElementValue =
            let itemProperty = dotnetType.GetProperty("Item")
            fun (list: Expression, index: Expression) ->
                Expression.MakeIndex(list, itemProperty, [ index ])
                :> Expression
        // TODO: F# lists are not nullable by default, however Parquet.Net
        // does not support list fields that aren't nullable and a nullable
        // list field class can't easily be created since some of the relevant
        // properties are internal. For now, wrap as a non-nullable reference
        // type.
        ValueSerializer.createList dotnetType elementSerializer getLength getElementValue
        |> ValueSerializer.forNonNullableReferenceTypeSerializer

    let createValueDeserializer (dotnetType: Type) (schema: ListSchema) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementDeserializer =
            ValueDeserializer.ofType elementDotnetType schema.Element
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
        ValueDeserializer.createList
            dotnetType elementDeserializer createEmpty createFromElementValues
        |> ValueDeserializer.forNonNullableReferenceTypeDeserializer

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isFSharpListType dotnetType
            then Option.Some (createValueSerializer dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if not (isFSharpListType dotnetType)
            then Option.None
            else
                match schema with
                | ValueSchema.List listSchema ->
                    Option.Some (createValueDeserializer dotnetType listSchema)
                | _ -> Option.None

type internal FSharpRecordConverterFactory() =
    let isFSharpRecordType = FSharpType.IsRecord

    let createValueSerializer (dotnetType: Type) =
        let fields =
            FSharpType.GetRecordFields(dotnetType)
            |> Array.map FieldSerializer.ofProperty
        // TODO: F# records are not nullable by default, however Parquet.Net
        // does not support struct fields that aren't nullable and a nullable
        // struct field class can't easily be created since some of the relevant
        // properties are internal. For now, wrap as a non-nullable reference
        // type.
        ValueSerializer.createRecord dotnetType fields
        |> ValueSerializer.forNonNullableReferenceTypeSerializer

    let tryCreateValueDeserializer (dotnetType: Type) (recordSchema: RecordSchema) =
        let fields = FSharpType.GetRecordFields(dotnetType)
        let fieldDeserializers =
            fields
            |> Array.choose (fun field ->
                recordSchema.Fields
                |> Array.tryFind (fun fieldSchema -> fieldSchema.Name = field.Name)
                |> Option.map _.Value
                |> Option.map (FieldDeserializer.ofProperty field))
        if fieldDeserializers.Length < fields.Length
        then Option.None
        else
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
            ValueDeserializer.createRecord dotnetType fieldDeserializers createFromFieldValues
            |> ValueDeserializer.forNonNullableReferenceTypeDeserializer
            |> Option.Some

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isFSharpRecordType dotnetType
            then Option.Some (createValueSerializer dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if not (isFSharpRecordType dotnetType)
            then Option.None
            else
                match schema with
                | ValueSchema.Record recordSchema ->
                    tryCreateValueDeserializer dotnetType recordSchema
                | _ -> Option.None

type internal UnionConverterFactory() =
    let isUnionType = FSharpType.IsUnion

    let createSimpleUnionSerializer (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.DotnetType
        // Unions in which all cases have no fields can be represented as a
        // simple string value containing the case name. Since a union value
        // can't be null and must be one of the possible cases, this value is
        // not optional.
        let dataDotnetType = typeof<string>
        let getDataValue = unionInfo.GetCaseName
        ValueSerializer.createAtomic dotnetType dataDotnetType getDataValue

    let createUnionCaseSerializer (unionInfo: UnionInfo) (unionCase: UnionCaseInfo) =
        // Union case data is represented as an optional record containing the
        // field values for that case. The record needs to be optional since
        // only one case from the union can be set and the others will be NULL.
        let dotnetType = unionInfo.DotnetType
        let valueSerializer =
            let dotnetType = unionInfo.DotnetType
            let fields =
                unionCase.Fields
                |> Array.map (FieldSerializer.ofUnionCaseField unionCase)
            ValueSerializer.createRecord dotnetType fields
        // The data for this case is NULL if the union tag does not match the
        // tag for this case.
        let isNull (union: Expression) =
            Expression.NotEqual(unionInfo.GetTag union, unionCase.Tag)
            :> Expression
        let getValue = id
        ValueSerializer.createOptional dotnetType valueSerializer isNull getValue

    let createComplexUnionSerializer (unionInfo: UnionInfo) =
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
            let valueSerializer =
                // TODO: Can we justg use ValueSerializer.ofType here?
                let dotnetType = typeof<string>
                let dataDotnetType = dotnetType
                let getDataValue = id
                ValueSerializer.createAtomic dotnetType dataDotnetType getDataValue
            let getValue = unionInfo.GetCaseName
            FieldSerializer.create name valueSerializer getValue
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
                let valueSerializer = createUnionCaseSerializer unionInfo unionCase
                let getValue = id
                FieldSerializer.create name valueSerializer getValue)
        let fields = Array.append [| typeField |] caseFields
        // TODO: F# unions are not nullable by default, however we are mapping
        // to a struct and Parquet.Net does not support struct fields that
        // aren't nullable, so wrap as a non-nullable reference type.
        ValueSerializer.createRecord dotnetType fields
        |> ValueSerializer.forNonNullableReferenceTypeSerializer

    let createUnionSerializer dotnetType =
        // Unions are represented in one of two ways depending on whether any of
        // the cases have associated data fields.
        let unionInfo = UnionInfo.ofUnion dotnetType
        if unionInfo.UnionCases
            |> Array.forall (fun unionCase -> unionCase.Fields.Length = 0)
        then createSimpleUnionSerializer unionInfo
        else createComplexUnionSerializer unionInfo

    let createSimpleUnionDeserializer (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.DotnetType
        // Unions in which all cases have no fields can be represented as a
        // simple string value containing the case name. Since a union value
        // can't be null and must be one of the possible cases, this value is
        // not optional.
        let dataDotnetType = typeof<string>
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
        ValueDeserializer.createAtomic dotnetType dataDotnetType createFromDataValue

    let tryCreateUnionCaseDeserializer
        (unionInfo: UnionInfo) (unionCase: UnionCaseInfo) (schema: RecordSchema) =
        // Union case data is represented as an optional record containing the
        // field values for that case. The record needs to be optional since
        // only one case from the union can be set and the others will be NULL.
        let dotnetType = unionInfo.DotnetType
        let valueDeserializer =
            let dotnetType = unionInfo.DotnetType
            let fieldDeserializers =
                unionCase.Fields
                |> Array.choose (fun field ->
                    schema.Fields
                    |> Array.tryFind (fun fieldSchema -> fieldSchema.Name = field.Name)
                    |> Option.map _.Value
                    |> Option.map (FieldDeserializer.ofUnionCaseField unionCase field))
            let createFromFieldValues = unionCase.CreateFromFieldValues
            if fieldDeserializers.Length < unionCase.Fields.Length
            then Option.None
            else Option.Some (ValueDeserializer.createRecord dotnetType fieldDeserializers createFromFieldValues)
        match valueDeserializer with
        | Option.None -> Option.None
        | Option.Some valueDeserializer ->
            // We can't use {Expression.Null} here because union types are not
            // nullable, however they do still have {null} as their default value
            // because they are reference types.
            let createNull = Expression.Default(dotnetType) :> Expression
            let createFromValue = id
            ValueDeserializer.createOptional
                dotnetType valueDeserializer createNull createFromValue
            |> Option.Some

    let tryCreateComplexUnionDeserializer (unionInfo: UnionInfo) (schema: RecordSchema) =
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
            schema.Fields
            |> Array.tryFind (fun fieldSchema -> fieldSchema.Name = name)
            |> Option.map (fun fieldSchema ->
                // TODO: We should really delegate this to ValueDeserializer.ofType
                // and pass in the field value schema to confirm that it's actually
                // a string, but this would require having a string converter that
                // can treat strings as required.
                let valueDeserializer =
                    let dotnetType = typeof<string>
                    let dataDotnetType = dotnetType
                    let createFromDataValue = id
                    ValueDeserializer.createAtomic
                        dotnetType dataDotnetType createFromDataValue
                FieldDeserializer.create name valueDeserializer)
        // Each union case with one or more fields is assigned an additional
        // field within the record to hold its associated data. The name of this
        // field matches the case name and the value is a record that contains
        // the case's field values.
        let caseFields =
            unionCasesWithFields
            |> Array.choose (fun unionCase ->
                let name = unionCase.Name
                // Note that there's a chance the case name is the same as the
                // field name chosen to store the union case name, in which case
                // we'd have two fields with the same name. We could add a level
                // of nesting to the object structure to avoid this potential
                // name conflict, but this adds extra complexity.
                if typeField.IsSome && name = typeField.Value.Name
                then Option.None
                else
                    schema.Fields
                    |> Array.tryFind (fun fieldSchema -> fieldSchema.Name = name)
                    |> Option.bind (fun fieldSchema ->
                        match fieldSchema.Value with
                        | ValueSchema.Record recordSchema ->
                            tryCreateUnionCaseDeserializer unionInfo unionCase recordSchema
                            |> Option.map (FieldDeserializer.create name)
                        | _ -> Option.None))
        if typeField.IsNone
            || caseFields.Length < unionCasesWithFields.Length
        then Option.None
        else
            let fields = Array.append [| typeField.Value |] caseFields
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
            ValueDeserializer.createRecord dotnetType fields createFromFieldValues
            |> ValueDeserializer.forNonNullableReferenceTypeDeserializer
            |> Option.Some

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isUnionType dotnetType
            then Option.Some (createUnionSerializer dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if not (isUnionType dotnetType)
            then Option.None
            else
                // Unions are represented in one of two ways depending on
                // whether any of the cases have associated data fields.
                let unionInfo = UnionInfo.ofUnion dotnetType
                if unionInfo.UnionCases
                    |> Array.forall (fun unionCase -> unionCase.Fields.Length = 0)
                then Option.Some (createSimpleUnionDeserializer unionInfo)
                else
                    match schema with
                    | ValueSchema.Record recordSchema ->
                        tryCreateComplexUnionDeserializer unionInfo recordSchema
                    | _ -> Option.None


type internal NullableConverterFactory() =
    let isNullableType = DotnetType.isGenericType<Nullable<_>>

    let createRequiredValueSerializer dotnetType (valueSerializer: ValueSerializer) =
        let isNull (nullable: Expression) =
            Expression.Not(Expression.Property(nullable, "HasValue"))
            :> Expression
        let getValue (nullable: Expression) =
            Expression.Property(nullable, "Value")
            :> Expression
        ValueSerializer.createOptional dotnetType valueSerializer isNull getValue

    let createOptionalValueSerializer (dotnetType: Type) (valueSerializer: ValueSerializer) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                let getValue = id
                FieldSerializer.create name valueSerializer getValue
            [| valueField |]
        ValueSerializer.createRecord valueSerializer.DotnetType fields
        |> createRequiredValueSerializer dotnetType

    let createValueSerializer (dotnetType: Type) =
        let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
        let valueSerializer = ValueSerializer.ofType valueDotnetType
        if valueSerializer.IsOptional
        // TODO: This shouldn't really be possible as nullable values have to be
        // value types and therefore can't be null, but while we need to treat
        // all record types as optional we need to handle this case.
        then createOptionalValueSerializer dotnetType valueSerializer
        else createRequiredValueSerializer dotnetType valueSerializer

    let createRequiredValueDeserializer dotnetType (valueDeserializer: ValueDeserializer) =
        let createNull = Expression.Null(dotnetType)
        let createFromValue =
            let constructor = dotnetType.GetConstructor([| valueDeserializer.DotnetType |])
            fun (value: Expression) ->
                Expression.New(constructor, value)
                :> Expression
        ValueDeserializer.createOptional
            dotnetType valueDeserializer createNull createFromValue

    let createOptionalValueDeserializer (dotnetType: Type) (valueDeserializer: ValueDeserializer) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                FieldDeserializer.create name valueDeserializer
            [| valueField |]
        let createFromFieldValues (fieldValues: Expression[]) =
            fieldValues[0]
        ValueDeserializer.createRecord valueDeserializer.DotnetType fields createFromFieldValues
        |> createRequiredValueDeserializer dotnetType

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isNullableType dotnetType
            then Option.Some (createValueSerializer dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if not (isNullableType dotnetType)
            then Option.None
            else
                let isValueOptional, valueSchema =
                    match schema with
                    | ValueSchema.Record recordSchema ->
                        if recordSchema.Fields.Length = 1
                            && recordSchema.Fields[0].Name = "Value"
                        then true, recordSchema.Fields[0].Value
                        else false, schema
                    | _ -> false, schema
                let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
                let valueDeserializer =
                    ValueDeserializer.ofType valueDotnetType valueSchema
                if isValueOptional
                // TODO: This shouldn't really be possible as nullable values have to be
                // value types and therefore can't be null, but while we need to treat
                // all record types as optional we need to handle this case.
                then Option.Some (createOptionalValueDeserializer dotnetType valueDeserializer)
                else Option.Some (createRequiredValueDeserializer dotnetType valueDeserializer)

type internal OptionConverterFactory() =
    let isOptionType = DotnetType.isGenericType<option<_>>

    let createRequiredValueSerializer dotnetType (valueSerializer: ValueSerializer) =
        let isNull =
            let optionModuleType =
                Assembly.Load("FSharp.Core").GetTypes()
                |> Array.filter (fun type' -> type'.Name = "OptionModule")
                |> Array.exactlyOne
            fun (option: Expression) ->
                Expression.Call(optionModuleType, "IsNone", [| valueSerializer.DotnetType |], option)
                :> Expression
        let getValue (option: Expression) =
            Expression.Property(option, "Value")
            :> Expression
        ValueSerializer.createOptional dotnetType valueSerializer isNull getValue

    let createOptionalValueSerializer (dotnetType: Type) (valueSerializer: ValueSerializer) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                let getValue = id
                FieldSerializer.create name valueSerializer getValue
            [| valueField |]
        ValueSerializer.createRecord valueSerializer.DotnetType fields
        |> createRequiredValueSerializer dotnetType

    let createValueSerializer (dotnetType: Type) =
        let valueDotnetType = dotnetType.GetGenericArguments()[0]
        let valueSerializer = ValueSerializer.ofType valueDotnetType
        if valueSerializer.IsOptional
        then createOptionalValueSerializer dotnetType valueSerializer
        else createRequiredValueSerializer dotnetType valueSerializer

    let createRequiredValueDeserializer dotnetType (valueDeserializer: ValueDeserializer) =
        let unionCases = FSharpType.GetUnionCases(dotnetType)
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
        ValueDeserializer.createOptional
            dotnetType valueDeserializer createNull createFromValue

    let createOptionalValueDeserializer (dotnetType: Type) (valueDeserializer: ValueDeserializer) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                FieldDeserializer.create name valueDeserializer
            [| valueField |]
        let createFromFieldValues (fieldValues: Expression[]) =
            fieldValues[0]
        ValueDeserializer.createRecord valueDeserializer.DotnetType fields createFromFieldValues
        |> createRequiredValueDeserializer dotnetType

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isOptionType dotnetType
            then Option.Some (createValueSerializer dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if not (isOptionType dotnetType)
            then Option.None
            else
                let isValueOptional, valueSchema =
                    match schema with
                    | ValueSchema.Record recordSchema ->
                        if recordSchema.Fields.Length = 1
                            && recordSchema.Fields[0].Name = "Value"
                        then true, recordSchema.Fields[0].Value
                        else false, schema
                    | _ -> false, schema
                let valueDotnetType = dotnetType.GetGenericArguments()[0]
                let valueDeserializer =
                    ValueDeserializer.ofType valueDotnetType valueSchema
                if isValueOptional
                then Option.Some (createOptionalValueDeserializer dotnetType valueDeserializer)
                else Option.Some (createRequiredValueDeserializer dotnetType valueDeserializer)

type internal ClassConverterFactory() =
    let isClassWithDefaultConstructor (dotnetType: Type) =
        dotnetType.IsClass
        && not (isNull (dotnetType.GetConstructor([||])))

    let createValueSerializer (dotnetType: Type) =
        let fields =
            dotnetType.GetProperties(BindingFlags.Instance ||| BindingFlags.Public)
            |> Array.map FieldSerializer.ofProperty
        ValueSerializer.createRecord dotnetType fields
        |> ValueSerializer.forReferenceTypeSerializer

    let tryCreateValueDeserializer (dotnetType: Type) (recordSchema: RecordSchema) =
        let fields = dotnetType.GetProperties(BindingFlags.Instance ||| BindingFlags.Public)
        let fieldDeserializers =
            fields
            |> Array.choose (fun field ->
                recordSchema.Fields
                |> Array.tryFind (fun fieldSchema -> fieldSchema.Name = field.Name)
                |> Option.map _.Value
                |> Option.map (FieldDeserializer.ofProperty field))
        if fieldDeserializers.Length < fields.Length
        then Option.None
        else
            let createFromFieldValues =
                let defaultConstructor = dotnetType.GetConstructor([||])
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
            ValueDeserializer.createRecord dotnetType fieldDeserializers createFromFieldValues
            |> ValueDeserializer.forReferenceTypeDeserializer
            |> Option.Some

    interface IValueConverterFactory with
        member this.TryCreateSerializer(dotnetType) =
            if isClassWithDefaultConstructor dotnetType
            then Option.Some (createValueSerializer dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if not (isClassWithDefaultConstructor dotnetType)
            then Option.None
            else
                match schema with
                | ValueSchema.Record recordSchema ->
                    tryCreateValueDeserializer dotnetType recordSchema
                | _ -> Option.None
