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

// TODO: Can we cache anything, e.g. reflected info?

// TODO: Attribute to select specific serializer type to use? Alternatively could
// be part of the serializer configuration?

// TODO: Eventually should be able to rename to remove 'Factory'
type internal IValueConverter =
    // TODO: Rename to sourceType?
    abstract member TryCreateSerializer
        : dotnetType:Type -> Serializer option
    // TODO: Rename to targetType and sourceSchema?
    abstract member TryCreateDeserializer
        : dotnetType:Type * schema:ValueSchema -> Deserializer option

type internal Serializer =
    | Atomic of AtomicSerializer
    | List of ListSerializer
    | Record of RecordSerializer
    | Optional of OptionalSerializer
    with
    member this.DotnetType =
        match this with
        | Serializer.Atomic atomicSerializer -> atomicSerializer.DotnetType
        | Serializer.List listSerializer -> listSerializer.DotnetType
        | Serializer.Record recordSerializer -> recordSerializer.DotnetType
        | Serializer.Optional optionalSerializer -> optionalSerializer.DotnetType

type internal AtomicSerializer = {
    DotnetType: Type
    DataDotnetType: Type
    GetDataValue: Expression -> Expression }

type internal ListSerializer = {
    DotnetType: Type
    ElementSerializer: Serializer
    GetLength: Expression -> Expression
    GetElementValue: Expression * Expression -> Expression }

type internal FieldSerializer = {
    Name: string
    Serializer: Serializer
    GetValue: Expression -> Expression }

type internal RecordSerializer = {
    DotnetType: Type
    Fields: FieldSerializer[] }

type internal OptionalSerializer = {
    DotnetType: Type
    Serializer: Serializer
    IsNull: Expression -> Expression
    GetValue: Expression -> Expression }

type internal Deserializer =
    | Atomic of AtomicDeserializer
    | List of ListDeserializer
    | Record of RecordDeserializer
    | Optional of OptionalDeserializer
    with
    member this.DotnetType =
        match this with
        | Deserializer.Atomic atomicDeserializer -> atomicDeserializer.DotnetType
        | Deserializer.List listDeserializer -> listDeserializer.DotnetType
        | Deserializer.Record recordDeserializer -> recordDeserializer.DotnetType
        | Deserializer.Optional optionalDeserializer -> optionalDeserializer.DotnetType

type internal AtomicDeserializer = {
    DotnetType: Type
    DataDotnetType: Type
    CreateFromDataValue: Expression -> Expression }

type internal ListDeserializer = {
    DotnetType: Type
    ElementDeserializer: Deserializer
    CreateEmpty: Expression
    CreateFromElementValues: Expression -> Expression }

type internal FieldDeserializer = {
    Name: string
    Deserializer: Deserializer }

type internal RecordDeserializer = {
    DotnetType: Type
    Fields: FieldDeserializer[]
    CreateFromFieldValues: Expression[] -> Expression }

type internal OptionalDeserializer = {
    DotnetType: Type
    Deserializer: Deserializer
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

module internal Serializer =
    let atomic dotnetType dataDotnetType getDataValue =
        Serializer.Atomic {
            AtomicSerializer.DotnetType = dotnetType
            AtomicSerializer.DataDotnetType = dataDotnetType
            AtomicSerializer.GetDataValue = getDataValue }

    let record dotnetType fields =
        Serializer.Record {
            RecordSerializer.DotnetType = dotnetType
            RecordSerializer.Fields = fields }

    let list dotnetType elementSerializer getLength getElementValue =
        Serializer.List {
            ListSerializer.DotnetType = dotnetType
            ListSerializer.ElementSerializer = elementSerializer
            ListSerializer.GetLength = getLength
            ListSerializer.GetElementValue = getElementValue }

    let optional dotnetType serializer isNull getValue =
        Serializer.Optional {
            OptionalSerializer.DotnetType = dotnetType
            OptionalSerializer.Serializer = serializer
            OptionalSerializer.IsNull = isNull
            OptionalSerializer.GetValue = getValue }

    let referenceTypeWrapper (serializer: Serializer) =
        let dotnetType = serializer.DotnetType
        let isNull = Expression.IsNull
        let getValue = id
        Serializer.optional dotnetType serializer isNull getValue

    // TODO: This shouldn't really be necessary, but while Parquet.Net
    // treats all struct and list fields as optional it's necessary.
    let nonNullableReferenceTypeWrapper (serializer: Serializer) =
        let dotnetType = serializer.DotnetType
        let isNull = fun value -> Expression.False
        let getValue = id
        Serializer.optional dotnetType serializer isNull getValue

    let private getSchema' isOptional serializer =
        match serializer with
        | Serializer.Atomic atomicSerializer ->
            ValueSchema.Atomic {
                IsOptional = isOptional
                DotnetType = atomicSerializer.DataDotnetType }
        | Serializer.List listSerializer ->
            ValueSchema.List {
                IsOptional = isOptional
                Element = Serializer.getSchema listSerializer.ElementSerializer }
        | Serializer.Record recordSerializer ->
            ValueSchema.Record {
                IsOptional = isOptional
                Fields = recordSerializer.Fields |> Array.map FieldSerializer.getSchema }
        | Serializer.Optional optionalSerializer ->
            Serializer.getSchema' true optionalSerializer.Serializer

    let getSchema serializer =
        Serializer.getSchema' false serializer

    let ofType (dotnetType: Type) : Serializer =
        ValueConverter.All
        |> Array.tryPick _.TryCreateSerializer(dotnetType)
        |> Option.defaultWith (fun () ->
            failwith $"unsupported type '{dotnetType.FullName}'")

    let of'<'Value> =
        ofType typeof<'Value>

module internal Deserializer =
    let atomic dotnetType dataDotnetType createFromDataValue =
        Deserializer.Atomic {
            AtomicDeserializer.DotnetType = dotnetType
            AtomicDeserializer.DataDotnetType = dataDotnetType
            AtomicDeserializer.CreateFromDataValue = createFromDataValue }

    let record dotnetType fields createFromFieldValues =
        Deserializer.Record {
            RecordDeserializer.DotnetType = dotnetType
            RecordDeserializer.Fields = fields
            RecordDeserializer.CreateFromFieldValues = createFromFieldValues }

    let list dotnetType elementDeserializer createEmpty createFromElementValues =
        Deserializer.List {
            ListDeserializer.DotnetType = dotnetType
            ListDeserializer.ElementDeserializer = elementDeserializer
            ListDeserializer.CreateEmpty = createEmpty
            ListDeserializer.CreateFromElementValues = createFromElementValues }

    let optional dotnetType deserializer createNull createFromValue =
        Deserializer.Optional {
            OptionalDeserializer.DotnetType = dotnetType
            OptionalDeserializer.Deserializer = deserializer
            OptionalDeserializer.CreateNull = createNull
            OptionalDeserializer.CreateFromValue = createFromValue }

    let referenceTypeWrapper (deserializer: Deserializer) =
        let dotnetType = deserializer.DotnetType
        let createNull = Expression.Null(dotnetType)
        let createFromValue = id
        Deserializer.optional
            dotnetType deserializer createNull createFromValue

    // TODO: This shouldn't really be necessary, but while Parquet.Net
    // treats all struct and list fields as optional it's necessary.
    let nonNullableReferenceTypeWrapper (deserializer: Deserializer) =
        let dotnetType = deserializer.DotnetType
        let createNull =
            Expression.Block(
                Expression.FailWith($"type '{dotnetType.FullName}' is not nullable"),
                Expression.Default(dotnetType))
            :> Expression
        let createFromValue = id
        Deserializer.optional
            dotnetType deserializer createNull createFromValue

    let private getSchema' isOptional deserializer =
        match deserializer with
        | Deserializer.Atomic atomicDeserializer ->
            ValueSchema.Atomic {
                IsOptional = isOptional
                DotnetType = atomicDeserializer.DataDotnetType }
        | Deserializer.List listDeserializer ->
            ValueSchema.List {
                IsOptional = isOptional
                Element = Deserializer.getSchema listDeserializer.ElementDeserializer }
        | Deserializer.Record recordDeserializer ->
            ValueSchema.Record {
                IsOptional = isOptional
                Fields = recordDeserializer.Fields |> Array.map FieldDeserializer.getSchema }
        | Deserializer.Optional optionalDeserializer ->
            Deserializer.getSchema' true optionalDeserializer.Deserializer

    let getSchema deserializer =
        Deserializer.getSchema' false deserializer

    let ofType (dotnetType: Type) schema : Deserializer =
        ValueConverter.All
        |> Array.tryPick _.TryCreateDeserializer(dotnetType, schema)
        |> Option.defaultWith (fun () ->
            failwith $"unsupported type '{dotnetType.FullName}'")

    let of'<'Value> schema =
        ofType typeof<'Value> schema

module private FieldSerializer =
    let create name serializer getValue =
        { FieldSerializer.Name = name
          FieldSerializer.Serializer = serializer
          FieldSerializer.GetValue = getValue }

    let ofProperty (field: PropertyInfo) : FieldSerializer =
        let name = field.Name
        let serializer = Serializer.ofType field.PropertyType
        let getValue (record: Expression) =
            Expression.Property(record, field)
            :> Expression
        create name serializer getValue

    let ofUnionCaseField (unionCase: UnionCaseInfo) (field: PropertyInfo) =
        let name = field.Name
        let serializer = Serializer.ofType field.PropertyType
        let getValue (union: Expression) =
            Expression.Property(
                Expression.Convert(union, unionCase.DotnetType), field)
            :> Expression
        create name serializer getValue

    let getSchema (fieldSerializer: FieldSerializer) =
        { FieldSchema.Name = fieldSerializer.Name
          FieldSchema.Value = Serializer.getSchema fieldSerializer.Serializer }

module private FieldDeserializer =
    let create name deserializer =
        { FieldDeserializer.Name = name
          FieldDeserializer.Deserializer = deserializer }

    let ofProperty (field: PropertyInfo) schema : FieldDeserializer =
        let name = field.Name
        let deserializer = Deserializer.ofType field.PropertyType schema
        create name deserializer

    let getSchema (fieldDeserializer: FieldDeserializer) =
        { FieldSchema.Name = fieldDeserializer.Name
          FieldSchema.Value = Deserializer.getSchema fieldDeserializer.Deserializer }

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

module internal ValueConverter =
    let All : IValueConverter[] = [|
        PrimitiveConverter<bool>()
        PrimitiveConverter<int8>()
        PrimitiveConverter<int16>()
        Int32Converter()
        PrimitiveConverter<int64>()
        PrimitiveConverter<uint8>()
        PrimitiveConverter<uint16>()
        PrimitiveConverter<uint32>()
        PrimitiveConverter<uint64>()
        PrimitiveConverter<float32>()
        PrimitiveConverter<float>()
        PrimitiveConverter<decimal>()
        PrimitiveConverter<DateTime>()
        PrimitiveConverter<Guid>()
        DateTimeOffsetConverter()
        StringConverter()
        // This must come before the generic array type since byte arrays are
        // supported as a primitive type in Parquet and are therefore handled as
        // atomic values rather than lists.
        ByteArrayConverter()
        Array1dConverter()
        GenericListConverter()
        FSharpListConverter()
        FSharpRecordConverter()
        NullableConverter()
        // This must come before the generic union type since option types are
        // handled in a special way.
        OptionConverter()
        UnionConverter()
        ClassConverter()
    |]

type internal PrimitiveConverter<'Value>() =
    let dotnetType = typeof<'Value>
    let dataDotnetType = dotnetType

    let serializer =
        let getDataValue = id
        Serializer.atomic dotnetType dataDotnetType getDataValue

    let deserializer =
        let createFromDataValue = id
        Deserializer.atomic dotnetType dataDotnetType createFromDataValue

    interface IValueConverter with
        member this.TryCreateSerializer(dotnetType) =
            if dotnetType = serializer.DotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if dotnetType = serializer.DotnetType
            then Option.Some deserializer
            else Option.None

type internal Int32Converter() =
    let dotnetType = typeof<int32>

    let serializer =
        let dataDotnetType = dotnetType
        let getDataValue = id
        Serializer.atomic dotnetType dataDotnetType getDataValue

    let createDeserializer dataDotnetType =
        let createFromDataValue dataValue =
            Expression.Convert(dataValue, dotnetType)
            :> Expression
        Deserializer.atomic dotnetType dataDotnetType createFromDataValue

    interface IValueConverter with
        member this.TryCreateSerializer(dotnetType) =
            if dotnetType = serializer.DotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if dotnetType <> typeof<int32>
            then Option.None
            else
                match schema with
                | ValueSchema.Atomic atomicSchema
                    when not atomicSchema.IsOptional
                        && (atomicSchema.DotnetType = typeof<int32>
                            || atomicSchema.DotnetType = typeof<int16>
                            || atomicSchema.DotnetType = typeof<int8>
                            || atomicSchema.DotnetType = typeof<uint16>
                            || atomicSchema.DotnetType = typeof<uint8>) ->
                    Option.Some (createDeserializer atomicSchema.DotnetType)
                | _ -> Option.None

type internal DateTimeOffsetConverter() =
    let dotnetType = typeof<DateTimeOffset>
    let dataDotnetType = typeof<DateTime>

    let serializer =
        let getDataValue (value: Expression) =
            Expression.Property(value, "UtcDateTime")
            :> Expression
        Serializer.atomic dotnetType dataDotnetType getDataValue

    let deserializer =
        let createFromDataValue (dataValue: Expression) =
            Expression.New(
                typeof<DateTimeOffset>.GetConstructor([| typeof<DateTime> |]),
                Expression.Call(dataValue, "ToUniversalTime", []))
            :> Expression
        Deserializer.atomic dotnetType dataDotnetType createFromDataValue

    interface IValueConverter with
        member this.TryCreateSerializer(dotnetType) =
            if dotnetType = serializer.DotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if dotnetType = serializer.DotnetType
            then Option.Some deserializer
            else Option.None

type internal StringConverter() =
    let dotnetType = typeof<string>
    let dataDotnetType = dotnetType

    let serializer =
        let getDataValue = id
        Serializer.atomic dotnetType dataDotnetType getDataValue
        |> Serializer.referenceTypeWrapper

    let deserializer =
        let createFromDataValue = id
        Deserializer.atomic dotnetType dataDotnetType createFromDataValue
        |> Deserializer.referenceTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(dotnetType) =
            if dotnetType = serializer.DotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if dotnetType = serializer.DotnetType
            then Option.Some deserializer
            else Option.None

type internal ByteArrayConverter() =
    let dotnetType = typeof<byte[]>
    let dataDotnetType = dotnetType

    let serializer =
        let getDataValue = id
        Serializer.atomic dotnetType dataDotnetType getDataValue
        |> Serializer.referenceTypeWrapper

    let deserializer =
        let createFromDataValue = id
        Deserializer.atomic dotnetType dataDotnetType createFromDataValue
        |> Deserializer.referenceTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(dotnetType) =
            if dotnetType = serializer.DotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if dotnetType = serializer.DotnetType
            then Option.Some deserializer
            else Option.None

type internal Array1dConverter() =
    let isArray1dType (dotnetType: Type) =
        dotnetType.IsArray
        && dotnetType.GetArrayRank() = 1

    let createSerializer (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetElementType()
        let elementSerializer = Serializer.ofType elementDotnetType
        let getLength (array: Expression) =
            Expression.Property(array, "Length")
            :> Expression
        let getElementValue (array: Expression, index: Expression) =
            Expression.ArrayIndex(array, [ index ])
            :> Expression
        Serializer.list dotnetType elementSerializer getLength getElementValue
        |> Serializer.referenceTypeWrapper

    let createDeserializer (dotnetType: Type) (schema: ListSchema) =
        let elementDotnetType = dotnetType.GetElementType()
        let elementDeserializer =
            Deserializer.ofType elementDotnetType schema.Element
        let createEmpty =
            Expression.NewArrayBounds(elementDotnetType, Expression.Constant(0))
        let createFromElementValues (elementValues: Expression) =
            Expression.Call(elementValues, "ToArray", [])
        Deserializer.list
            dotnetType elementDeserializer createEmpty createFromElementValues
        |> Deserializer.referenceTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(dotnetType) =
            if isArray1dType dotnetType
            then Option.Some (createSerializer dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if not (isArray1dType dotnetType)
            then Option.None
            else
                match schema with
                | ValueSchema.List listSchema ->
                    Option.Some (createDeserializer dotnetType listSchema)
                | _ -> Option.None

type internal GenericListConverter() =
    let isGenericListType = DotnetType.isGenericType<ResizeArray<_>>

    let createSerializer (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementSerializer = Serializer.ofType elementDotnetType
        let getLength (list: Expression) =
            Expression.Property(list, "Count")
            :> Expression
        let getElementValue (list: Expression, index: Expression) =
            Expression.MakeIndex(list, dotnetType.GetProperty("Item"), [ index ])
            :> Expression
        Serializer.list dotnetType elementSerializer getLength getElementValue
        |> Serializer.referenceTypeWrapper

    let createDeserializer (dotnetType: Type) (schema: ListSchema) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementDeserializer =
            Deserializer.ofType elementDotnetType schema.Element
        let createEmpty = Expression.New(dotnetType)
        let createFromElementValues = id
        Deserializer.list
            dotnetType elementDeserializer createEmpty createFromElementValues
        |> Deserializer.referenceTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(dotnetType) =
            if isGenericListType dotnetType
            then Option.Some (createSerializer dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if not (isGenericListType dotnetType)
            then Option.None
            else
                match schema with
                | ValueSchema.List listSchema ->
                    Option.Some (createDeserializer dotnetType listSchema)
                | _ -> Option.None

type internal FSharpListConverter() =
    let isFSharpListType = DotnetType.isGenericType<list<_>>

    let createSerializer (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementSerializer = Serializer.ofType elementDotnetType
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
        Serializer.list dotnetType elementSerializer getLength getElementValue
        |> Serializer.nonNullableReferenceTypeWrapper

    let createDeserializer (dotnetType: Type) (schema: ListSchema) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementDeserializer =
            Deserializer.ofType elementDotnetType schema.Element
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
        Deserializer.list
            dotnetType elementDeserializer createEmpty createFromElementValues
        |> Deserializer.nonNullableReferenceTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(dotnetType) =
            if isFSharpListType dotnetType
            then Option.Some (createSerializer dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if not (isFSharpListType dotnetType)
            then Option.None
            else
                match schema with
                | ValueSchema.List listSchema ->
                    Option.Some (createDeserializer dotnetType listSchema)
                | _ -> Option.None

type internal FSharpRecordConverter() =
    let isFSharpRecordType = FSharpType.IsRecord

    let createSerializer (dotnetType: Type) =
        let fields =
            FSharpType.GetRecordFields(dotnetType)
            |> Array.map FieldSerializer.ofProperty
        // TODO: F# records are not nullable by default, however Parquet.Net
        // does not support struct fields that aren't nullable and a nullable
        // struct field class can't easily be created since some of the relevant
        // properties are internal. For now, wrap as a non-nullable reference
        // type.
        Serializer.record dotnetType fields
        |> Serializer.nonNullableReferenceTypeWrapper

    let tryCreateDeserializer (dotnetType: Type) (recordSchema: RecordSchema) =
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
            Deserializer.record dotnetType fieldDeserializers createFromFieldValues
            |> Deserializer.nonNullableReferenceTypeWrapper
            |> Option.Some

    interface IValueConverter with
        member this.TryCreateSerializer(dotnetType) =
            if isFSharpRecordType dotnetType
            then Option.Some (createSerializer dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if not (isFSharpRecordType dotnetType)
            then Option.None
            else
                match schema with
                | ValueSchema.Record recordSchema ->
                    tryCreateDeserializer dotnetType recordSchema
                | _ -> Option.None

type internal UnionConverter() =
    let isUnionType = FSharpType.IsUnion

    let createSimpleUnionSerializer (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.DotnetType
        // Unions in which all cases have no fields can be represented as a
        // simple string value containing the case name. Since a union value
        // can't be null and must be one of the possible cases, this value is
        // not optional.
        let dataDotnetType = typeof<string>
        let getDataValue = unionInfo.GetCaseName
        Serializer.atomic dotnetType dataDotnetType getDataValue

    let createUnionCaseSerializer (unionInfo: UnionInfo) (unionCase: UnionCaseInfo) =
        // Union case data is represented as an optional record containing the
        // field values for that case. The record needs to be optional since
        // only one case from the union can be set and the others will be NULL.
        let dotnetType = unionInfo.DotnetType
        let serializer =
            let dotnetType = unionInfo.DotnetType
            let fields =
                unionCase.Fields
                |> Array.map (FieldSerializer.ofUnionCaseField unionCase)
            Serializer.record dotnetType fields
        // The data for this case is NULL if the union tag does not match the
        // tag for this case.
        let isNull (union: Expression) =
            Expression.NotEqual(unionInfo.GetTag union, unionCase.Tag)
            :> Expression
        let getValue = id
        Serializer.optional dotnetType serializer isNull getValue

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
            let serializer =
                // TODO: We should really delegate this to Serializer.ofType,
                // but this would require having a string converter that
                // can treat strings as required.
                let dotnetType = typeof<string>
                let dataDotnetType = dotnetType
                let getDataValue = id
                Serializer.atomic dotnetType dataDotnetType getDataValue
            let getValue = unionInfo.GetCaseName
            FieldSerializer.create name serializer getValue
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
                let serializer = createUnionCaseSerializer unionInfo unionCase
                let getValue = id
                FieldSerializer.create name serializer getValue)
        let fields = Array.append [| typeField |] caseFields
        // TODO: F# unions are not nullable by default, however we are mapping
        // to a struct and Parquet.Net does not support struct fields that
        // aren't nullable, so wrap as a non-nullable reference type.
        Serializer.record dotnetType fields
        |> Serializer.nonNullableReferenceTypeWrapper

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
        Deserializer.atomic dotnetType dataDotnetType createFromDataValue

    let tryCreateUnionCaseDeserializer
        (unionInfo: UnionInfo) (unionCase: UnionCaseInfo) (schema: RecordSchema) =
        // Union case data is represented as an optional record containing the
        // field values for that case. The record needs to be optional since
        // only one case from the union can be set and the others will be NULL.
        let dotnetType = unionInfo.DotnetType
        let deserializer =
            let dotnetType = unionInfo.DotnetType
            let fieldDeserializers =
                unionCase.Fields
                |> Array.choose (fun field ->
                    schema.Fields
                    |> Array.tryFind (fun fieldSchema -> fieldSchema.Name = field.Name)
                    |> Option.map _.Value
                    |> Option.map (FieldDeserializer.ofProperty field))
            let createFromFieldValues = unionCase.CreateFromFieldValues
            if fieldDeserializers.Length < unionCase.Fields.Length
            then Option.None
            else Option.Some (Deserializer.record dotnetType fieldDeserializers createFromFieldValues)
        match deserializer with
        | Option.None -> Option.None
        | Option.Some deserializer ->
            // We can't use {Expression.Null} here because union types are not
            // nullable, however they do still have {null} as their default value
            // because they are reference types.
            let createNull = Expression.Default(dotnetType) :> Expression
            let createFromValue = id
            Deserializer.optional
                dotnetType deserializer createNull createFromValue
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
                // TODO: We should really delegate this to Deserializer.ofType
                // and pass in the field value schema to confirm that it's actually
                // a string, but this would require having a string converter that
                // can treat strings as required.
                let deserializer =
                    let dotnetType = typeof<string>
                    let dataDotnetType = dotnetType
                    let createFromDataValue = id
                    Deserializer.atomic
                        dotnetType dataDotnetType createFromDataValue
                FieldDeserializer.create name deserializer)
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
            Deserializer.record dotnetType fields createFromFieldValues
            |> Deserializer.nonNullableReferenceTypeWrapper
            |> Option.Some

    interface IValueConverter with
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


type internal NullableConverter() =
    let isNullableType = DotnetType.isGenericType<Nullable<_>>

    let createRequiredSerializer dotnetType (serializer: Serializer) =
        let isNull (nullable: Expression) =
            Expression.Not(Expression.Property(nullable, "HasValue"))
            :> Expression
        let getValue (nullable: Expression) =
            Expression.Property(nullable, "Value")
            :> Expression
        Serializer.optional dotnetType serializer isNull getValue

    let createOptionalSerializer (dotnetType: Type) (serializer: Serializer) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                let getValue = id
                FieldSerializer.create name serializer getValue
            [| valueField |]
        Serializer.record serializer.DotnetType fields
        |> createRequiredSerializer dotnetType

    let createSerializer (dotnetType: Type) =
        let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
        let serializer = Serializer.ofType valueDotnetType
        if serializer.IsOptional
        // TODO: This shouldn't really be possible as nullable values have to be
        // value types and therefore can't be null, but while we need to treat
        // all record types as optional we need to handle this case.
        then createOptionalSerializer dotnetType serializer
        else createRequiredSerializer dotnetType serializer

    let createRequiredDeserializer dotnetType (deserializer: Deserializer) =
        let createNull = Expression.Null(dotnetType)
        let createFromValue =
            let constructor = dotnetType.GetConstructor([| deserializer.DotnetType |])
            fun (value: Expression) ->
                Expression.New(constructor, value)
                :> Expression
        Deserializer.optional
            dotnetType deserializer createNull createFromValue

    let createOptionalDeserializer (dotnetType: Type) (deserializer: Deserializer) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                FieldDeserializer.create name deserializer
            [| valueField |]
        let createFromFieldValues (fieldValues: Expression[]) =
            fieldValues[0]
        Deserializer.record deserializer.DotnetType fields createFromFieldValues
        |> createRequiredDeserializer dotnetType

    interface IValueConverter with
        member this.TryCreateSerializer(dotnetType) =
            if isNullableType dotnetType
            then Option.Some (createSerializer dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if not (isNullableType dotnetType)
            then Option.None
            else
                let isValueOptional, valueSchema =
                    match schema with
                    | ValueSchema.Record recordSchema ->
                        // TODO: This seems a bit hacky!
                        if recordSchema.Fields.Length = 1
                            && recordSchema.Fields[0].Name = "Value"
                        then true, recordSchema.Fields[0].Value
                        else false, schema
                    | _ -> false, schema
                let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
                let valueSchema =
                    if not isValueOptional
                    then valueSchema.MakeRequired()
                    else valueSchema
                let deserializer =
                    Deserializer.ofType valueDotnetType valueSchema
                if isValueOptional
                // TODO: This shouldn't really be possible as nullable values have to be
                // value types and therefore can't be null, but while we need to treat
                // all record types as optional we need to handle this case.
                then Option.Some (createOptionalDeserializer dotnetType deserializer)
                else Option.Some (createRequiredDeserializer dotnetType deserializer)

type internal OptionConverter() =
    let isOptionType = DotnetType.isGenericType<option<_>>

    let createRequiredSerializer dotnetType (serializer: Serializer) =
        let isNull =
            let optionModuleType =
                Assembly.Load("FSharp.Core").GetTypes()
                |> Array.filter (fun type' -> type'.Name = "OptionModule")
                |> Array.exactlyOne
            fun (option: Expression) ->
                Expression.Call(optionModuleType, "IsNone", [| serializer.DotnetType |], option)
                :> Expression
        let getValue (option: Expression) =
            Expression.Property(option, "Value")
            :> Expression
        Serializer.optional dotnetType serializer isNull getValue

    let createOptionalSerializer (dotnetType: Type) (serializer: Serializer) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                let getValue = id
                FieldSerializer.create name serializer getValue
            [| valueField |]
        Serializer.record serializer.DotnetType fields
        |> createRequiredSerializer dotnetType

    let createSerializer (dotnetType: Type) =
        let valueDotnetType = dotnetType.GetGenericArguments()[0]
        let serializer = Serializer.ofType valueDotnetType
        if serializer.IsOptional
        then createOptionalSerializer dotnetType serializer
        else createRequiredSerializer dotnetType serializer

    let createRequiredDeserializer dotnetType (deserializer: Deserializer) =
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
        Deserializer.optional
            dotnetType deserializer createNull createFromValue

    let createOptionalDeserializer (dotnetType: Type) (deserializer: Deserializer) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                FieldDeserializer.create name deserializer
            [| valueField |]
        let createFromFieldValues (fieldValues: Expression[]) =
            fieldValues[0]
        Deserializer.record deserializer.DotnetType fields createFromFieldValues
        |> createRequiredDeserializer dotnetType

    interface IValueConverter with
        member this.TryCreateSerializer(dotnetType) =
            if isOptionType dotnetType
            then Option.Some (createSerializer dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if not (isOptionType dotnetType)
            then Option.None
            else
                let isValueOptional, valueSchema =
                    match schema with
                    | ValueSchema.Record recordSchema ->
                        // TODO: This seems a bit hacky!
                        if recordSchema.Fields.Length = 1
                            && recordSchema.Fields[0].Name = "Value"
                        then true, recordSchema.Fields[0].Value
                        else false, schema
                    | _ -> false, schema
                let valueDotnetType = dotnetType.GetGenericArguments()[0]
                let valueSchema =
                    if not isValueOptional
                    then valueSchema.MakeRequired()
                    else valueSchema
                let deserializer =
                    Deserializer.ofType valueDotnetType valueSchema
                if isValueOptional
                then Option.Some (createOptionalDeserializer dotnetType deserializer)
                else Option.Some (createRequiredDeserializer dotnetType deserializer)

type internal ClassConverter() =
    let isClassWithDefaultConstructor (dotnetType: Type) =
        dotnetType.IsClass
        && not (isNull (dotnetType.GetConstructor([||])))

    let createSerializer (dotnetType: Type) =
        let fields =
            dotnetType.GetProperties(BindingFlags.Instance ||| BindingFlags.Public)
            |> Array.map FieldSerializer.ofProperty
        Serializer.record dotnetType fields
        |> Serializer.referenceTypeWrapper

    let tryCreateDeserializer (dotnetType: Type) (recordSchema: RecordSchema) =
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
            Deserializer.record dotnetType fieldDeserializers createFromFieldValues
            |> Deserializer.referenceTypeWrapper
            |> Option.Some

    interface IValueConverter with
        member this.TryCreateSerializer(dotnetType) =
            if isClassWithDefaultConstructor dotnetType
            then Option.Some (createSerializer dotnetType)
            else Option.None

        member this.TryCreateDeserializer(dotnetType, schema) =
            if not (isClassWithDefaultConstructor dotnetType)
            then Option.None
            else
                match schema with
                | ValueSchema.Record recordSchema ->
                    tryCreateDeserializer dotnetType recordSchema
                | _ -> Option.None
