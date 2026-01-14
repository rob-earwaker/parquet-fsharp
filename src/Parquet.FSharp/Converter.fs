namespace rec Parquet.FSharp
#nowarn 40

open FSharp.Reflection
open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Reflection

type SerializationException(message) =
    inherit Exception(message)

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

type internal IValueConverter =
    abstract member TryCreateSerializer
        : sourceType:Type -> Serializer option
    abstract member TryCreateDeserializer
        : sourceSchema:ValueSchema * targetType:Type -> Deserializer option

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
    GetEnumerator: Expression -> Expression }

type internal FieldSerializer = {
    Name: string
    ValueSerializer: Serializer
    GetValue: Expression -> Expression }

type internal RecordSerializer = {
    DotnetType: Type
    Fields: FieldSerializer[] }

type internal OptionalSerializer = {
    DotnetType: Type
    ValueSerializer: Serializer
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
    ValueDeserializer: Deserializer }

type internal RecordDeserializer = {
    DotnetType: Type
    Fields: FieldDeserializer[]
    CreateFromFieldValues: Expression[] -> Expression }

type internal OptionalDeserializer = {
    DotnetType: Type
    ValueDeserializer: Deserializer
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
            DotnetType = dotnetType
            DataDotnetType = dataDotnetType
            GetDataValue = getDataValue }

    let record dotnetType fields =
        Serializer.Record {
            DotnetType = dotnetType
            Fields = fields }

    let list dotnetType elementSerializer getEnumerator =
        Serializer.List {
            DotnetType = dotnetType
            ElementSerializer = elementSerializer
            GetEnumerator = getEnumerator }

    let optional dotnetType valueSerializer isNull getValue =
        Serializer.Optional {
            DotnetType = dotnetType
            ValueSerializer = valueSerializer
            IsNull = isNull
            GetValue = getValue }

    let referenceTypeWrapper (valueSerializer: Serializer) =
        let dotnetType = valueSerializer.DotnetType
        let isNull = Expression.IsNull
        let getValue = id
        Serializer.optional dotnetType valueSerializer isNull getValue

    // TODO: This shouldn't really be necessary, but while Parquet.Net
    // treats all struct and list fields as optional it's necessary.
    let nonNullableReferenceTypeWrapper (valueSerializer: Serializer) =
        let dotnetType = valueSerializer.DotnetType
        let isNull = fun value -> Expression.False
        let getValue = id
        Serializer.optional dotnetType valueSerializer isNull getValue

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
            Serializer.getSchema' true optionalSerializer.ValueSerializer

    let getSchema serializer =
        Serializer.getSchema' false serializer

    let resolve (sourceType: Type) : Serializer =
        ValueConverter.All
        |> Array.tryPick _.TryCreateSerializer(sourceType)
        |> Option.defaultWith (fun () ->
            failwith $"unsupported type '{sourceType.FullName}'")

module internal Deserializer =
    let atomic dotnetType dataDotnetType createFromDataValue =
        Deserializer.Atomic {
            DotnetType = dotnetType
            DataDotnetType = dataDotnetType
            CreateFromDataValue = createFromDataValue }

    let record dotnetType fields createFromFieldValues =
        Deserializer.Record {
            DotnetType = dotnetType
            Fields = fields
            CreateFromFieldValues = createFromFieldValues }

    let list dotnetType elementDeserializer createEmpty createFromElementValues =
        Deserializer.List {
            DotnetType = dotnetType
            ElementDeserializer = elementDeserializer
            CreateEmpty = createEmpty
            CreateFromElementValues = createFromElementValues }

    let optional dotnetType valueDeserializer createNull createFromValue =
        Deserializer.Optional {
            DotnetType = dotnetType
            ValueDeserializer = valueDeserializer
            CreateNull = createNull
            CreateFromValue = createFromValue }

    let referenceTypeWrapper (valueDeserializer: Deserializer) =
        let dotnetType = valueDeserializer.DotnetType
        let createNull = Expression.Null(dotnetType)
        let createFromValue = id
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    // TODO: Is there a better name for this?
    // TODO: Should this require an attribute since it's somewhat unsafe?
    let nonNullableValueTypeWrapper (valueDeserializer: Deserializer) =
        let dotnetType = valueDeserializer.DotnetType
        let createNull =
            Expression.Block(
                Expression.FailWith<SerializationException>(
                    "null value encountered for non-nullable"
                    + $" type '{dotnetType.FullName}'"),
                Expression.Default(dotnetType))
            :> Expression
        let createFromValue = id
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    // TODO: This shouldn't really be necessary, but while Parquet.Net
    // treats all struct and list fields as optional it's necessary.
    let nonNullableReferenceTypeWrapper (valueDeserializer: Deserializer) =
        let dotnetType = valueDeserializer.DotnetType
        let createNull =
            Expression.Block(
                Expression.FailWith<SerializationException>(
                    $"null value encountered for type '{dotnetType.FullName}'"
                    + " which is not treated as nullable by default"),
                Expression.Default(dotnetType))
            :> Expression
        let createFromValue = id
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

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
            Deserializer.getSchema' true optionalDeserializer.ValueDeserializer

    let getSchema deserializer =
        Deserializer.getSchema' false deserializer

    let resolve sourceSchema targetType : Deserializer =
        ValueConverter.All
        |> Array.tryPick _.TryCreateDeserializer(sourceSchema, targetType)
        |> Option.defaultWith (fun () ->
            failwith <|
                "could not find converter to deserialize from schema"
                + $" '{sourceSchema}' to type '{targetType.FullName}'")

module private FieldSerializer =
    let create name valueSerializer getValue =
        { FieldSerializer.Name = name
          FieldSerializer.ValueSerializer = valueSerializer
          FieldSerializer.GetValue = getValue }

    let ofProperty (field: PropertyInfo) : FieldSerializer =
        let name = field.Name
        let valueSerializer = Serializer.resolve field.PropertyType
        let getValue (record: Expression) =
            Expression.Property(record, field)
            :> Expression
        create name valueSerializer getValue

    let ofUnionCaseField (unionCase: UnionCaseInfo) (field: PropertyInfo) =
        let name = field.Name
        let valueSerializer = Serializer.resolve field.PropertyType
        let getValue (union: Expression) =
            Expression.Property(
                Expression.Convert(union, unionCase.DotnetType), field)
            :> Expression
        create name valueSerializer getValue

    let getSchema (fieldSerializer: FieldSerializer) =
        { FieldSchema.Name = fieldSerializer.Name
          FieldSchema.Value = Serializer.getSchema fieldSerializer.ValueSerializer }

module private FieldDeserializer =
    let create name valueDeserializer =
        { FieldDeserializer.Name = name
          FieldDeserializer.ValueDeserializer = valueDeserializer }

    let ofProperty schema (field: PropertyInfo) : FieldDeserializer =
        let name = field.Name
        let deserializer = Deserializer.resolve schema field.PropertyType
        create name deserializer

    let getSchema (fieldDeserializer: FieldDeserializer) =
        { FieldSchema.Name = fieldDeserializer.Name
          FieldSchema.Value = Deserializer.getSchema fieldDeserializer.ValueDeserializer }

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
    // TODO: Rename all of these to Default*Converter?
    let All : IValueConverter[] = [|
        PrimitiveConverter<bool>()
        Int8Converter()
        Int16Converter()
        Int32Converter()
        Int64Converter()
        UInt8Converter()
        UInt16Converter()
        UInt32Converter()
        UInt64Converter()
        Float32Converter()
        Float64Converter()
        DecimalConverter()
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
        // This must come before the generic union type since option types are
        // handled in a special way.
        OptionConverter()
        NullableConverter()
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
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType = dotnetType
            then Option.Some deserializer
            else Option.None

type internal Int8Converter() =
    let dotnetType = typeof<int8>
    let dataDotnetType = dotnetType

    let serializer =
        let getDataValue = id
        Serializer.atomic dotnetType dataDotnetType getDataValue

    let requiredDeserializer =
        let createFromDataValue = id
        Deserializer.atomic dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.nonNullableValueTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.Atomic atomicSchema
                    when atomicSchema.DotnetType = dotnetType ->
                    if atomicSchema.IsOptional
                    then Option.Some optionalDeserializer
                    else Option.Some requiredDeserializer
                | _ -> Option.None

type internal Int16Converter() =
    let dotnetType = typeof<int16>

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
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.Atomic atomicSchema
                    when not atomicSchema.IsOptional
                        && (atomicSchema.DotnetType = dotnetType
                            || atomicSchema.DotnetType = typeof<int8>
                            || atomicSchema.DotnetType = typeof<uint8>) ->
                    Option.Some (createDeserializer atomicSchema.DotnetType)
                | _ -> Option.None

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
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.Atomic atomicSchema
                    when not atomicSchema.IsOptional
                        && (atomicSchema.DotnetType = dotnetType
                            || atomicSchema.DotnetType = typeof<int16>
                            || atomicSchema.DotnetType = typeof<int8>
                            || atomicSchema.DotnetType = typeof<uint16>
                            || atomicSchema.DotnetType = typeof<uint8>) ->
                    Option.Some (createDeserializer atomicSchema.DotnetType)
                | _ -> Option.None

type internal Int64Converter() =
    let dotnetType = typeof<int64>

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
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.Atomic atomicSchema
                    when not atomicSchema.IsOptional
                        && (atomicSchema.DotnetType = dotnetType
                            || atomicSchema.DotnetType = typeof<int32>
                            || atomicSchema.DotnetType = typeof<int16>
                            || atomicSchema.DotnetType = typeof<int8>
                            || atomicSchema.DotnetType = typeof<uint32>
                            || atomicSchema.DotnetType = typeof<uint16>
                            || atomicSchema.DotnetType = typeof<uint8>) ->
                    Option.Some (createDeserializer atomicSchema.DotnetType)
                | _ -> Option.None

type internal UInt8Converter() =
    let dotnetType = typeof<uint8>

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
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.Atomic atomicSchema
                    when not atomicSchema.IsOptional
                        && atomicSchema.DotnetType = dotnetType ->
                    Option.Some (createDeserializer atomicSchema.DotnetType)
                | _ -> Option.None

type internal UInt16Converter() =
    let dotnetType = typeof<uint16>

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
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.Atomic atomicSchema
                    when not atomicSchema.IsOptional
                        && (atomicSchema.DotnetType = dotnetType
                            || atomicSchema.DotnetType = typeof<uint8>) ->
                    Option.Some (createDeserializer atomicSchema.DotnetType)
                | _ -> Option.None

type internal UInt32Converter() =
    let dotnetType = typeof<uint32>

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
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.Atomic atomicSchema
                    when not atomicSchema.IsOptional
                        && (atomicSchema.DotnetType = dotnetType
                            || atomicSchema.DotnetType = typeof<uint16>
                            || atomicSchema.DotnetType = typeof<uint8>) ->
                    Option.Some (createDeserializer atomicSchema.DotnetType)
                | _ -> Option.None

type internal UInt64Converter() =
    let dotnetType = typeof<uint64>

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
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.Atomic atomicSchema
                    when not atomicSchema.IsOptional
                        && (atomicSchema.DotnetType = dotnetType
                            || atomicSchema.DotnetType = typeof<uint32>
                            || atomicSchema.DotnetType = typeof<uint16>
                            || atomicSchema.DotnetType = typeof<uint8>) ->
                    Option.Some (createDeserializer atomicSchema.DotnetType)
                | _ -> Option.None

type internal Float32Converter() =
    let dotnetType = typeof<float32>

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
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.Atomic atomicSchema
                    when not atomicSchema.IsOptional
                        && (atomicSchema.DotnetType = dotnetType
                            || atomicSchema.DotnetType = typeof<int16>
                            || atomicSchema.DotnetType = typeof<int8>
                            || atomicSchema.DotnetType = typeof<uint16>
                            || atomicSchema.DotnetType = typeof<uint8>) ->
                    Option.Some (createDeserializer atomicSchema.DotnetType)
                | _ -> Option.None

type internal Float64Converter() =
    let dotnetType = typeof<float>

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
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.Atomic atomicSchema
                    when not atomicSchema.IsOptional
                        && (atomicSchema.DotnetType = dotnetType
                            || atomicSchema.DotnetType = typeof<float32>
                            || atomicSchema.DotnetType = typeof<int32>
                            || atomicSchema.DotnetType = typeof<int16>
                            || atomicSchema.DotnetType = typeof<int8>
                            || atomicSchema.DotnetType = typeof<uint32>
                            || atomicSchema.DotnetType = typeof<uint16>
                            || atomicSchema.DotnetType = typeof<uint8>) ->
                    Option.Some (createDeserializer atomicSchema.DotnetType)
                | _ -> Option.None

type internal DecimalConverter() =
    let dotnetType = typeof<decimal>

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
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.Atomic atomicSchema
                    when not atomicSchema.IsOptional
                        && (atomicSchema.DotnetType = dotnetType
                            || atomicSchema.DotnetType = typeof<int64>
                            || atomicSchema.DotnetType = typeof<int32>
                            || atomicSchema.DotnetType = typeof<int16>
                            || atomicSchema.DotnetType = typeof<int8>
                            || atomicSchema.DotnetType = typeof<uint64>
                            || atomicSchema.DotnetType = typeof<uint32>
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
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType = dotnetType
            then Option.Some deserializer
            else Option.None

type internal StringConverter() =
    let dotnetType = typeof<string>
    let dataDotnetType = dotnetType

    let serializer =
        let getDataValue = id
        Serializer.atomic dotnetType dataDotnetType getDataValue
        |> Serializer.referenceTypeWrapper

    // Deserializer for required values, i.e. those that will never have null
    // values according to the source schema.
    let requiredDeserializer =
        let createFromDataValue = id
        Deserializer.atomic dotnetType dataDotnetType createFromDataValue

    // Deserializer for optional values, i.e. those that could have null values
    // according to the source schema. Since we usually don't want null values
    // in F#, we just wrap as a non-nullable type. This means an exception will
    // be thrown if a null value is encountered in the data.
    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.nonNullableReferenceTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema with
                // Only support atomic values with the correct type.
                | ValueSchema.Atomic atomicSchema
                    when atomicSchema.DotnetType = dotnetType ->
                    // Choose the right deserializer based on whether the values
                    // are optional.
                    if atomicSchema.IsOptional
                    then Option.Some optionalDeserializer
                    else Option.Some requiredDeserializer
                | _ -> Option.None

type internal ByteArrayConverter() =
    let dotnetType = typeof<byte[]>
    let dataDotnetType = dotnetType

    let serializer =
        let getDataValue = id
        Serializer.atomic dotnetType dataDotnetType getDataValue
        |> Serializer.referenceTypeWrapper

    // Deserializer for required values, i.e. those that will never have null
    // values according to the source schema.
    let requiredDeserializer =
        let createFromDataValue = id
        Deserializer.atomic dotnetType dataDotnetType createFromDataValue

    // Deserializer for optional values, i.e. those that could have null values
    // according to the source schema. Since we usually don't want null values
    // in F#, we just wrap as a non-nullable type. This means an exception will
    // be thrown if a null value is encountered in the data.
    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.nonNullableReferenceTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema with
                // Only support atomic values with the correct type.
                | ValueSchema.Atomic atomicSchema
                    when atomicSchema.DotnetType = dotnetType ->
                    // Choose the right deserializer based on whether the values
                    // are optional.
                    if atomicSchema.IsOptional
                    then Option.Some optionalDeserializer
                    else Option.Some requiredDeserializer
                | _ -> Option.None

type internal Array1dConverter() =
    let isArray1dType (dotnetType: Type) =
        dotnetType.IsArray
        && dotnetType.GetArrayRank() = 1

    let createSerializer (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetElementType()
        let elementSerializer = Serializer.resolve elementDotnetType
        let getEnumerator =
            let enumerableType =
                typedefof<IEnumerable<_>>.MakeGenericType(elementDotnetType)
            fun (array: Expression) ->
                let enumerable = Expression.Convert(array, enumerableType)
                Expression.Call(enumerable, "GetEnumerator", [])
        Serializer.list dotnetType elementSerializer getEnumerator
        |> Serializer.referenceTypeWrapper

    let createDeserializer (schema: ListSchema) (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetElementType()
        let elementDeserializer =
            Deserializer.resolve schema.Element elementDotnetType
        let createEmpty =
            Expression.NewArrayBounds(elementDotnetType, Expression.Constant(0))
        let createFromElementValues (elementValues: Expression) =
            Expression.Call(elementValues, "ToArray", [])
        Deserializer.list
            dotnetType elementDeserializer createEmpty createFromElementValues
        |> Deserializer.referenceTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if isArray1dType sourceType
            then Option.Some (createSerializer sourceType)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if not (isArray1dType targetType)
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.List listSchema ->
                    Option.Some (createDeserializer listSchema targetType)
                | _ -> Option.None

type internal GenericListConverter() =
    let isGenericListType = DotnetType.isGenericType<ResizeArray<_>>

    let createSerializer (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementSerializer = Serializer.resolve elementDotnetType
        let getEnumerator =
            let enumerableType =
                typedefof<IEnumerable<_>>.MakeGenericType(elementDotnetType)
            fun (list: Expression) ->
                let enumerable = Expression.Convert(list, enumerableType)
                Expression.Call(enumerable, "GetEnumerator", [])
        Serializer.list dotnetType elementSerializer getEnumerator
        |> Serializer.referenceTypeWrapper

    let createDeserializer (dotnetType: Type) (schema: ListSchema) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementDeserializer =
            Deserializer.resolve schema.Element elementDotnetType
        let createEmpty = Expression.New(dotnetType)
        let createFromElementValues = id
        Deserializer.list
            dotnetType elementDeserializer createEmpty createFromElementValues
        |> Deserializer.referenceTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if isGenericListType sourceType
            then Option.Some (createSerializer sourceType)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if not (isGenericListType targetType)
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.List listSchema ->
                    Option.Some (createDeserializer targetType listSchema)
                | _ -> Option.None

type internal FSharpListConverter() =
    let isFSharpListType = DotnetType.isGenericType<list<_>>

    let createSerializer (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementSerializer = Serializer.resolve elementDotnetType
        let getEnumerator =
            let enumerableType =
                typedefof<IEnumerable<_>>.MakeGenericType(elementDotnetType)
            fun (list: Expression) ->
                let enumerable = Expression.Convert(list, enumerableType)
                Expression.Call(enumerable, "GetEnumerator", [])
        // TODO: F# lists are not nullable by default, however Parquet.Net
        // does not support list fields that aren't nullable and a nullable
        // list field class can't easily be created since some of the relevant
        // properties are internal. For now, wrap as a non-nullable reference
        // type.
        Serializer.list dotnetType elementSerializer getEnumerator
        |> Serializer.nonNullableReferenceTypeWrapper

    let createDeserializer (dotnetType: Type) (schema: ListSchema) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementDeserializer =
            Deserializer.resolve schema.Element elementDotnetType
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
        member this.TryCreateSerializer(sourceType) =
            if isFSharpListType sourceType
            then Option.Some (createSerializer sourceType)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if not (isFSharpListType targetType)
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.List listSchema ->
                    Option.Some (createDeserializer targetType listSchema)
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
                |> Option.map (fun fieldSchema ->
                    FieldDeserializer.ofProperty fieldSchema.Value field))
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
        member this.TryCreateSerializer(sourceType) =
            if isFSharpRecordType sourceType
            then Option.Some (createSerializer sourceType)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if not (isFSharpRecordType targetType)
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.Record recordSchema ->
                    tryCreateDeserializer targetType recordSchema
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
        let valueSerializer =
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
        Serializer.optional dotnetType valueSerializer isNull getValue

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
                // TODO: We should really delegate this to Serializer.ofType,
                // but this would require having a string converter that
                // can treat strings as required.
                let dotnetType = typeof<string>
                let dataDotnetType = dotnetType
                let getDataValue = id
                Serializer.atomic dotnetType dataDotnetType getDataValue
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
                    |> Option.map (fun fieldSchema ->
                        FieldDeserializer.ofProperty fieldSchema.Value field))
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
        member this.TryCreateSerializer(sourceType) =
            if isUnionType sourceType
            then Option.Some (createUnionSerializer sourceType)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if not (isUnionType targetType)
            then Option.None
            else
                // Unions are represented in one of two ways depending on
                // whether any of the cases have associated data fields.
                let unionInfo = UnionInfo.ofUnion targetType
                if unionInfo.UnionCases
                    |> Array.forall (fun unionCase -> unionCase.Fields.Length = 0)
                then Option.Some (createSimpleUnionDeserializer unionInfo)
                else
                    match sourceSchema with
                    | ValueSchema.Record recordSchema ->
                        tryCreateComplexUnionDeserializer unionInfo recordSchema
                    | _ -> Option.None

type internal OptionConverter() =
    let isOptionType = DotnetType.isGenericType<option<_>>

    let createRequiredValueSerializer dotnetType (valueSerializer: Serializer) =
        let isNull =
            let optionModuleType =
                Assembly.Load("FSharp.Core").GetTypes()
                |> Array.filter (fun type' -> type'.Name = "OptionModule")
                |> Array.exactlyOne
            fun (option: Expression) ->
                Expression.Call(
                    optionModuleType, "IsNone", [| valueSerializer.DotnetType |], option)
                :> Expression
        let getValue (option: Expression) =
            Expression.Property(option, "Value")
            :> Expression
        Serializer.optional dotnetType valueSerializer isNull getValue

    let createOptionalValueSerializer (dotnetType: Type) (valueSerializer: Serializer) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                let getValue = id
                FieldSerializer.create name valueSerializer getValue
            [| valueField |]
        Serializer.record valueSerializer.DotnetType fields
        |> createRequiredValueSerializer dotnetType

    let createSerializer (dotnetType: Type) =
        let valueDotnetType = dotnetType.GetGenericArguments()[0]
        let valueSerializer = Serializer.resolve valueDotnetType
        if valueSerializer.IsOptional
        then createOptionalValueSerializer dotnetType valueSerializer
        else createRequiredValueSerializer dotnetType valueSerializer

    let createRequiredValueDeserializer dotnetType (valueDeserializer: Deserializer) =
        // TODO: Can we just use UnionInfo for this?
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
            dotnetType valueDeserializer createNull createFromValue

    let createOptionalValueDeserializer (dotnetType: Type) (valueDeserializer: Deserializer) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                FieldDeserializer.create name valueDeserializer
            [| valueField |]
        let createFromFieldValues (fieldValues: Expression[]) =
            fieldValues[0]
        Deserializer.record valueDeserializer.DotnetType fields createFromFieldValues
        |> createRequiredValueDeserializer dotnetType

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if isOptionType sourceType
            then Option.Some (createSerializer sourceType)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if not (isOptionType targetType)
            then Option.None
            else
                let isValueOptional, valueSchema =
                    match sourceSchema with
                    | ValueSchema.Record recordSchema ->
                        // TODO: This seems a bit hacky!
                        if recordSchema.Fields.Length = 1
                            && recordSchema.Fields[0].Name = "Value"
                        then true, recordSchema.Fields[0].Value
                        else false, sourceSchema.MakeRequired()
                    | _ -> false, sourceSchema.MakeRequired()
                let valueDotnetType = targetType.GetGenericArguments()[0]
                let valueDeserializer =
                    Deserializer.resolve valueSchema valueDotnetType
                if isValueOptional
                then Option.Some (createOptionalValueDeserializer targetType valueDeserializer)
                else Option.Some (createRequiredValueDeserializer targetType valueDeserializer)

type internal NullableConverter() =
    let isNullableType = DotnetType.isGenericType<Nullable<_>>

    let createRequiredValueSerializer dotnetType (valueSerializer: Serializer) =
        let isNull (nullable: Expression) =
            Expression.Not(Expression.Property(nullable, "HasValue"))
            :> Expression
        let getValue (nullable: Expression) =
            Expression.Property(nullable, "Value")
            :> Expression
        Serializer.optional dotnetType valueSerializer isNull getValue

    let createOptionalValueSerializer (dotnetType: Type) (valueSerializer: Serializer) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                let getValue = id
                FieldSerializer.create name valueSerializer getValue
            [| valueField |]
        Serializer.record valueSerializer.DotnetType fields
        |> createRequiredValueSerializer dotnetType

    let createSerializer (dotnetType: Type) =
        let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
        let valueSerializer = Serializer.resolve valueDotnetType
        if valueSerializer.IsOptional
        // TODO: This shouldn't really be possible as nullable values have to be
        // value types and therefore can't be null, but while we need to treat
        // all record types as optional we need to handle this case.
        then createOptionalValueSerializer dotnetType valueSerializer
        else createRequiredValueSerializer dotnetType valueSerializer

    let createRequiredValueDeserializer dotnetType (valueDeserializer: Deserializer) =
        let createNull = Expression.Null(dotnetType)
        let createFromValue =
            let constructor = dotnetType.GetConstructor([| valueDeserializer.DotnetType |])
            fun (value: Expression) ->
                Expression.New(constructor, value)
                :> Expression
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    let createOptionalValueDeserializer (dotnetType: Type) (valueDeserializer: Deserializer) =
        let fields =
            let valueField =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Value"
                FieldDeserializer.create name valueDeserializer
            [| valueField |]
        let createFromFieldValues (fieldValues: Expression[]) =
            fieldValues[0]
        Deserializer.record valueDeserializer.DotnetType fields createFromFieldValues
        |> createRequiredValueDeserializer dotnetType

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if isNullableType sourceType
            then Option.Some (createSerializer sourceType)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if not (isNullableType targetType)
            then Option.None
            else
                let isValueOptional, valueSchema =
                    match sourceSchema with
                    | ValueSchema.Record recordSchema ->
                        // TODO: This seems a bit hacky!
                        if recordSchema.Fields.Length = 1
                            && recordSchema.Fields[0].Name = "Value"
                        then true, recordSchema.Fields[0].Value
                        else false, sourceSchema.MakeRequired()
                    | _ -> false, sourceSchema.MakeRequired()
                let valueDotnetType = Nullable.GetUnderlyingType(targetType)
                let valueDeserializer =
                    Deserializer.resolve valueSchema valueDotnetType
                if isValueOptional
                // TODO: This shouldn't really be possible as nullable values have to be
                // value types and therefore can't be null, but while we need to treat
                // all record types as optional we need to handle this case.
                then Option.Some (createOptionalValueDeserializer targetType valueDeserializer)
                else Option.Some (createRequiredValueDeserializer targetType valueDeserializer)

type internal ClassConverter() =
    let isClassWithDefaultConstructor (dotnetType: Type) =
        dotnetType.IsClass
        && not (isNull (dotnetType.GetConstructor([||])))

    let getProperties (dotnetType: Type) =
        dotnetType.GetProperties(BindingFlags.Instance ||| BindingFlags.Public)

    let createSerializer (dotnetType: Type) =
        let fieldSerializers =
            getProperties dotnetType
            |> Array.map FieldSerializer.ofProperty
        Serializer.record dotnetType fieldSerializers
        |> Serializer.referenceTypeWrapper

    let tryCreateDeserializer (dotnetType: Type) (recordSchema: RecordSchema) =
        let properties = getProperties dotnetType
        let fieldDeserializers =
            properties
            |> Array.choose (fun field ->
                recordSchema.Fields
                |> Array.tryFind (fun fieldSchema -> fieldSchema.Name = field.Name)
                |> Option.map (fun fieldSchema ->
                    FieldDeserializer.ofProperty fieldSchema.Value field))
        if fieldDeserializers.Length < properties.Length
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
                            yield! Array.zip properties fieldValues
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
        member this.TryCreateSerializer(sourceType) =
            if isClassWithDefaultConstructor sourceType
            then Option.Some (createSerializer sourceType)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if not (isClassWithDefaultConstructor targetType)
            then Option.None
            else
                match sourceSchema with
                | ValueSchema.Record recordSchema ->
                    tryCreateDeserializer targetType recordSchema
                | _ -> Option.None
