namespace rec Parquet.FSharp
#nowarn 40

open FSharp.Reflection
open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Reflection

type SerializationException(message) =
    inherit Exception(message)

// TODO: Attribute ideas:
//   - ParquetAttribute (base class)
//
//   - ParquetField(name: string, required: bool, optional: bool, allowNulls: bool)
//   - ParquetDecimalField(<inherited>, scale: int, precision: int)
//   - ParquetDateTimeField(<inherited>, isAdjustedToUtc: bool, unit: <enum TimeUnit>)
//   - ParquetDateTimeOffsetField(<inherited>, unit: <enum TimeUnit>)
//   - ParquetUnionField(<inherited>, simple: bool, caseTypeFieldName: string)

//   - ParquetUnion(caseTypeFieldName: string)
//   - ParquetUnionCase(typeName: string, dataFieldName: string)
//   - ParquetRequired()
//   - ParquetOptional(allowNulls: bool)

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

// TODO: Add converter type to serializer/deserializer to we can catch exceptions
// that occur when calling the compiled lambda functions and enrich with info about
// which converter they originated from and which lambda function they originated from.

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
    member this.Schema =
        match this with
        | Serializer.Atomic atomicSerializer -> atomicSerializer.Schema
        | Serializer.List listSerializer -> listSerializer.Schema
        | Serializer.Record recordSerializer -> recordSerializer.Schema
        | Serializer.Optional optionalSerializer -> optionalSerializer.Schema

    member this.DotnetType =
        match this with
        | Serializer.Atomic atomicSerializer -> atomicSerializer.DotnetType
        | Serializer.List listSerializer -> listSerializer.DotnetType
        | Serializer.Record recordSerializer -> recordSerializer.DotnetType
        | Serializer.Optional optionalSerializer -> optionalSerializer.DotnetType

type internal AtomicSerializer = {
    Schema: ValueSchema
    DotnetType: Type
    DataDotnetType: Type
    GetDataValue: Expression -> Expression }

type internal ListSerializer = {
    Schema: ValueSchema
    DotnetType: Type
    ElementSerializer: Serializer
    GetEnumerator: Expression -> Expression }

type internal FieldSerializer = {
    Schema: FieldSchema
    Name: string
    ValueSerializer: Serializer
    GetValue: Expression -> Expression }

type internal RecordSerializer = {
    Schema: ValueSchema
    DotnetType: Type
    FieldSerializers: FieldSerializer[] }

type internal OptionalSerializer = {
    Schema: ValueSchema
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
    member this.Schema =
        match this with
        | Deserializer.Atomic atomicDeserializer -> atomicDeserializer.Schema
        | Deserializer.List listDeserializer -> listDeserializer.Schema
        | Deserializer.Record recordDeserializer -> recordDeserializer.Schema
        | Deserializer.Optional optionalDeserializer -> optionalDeserializer.Schema

    member this.DotnetType =
        match this with
        | Deserializer.Atomic atomicDeserializer -> atomicDeserializer.DotnetType
        | Deserializer.List listDeserializer -> listDeserializer.DotnetType
        | Deserializer.Record recordDeserializer -> recordDeserializer.DotnetType
        | Deserializer.Optional optionalDeserializer -> optionalDeserializer.DotnetType

type internal AtomicDeserializer = {
    Schema: ValueSchema
    DotnetType: Type
    DataDotnetType: Type
    CreateFromDataValue: Expression -> Expression }

type internal ListDeserializer = {
    Schema: ValueSchema
    DotnetType: Type
    ElementDeserializer: Deserializer
    CreateEmpty: Expression
    CreateFromElementValues: Expression -> Expression }

type internal FieldDeserializer = {
    Schema: FieldSchema
    Name: string
    ValueDeserializer: Deserializer }

type internal RecordDeserializer = {
    Schema: ValueSchema
    DotnetType: Type
    FieldDeserializers: FieldDeserializer[]
    CreateFromFieldValues: Expression[] -> Expression }

type internal OptionalDeserializer = {
    Schema: ValueSchema
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

module internal Serializer =
    let atomic schema dotnetType dataDotnetType getDataValue =
        let schema =
            let isOptional = false
            ValueSchema.create isOptional schema
        Serializer.Atomic {
            Schema = schema
            DotnetType = dotnetType
            DataDotnetType = dataDotnetType
            GetDataValue = getDataValue }

    let record dotnetType (fieldSerializers: FieldSerializer[]) =
        let schema =
            let isOptional = false
            let valueType =
                fieldSerializers
                |> Array.map (fun fieldSerailizer -> fieldSerailizer.Schema)
                |> ValueTypeSchema.record
            ValueSchema.create isOptional valueType
        Serializer.Record {
            Schema = schema
            DotnetType = dotnetType
            FieldSerializers = fieldSerializers }

    let list dotnetType (elementSerializer: Serializer) getEnumerator =
        let schema =
            let isOptional = false
            let valueType = ValueTypeSchema.list elementSerializer.Schema
            ValueSchema.create isOptional valueType
        Serializer.List {
            Schema = schema
            DotnetType = dotnetType
            ElementSerializer = elementSerializer
            GetEnumerator = getEnumerator }

    let optional dotnetType (valueSerializer: Serializer) isNull getValue =
        Serializer.Optional {
            Schema = valueSerializer.Schema.MakeOptional()
            DotnetType = dotnetType
            ValueSerializer = valueSerializer
            IsNull = isNull
            GetValue = getValue }

    let throwIfNull (value: Expression) =
        // if isNull value then
        //     raise SerializationException(...)
        Expression.IfThen(
            Expression.IsNull(value),
            Expression.FailWith<SerializationException>(
                "null value encountered during serialization for type"
                + $" '{value.Type.FullName}' which is not treated as nullable"
                + " by default"))
        :> Expression

    let referenceTypeWrapper (valueSerializer: Serializer) =
        let dotnetType = valueSerializer.DotnetType
        let isNull = Expression.IsNull
        let getValue = id
        Serializer.optional dotnetType valueSerializer isNull getValue

    let nonNullableReferenceTypeWrapper (valueSerializer: Serializer) =
        let dotnetType = valueSerializer.DotnetType
        let isNull = fun value -> Expression.False
        let getValue = id
        Serializer.optional dotnetType valueSerializer isNull getValue

    let resolve (sourceType: Type) : Serializer =
        ValueConverter.All
        |> Array.tryPick _.TryCreateSerializer(sourceType)
        |> Option.defaultWith (fun () ->
            // TODO: This will likely end up depending on attributes as well,
            // so probably will want to make the exception more generic to
            // avoid confusion if there is a converter registered to support the
            // specified type.
            failwith $"unsupported type '{sourceType.FullName}'")

module internal Deserializer =
    let atomic schema dotnetType dataDotnetType createFromDataValue =
        let schema =
            let isOptional = false
            ValueSchema.create isOptional schema
        Deserializer.Atomic {
            Schema = schema
            DotnetType = dotnetType
            DataDotnetType = dataDotnetType
            CreateFromDataValue = createFromDataValue }

    let record dotnetType (fieldDeserializers: FieldDeserializer[]) createFromFieldValues =
        let schema =
            let isOptional = false
            let valueType =
                fieldDeserializers
                |> Array.map (fun fieldDeserializer -> fieldDeserializer.Schema)
                |> ValueTypeSchema.record
            ValueSchema.create isOptional valueType
        Deserializer.Record {
            Schema = schema
            DotnetType = dotnetType
            FieldDeserializers = fieldDeserializers
            CreateFromFieldValues = createFromFieldValues }

    let list dotnetType (elementDeserializer: Deserializer) createEmpty createFromElementValues =
        let schema =
            let isOptional = false
            let valueType = ValueTypeSchema.list elementDeserializer.Schema
            ValueSchema.create isOptional valueType
        Deserializer.List {
            Schema = schema
            DotnetType = dotnetType
            ElementDeserializer = elementDeserializer
            CreateEmpty = createEmpty
            CreateFromElementValues = createFromElementValues }

    let optional dotnetType (valueDeserializer: Deserializer) createNull createFromValue =
        Deserializer.Optional {
            Schema = valueDeserializer.Schema.MakeOptional()
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

    let optionalNonNullableTypeWrapper (valueDeserializer: Deserializer) =
        let dotnetType = valueDeserializer.DotnetType
        let createNull =
            Expression.Block(
                Expression.FailWith<SerializationException>(
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{dotnetType.FullName}'"),
                Expression.Default(dotnetType))
            :> Expression
        let createFromValue = id
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    let optionalNullableTypeWrapper (valueDeserializer: Deserializer) =
        let dotnetType = valueDeserializer.DotnetType
        let createNull =
            Expression.Block(
                Expression.FailWith<SerializationException>(
                    "null value encountered during deserialization for type"
                    + $" '{dotnetType.FullName}' which is not treated as"
                    + " nullable by default"),
                Expression.Default(dotnetType))
            :> Expression
        let createFromValue = id
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    let resolve sourceSchema targetType : Deserializer =
        ValueConverter.All
        |> Array.tryPick _.TryCreateDeserializer(sourceSchema, targetType)
        |> Option.defaultWith (fun () ->
            // TODO: This will likely end up depending on attributes as well,
            // so probably will want to make the exception more generic to
            // avoid confusion if there is a converter registered to support the
            // specified type.
            failwith <|
                "could not find converter to deserialize from schema"
                + $" '{sourceSchema}' to type '{targetType.FullName}'")

module private FieldSerializer =
    let create name (valueSerializer: Serializer) getValue =
        let schema = FieldSchema.create name valueSerializer.Schema
        { FieldSerializer.Schema = schema
          FieldSerializer.Name = name
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

module private FieldDeserializer =
    let create name (valueDeserializer: Deserializer) =
        let schema = FieldSchema.create name valueDeserializer.Schema
        { FieldDeserializer.Schema = schema
          FieldDeserializer.Name = name
          FieldDeserializer.ValueDeserializer = valueDeserializer }

    let ofProperty schema (field: PropertyInfo) : FieldDeserializer =
        let name = field.Name
        let deserializer = Deserializer.resolve schema field.PropertyType
        create name deserializer

module internal ValueConverter =
    let All : IValueConverter[] = [|
        // TODO: These should probably be singletons.
        DefaultBoolConverter()
        DefaultInt8Converter()
        DefaultInt16Converter()
        DefaultInt32Converter()
        DefaultInt64Converter()
        DefaultUInt8Converter()
        DefaultUInt16Converter()
        DefaultUInt32Converter()
        DefaultUInt64Converter()
        DefaultFloat32Converter()
        DefaultFloat64Converter()
        DefaultDecimalConverter()
        DefaultGuidConverter()
        DefaultDateTimeConverter()
        DefaultDateTimeOffsetConverter()
        DefaultStringConverter()
        // This must come before the generic array type since byte arrays are
        // supported as a primitive type in Parquet and are therefore handled as
        // atomic values rather than lists.
        DefaultByteArrayConverter()
        DefaultListConverter()
        DefaultArray1dConverter()
        DefaultResizeArrayConverter()
        DefaultRecordConverter()
        // This must come before the generic union type since option types are
        // handled in a special way.
        DefaultOptionConverter()
        DefaultNullableConverter()
        DefaultUnionConverter()
        DefaultClassConverter()
    |]

type internal DefaultBoolConverter() =
    let dotnetType = typeof<bool>
    let dataDotnetType = dotnetType

    let serializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let requiredDeserializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue = id
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType ->
                    if sourceSchema.IsOptional
                    then Option.Some optionalDeserializer
                    else Option.Some requiredDeserializer
                | _ -> Option.None

type internal DefaultInt8Converter() =
    let dotnetType = typeof<int8>
    let dataDotnetType = dotnetType

    let serializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let requiredDeserializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue = id
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType ->
                    if sourceSchema.IsOptional
                    then Option.Some optionalDeserializer
                    else Option.Some requiredDeserializer
                | _ -> Option.None

type internal DefaultInt16Converter() =
    let dotnetType = typeof<int16>

    // TODO: Could support serializing as smaller integer type using an attribute.
    let serializer =
        let dataDotnetType = dotnetType
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let createRequiredDeserializer dataDotnetType =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue (dataValue: Expression) =
            if dataDotnetType = dotnetType
            then dataValue
            else Expression.Convert(dataValue, dotnetType)
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let createOptionalDeserializer dataDotnetType =
        createRequiredDeserializer dataDotnetType
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType
                        || primitiveSchema.DataDotnetType = typeof<int8>
                        || primitiveSchema.DataDotnetType = typeof<uint8> ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer primitiveSchema.DataDotnetType)
                    else Option.Some (createRequiredDeserializer primitiveSchema.DataDotnetType)
                | _ -> Option.None

type internal DefaultInt32Converter() =
    let dotnetType = typeof<int32>

    let serializer =
        let dataDotnetType = dotnetType
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let createRequiredDeserializer dataDotnetType =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue (dataValue: Expression) =
            if dataDotnetType = dotnetType
            then dataValue
            else Expression.Convert(dataValue, dotnetType)
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let createOptionalDeserializer dataDotnetType =
        createRequiredDeserializer dataDotnetType
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType
                        || primitiveSchema.DataDotnetType = typeof<int16>
                        || primitiveSchema.DataDotnetType = typeof<int8>
                        || primitiveSchema.DataDotnetType = typeof<uint16>
                        || primitiveSchema.DataDotnetType = typeof<uint8> ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer primitiveSchema.DataDotnetType)
                    else Option.Some (createRequiredDeserializer primitiveSchema.DataDotnetType)
                | _ -> Option.None

type internal DefaultInt64Converter() =
    let dotnetType = typeof<int64>

    let serializer =
        let dataDotnetType = dotnetType
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let createRequiredDeserializer dataDotnetType =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue (dataValue: Expression) =
            if dataDotnetType = dotnetType
            then dataValue
            else Expression.Convert(dataValue, dotnetType)
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let createOptionalDeserializer dataDotnetType =
        createRequiredDeserializer dataDotnetType
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType
                        || primitiveSchema.DataDotnetType = typeof<int32>
                        || primitiveSchema.DataDotnetType = typeof<int16>
                        || primitiveSchema.DataDotnetType = typeof<int8>
                        || primitiveSchema.DataDotnetType = typeof<uint32>
                        || primitiveSchema.DataDotnetType = typeof<uint16>
                        || primitiveSchema.DataDotnetType = typeof<uint8> ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer primitiveSchema.DataDotnetType)
                    else Option.Some (createRequiredDeserializer primitiveSchema.DataDotnetType)
                | _ -> Option.None

type internal DefaultUInt8Converter() =
    let dotnetType = typeof<uint8>
    let dataDotnetType = dotnetType
    
    let serializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let requiredDeserializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue = id
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType ->
                    if sourceSchema.IsOptional
                    then Option.Some optionalDeserializer
                    else Option.Some requiredDeserializer
                | _ -> Option.None

type internal DefaultUInt16Converter() =
    let dotnetType = typeof<uint16>

    let serializer =
        let dataDotnetType = dotnetType
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let createRequiredDeserializer dataDotnetType =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue (dataValue: Expression) =
            if dataDotnetType = dotnetType
            then dataValue
            else Expression.Convert(dataValue, dotnetType)
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let createOptionalDeserializer dataDotnetType =
        createRequiredDeserializer dataDotnetType
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType
                        || primitiveSchema.DataDotnetType = typeof<uint8> ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer primitiveSchema.DataDotnetType)
                    else Option.Some (createRequiredDeserializer primitiveSchema.DataDotnetType)
                | _ -> Option.None

type internal DefaultUInt32Converter() =
    let dotnetType = typeof<uint32>

    let serializer =
        let dataDotnetType = dotnetType
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let createRequiredDeserializer dataDotnetType =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue (dataValue: Expression) =
            if dataDotnetType = dotnetType
            then dataValue
            else Expression.Convert(dataValue, dotnetType)
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let createOptionalDeserializer dataDotnetType =
        createRequiredDeserializer dataDotnetType
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType
                        || primitiveSchema.DataDotnetType = typeof<uint16>
                        || primitiveSchema.DataDotnetType = typeof<uint8> ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer primitiveSchema.DataDotnetType)
                    else Option.Some (createRequiredDeserializer primitiveSchema.DataDotnetType)
                | _ -> Option.None

type internal DefaultUInt64Converter() =
    let dotnetType = typeof<uint64>

    let serializer =
        let dataDotnetType = dotnetType
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let createRequiredDeserializer dataDotnetType =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue (dataValue: Expression) =
            if dataDotnetType = dotnetType
            then dataValue
            else Expression.Convert(dataValue, dotnetType)
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let createOptionalDeserializer dataDotnetType =
        createRequiredDeserializer dataDotnetType
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType
                        || primitiveSchema.DataDotnetType = typeof<uint32>
                        || primitiveSchema.DataDotnetType = typeof<uint16>
                        || primitiveSchema.DataDotnetType = typeof<uint8> ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer primitiveSchema.DataDotnetType)
                    else Option.Some (createRequiredDeserializer primitiveSchema.DataDotnetType)
                | _ -> Option.None

type internal DefaultFloat32Converter() =
    let dotnetType = typeof<float32>

    let serializer =
        let dataDotnetType = dotnetType
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let createRequiredDeserializer dataDotnetType =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue (dataValue: Expression) =
            if dataDotnetType = dotnetType
            then dataValue
            else Expression.Convert(dataValue, dotnetType)
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let createOptionalDeserializer dataDotnetType =
        createRequiredDeserializer dataDotnetType
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType
                        || primitiveSchema.DataDotnetType = typeof<int16>
                        || primitiveSchema.DataDotnetType = typeof<int8>
                        || primitiveSchema.DataDotnetType = typeof<uint16>
                        || primitiveSchema.DataDotnetType = typeof<uint8> ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer primitiveSchema.DataDotnetType)
                    else Option.Some (createRequiredDeserializer primitiveSchema.DataDotnetType)
                | _ -> Option.None

type internal DefaultFloat64Converter() =
    let dotnetType = typeof<float>

    let serializer =
        let dataDotnetType = dotnetType
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let createRequiredDeserializer dataDotnetType =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue (dataValue: Expression) =
            if dataDotnetType = dotnetType
            then dataValue
            else Expression.Convert(dataValue, dotnetType)
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let createOptionalDeserializer dataDotnetType =
        createRequiredDeserializer dataDotnetType
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType
                        || primitiveSchema.DataDotnetType = typeof<float32>
                        || primitiveSchema.DataDotnetType = typeof<int32>
                        || primitiveSchema.DataDotnetType = typeof<int16>
                        || primitiveSchema.DataDotnetType = typeof<int8>
                        || primitiveSchema.DataDotnetType = typeof<uint32>
                        || primitiveSchema.DataDotnetType = typeof<uint16>
                        || primitiveSchema.DataDotnetType = typeof<uint8> ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer primitiveSchema.DataDotnetType)
                    else Option.Some (createRequiredDeserializer primitiveSchema.DataDotnetType)
                | _ -> Option.None

type internal DefaultDecimalConverter() =
    let dotnetType = typeof<decimal>

    let serializer =
        let dataDotnetType = dotnetType
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let createRequiredDeserializer dataDotnetType =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue (dataValue: Expression) =
            if dataDotnetType = dotnetType
            then dataValue
            else Expression.Convert(dataValue, dotnetType)
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let createOptionalDeserializer dataDotnetType =
        createRequiredDeserializer dataDotnetType
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType
                        || primitiveSchema.DataDotnetType = typeof<int64>
                        || primitiveSchema.DataDotnetType = typeof<int32>
                        || primitiveSchema.DataDotnetType = typeof<int16>
                        || primitiveSchema.DataDotnetType = typeof<int8>
                        || primitiveSchema.DataDotnetType = typeof<uint64>
                        || primitiveSchema.DataDotnetType = typeof<uint32>
                        || primitiveSchema.DataDotnetType = typeof<uint16>
                        || primitiveSchema.DataDotnetType = typeof<uint8> ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer primitiveSchema.DataDotnetType)
                    else Option.Some (createRequiredDeserializer primitiveSchema.DataDotnetType)
                | _ -> Option.None

type internal DefaultGuidConverter() =
    let dotnetType = typeof<Guid>
    let dataDotnetType = dotnetType

    let serializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let requiredDeserializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue = id
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType ->
                    if sourceSchema.IsOptional
                    then Option.Some optionalDeserializer
                    else Option.Some requiredDeserializer
                | _ -> Option.None

// TODO: Support other TimestampTypes from Parquet.Net
// TODO: Handle UTC vs Local for both serialization and deserialization.
// ---
// Parquet.Net behaviour:
//
// DateTime (no attribute)
//   => INT96
//   => serialization ignores Kind, no truncation
//   => deserialization assumes UTC
//
// DateTime [ParquetTimestamp(<resolution>, logical=false, <utc-adjusted-ignored>>)]
//   => INT64, TIMESTAMP_<resolution>, (no logical type)
//   => serialization ignores Kind, truncates to <resolution>
//   => deserialization assumes UTC

// DateTime [ParquetTimestamp(<resolution>, logical=true, utcAdjusted=true)]
//   => INT64, (no converted type), TIMESTAMP(unit: <resolution>, isAdjustedToUtc: true)
//   => serialization ignores Kind, truncates to <resolution>
//   => deserialization assumes UTC

// DateTime [ParquetTimestamp(<resolution>, logical=true, utcAdjusted=false)]
//   => INT64, (no converted type), TIMESTAMP(unit: <resolution>, isAdjustedToUtc: false)
//   => serialization ignores Kind, truncates to <resolution>
//   => deserialization assumes Local

type internal DefaultDateTimeConverter() =
    let dotnetType = typeof<DateTime>
    let dataDotnetType = dotnetType

    let serializer =
        let schema =
            let isAdjustedToUtc = true
            ValueTypeSchema.dateTime isAdjustedToUtc
        let getDataValue (dateTime: Expression) =
            // if dateTime.Kind <> DateTimeKind.Utc then
            //     raise SerializationException(...)
            // dateTime
            let kind = Expression.Property(dateTime, "Kind")
            Expression.Block(
                Expression.IfThen(
                    Expression.NotEqual(kind, Expression.Constant(DateTimeKind.Utc)),
                    Expression.FailWith<SerializationException>(
                        Expression.Constant(
                            "encountered 'DateTime' with 'DateTimeKind."),
                        Expression.Call(kind, "ToString", []),
                        Expression.Constant(
                            "' during serialization of timestamp with instant"
                            + " semantics which only allows 'DateTimeKind.Utc'"
                            + " by default"))),
                dateTime)
            :> Expression
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let requiredDeserializer =
        let schema =
            let isAdjustedToUtc = true
            ValueTypeSchema.dateTime isAdjustedToUtc
        let createFromDataValue = id
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.DateTime dateTimeSchema
                    when dateTimeSchema.IsAdjustedToUtc ->
                    if sourceSchema.IsOptional
                    then Option.Some optionalDeserializer
                    else Option.Some requiredDeserializer
                | _ -> Option.None

// TODO: Handle UTC vs Local for both serialization and deserialization.
type internal DefaultDateTimeOffsetConverter() =
    let dotnetType = typeof<DateTimeOffset>
    let dataDotnetType = typeof<DateTime>

    let serializer =
        let schema =
            let isAdjustedToUtc = true
            ValueTypeSchema.dateTime isAdjustedToUtc
        let getDataValue (value: Expression) =
            Expression.Property(value, "UtcDateTime")
            :> Expression
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let requiredDeserializer =
        let schema =
            let isAdjustedToUtc = true
            ValueTypeSchema.dateTime isAdjustedToUtc
        let createFromDataValue (dateTime: Expression) =
            Expression.New(
                typeof<DateTimeOffset>.GetConstructor([| typeof<DateTime> |]),
                dateTime)
            :> Expression
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.DateTime dateTimeSchema
                    when dateTimeSchema.IsAdjustedToUtc ->
                    if sourceSchema.IsOptional
                    then Option.Some optionalDeserializer
                    else Option.Some requiredDeserializer
                | _ -> Option.None

type internal DefaultStringConverter() =
    let dotnetType = typeof<string>
    let dataDotnetType = dotnetType

    let serializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue (value: Expression) =
            Expression.Block(
                Serializer.throwIfNull value,
                value)
            :> Expression
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    // Deserializer for required values, i.e. those that will never have null
    // values according to the source schema.
    let requiredDeserializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue = id
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    // Deserializer for optional values, i.e. those that could have null values
    // according to the source schema. Since we usually don't want null values
    // in F#, we just wrap as a non-nullable type. This means an exception will
    // be thrown if a null value is encountered in the data.
    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                // Only support atomic values with the correct type.
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType ->
                    // Choose the right deserializer based on whether the values
                    // are optional.
                    if sourceSchema.IsOptional
                    then Option.Some optionalDeserializer
                    else Option.Some requiredDeserializer
                | _ -> Option.None

type internal DefaultByteArrayConverter() =
    let dotnetType = typeof<byte[]>
    let dataDotnetType = dotnetType

    let serializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue (value: Expression) =
            Expression.Block(
                Serializer.throwIfNull value,
                value)
            :> Expression
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    // Deserializer for required values, i.e. those that will never have null
    // values according to the source schema.
    let requiredDeserializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue = id
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    // Deserializer for optional values, i.e. those that could have null values
    // according to the source schema. Since we usually don't want null values
    // in F#, we just wrap as a non-nullable type. This means an exception will
    // be thrown if a null value is encountered in the data.
    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                // Only support atomic values with the correct type.
                | ValueTypeSchema.Primitive primitiveSchema
                    // TODO: Support reading binary-backed types, e.g. Guid, string?
                    when primitiveSchema.DataDotnetType = dotnetType ->
                    // Choose the right deserializer based on whether the values
                    // are optional.
                    if sourceSchema.IsOptional
                    then Option.Some optionalDeserializer
                    else Option.Some requiredDeserializer
                | _ -> Option.None

type internal DefaultListConverter() =
    let isListType = DotnetType.isGenericType<list<_>>

    let createSerializer (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementSerializer = Serializer.resolve elementDotnetType
        let getEnumerator (list: Expression) =
            // let enumerable = list :> IEnumerable<'Element>
            // enumerable.GetEnumerator()
            let enumerable =
                Expression.Variable(
                    typedefof<IEnumerable<_>>.MakeGenericType(elementDotnetType),
                    "enumerable")
            Expression.Block(
                [ enumerable ],
                Expression.Assign(enumerable, Expression.Convert(list, enumerable.Type)),
                Expression.Call(enumerable, "GetEnumerator", []))
            :> Expression
        Serializer.list dotnetType elementSerializer getEnumerator

    // Deserializer for required values, i.e. those that will never have null
    // values according to the source schema.
    let createRequiredDeserializer (schema: ListTypeSchema) (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementDeserializer =
            Deserializer.resolve schema.Element elementDotnetType
        let createEmpty =
            Expression.Property(null, dotnetType.GetProperty("Empty"))
        let createFromElementValues (elementValues: Expression) =
            let seqModuleType =
                Assembly.Load("FSharp.Core").GetTypes()
                |> Array.filter (fun type' -> type'.Name = "SeqModule")
                |> Array.exactlyOne
            Expression.Call(seqModuleType, "ToList", [| elementDotnetType |], elementValues)
            :> Expression
        Deserializer.list
            dotnetType elementDeserializer createEmpty createFromElementValues

    // Deserializer for optional values, i.e. those that could have null values
    // according to the source schema. Since we usually don't want null values
    // in F#, we just wrap as a non-nullable type. This means an exception will
    // be thrown if a null value is encountered in the data.
    let createOptionalDeserializer schema dotnetType =
        createRequiredDeserializer schema dotnetType
        |> Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if isListType sourceType
            then Option.Some (createSerializer sourceType)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if not (isListType targetType)
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.List listSchema ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer listSchema targetType)
                    else Option.Some (createRequiredDeserializer listSchema targetType)
                | _ -> Option.None

type internal DefaultArray1dConverter() =
    let isArray1dType (dotnetType: Type) =
        dotnetType.IsArray
        && dotnetType.GetArrayRank() = 1

    let createSerializer (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetElementType()
        let elementSerializer = Serializer.resolve elementDotnetType
        let getEnumerator (array: Expression) =
            // if isNull array then
            //     raise SerializationException(...)
            // let enumerable = array :> IEnumerable<'Element>
            // enumerable.GetEnumerator()
            let enumerable =
                Expression.Variable(
                    typedefof<IEnumerable<_>>.MakeGenericType(elementDotnetType),
                    "enumerable")
            Expression.Block(
                [ enumerable ],
                Serializer.throwIfNull array,
                Expression.Assign(enumerable, Expression.Convert(array, enumerable.Type)),
                Expression.Call(enumerable, "GetEnumerator", []))
            :> Expression
        Serializer.list dotnetType elementSerializer getEnumerator

    // Deserializer for required values, i.e. those that will never have null
    // values according to the source schema.
    let createRequiredDeserializer (schema: ListTypeSchema) (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetElementType()
        let elementDeserializer =
            Deserializer.resolve schema.Element elementDotnetType
        let createEmpty =
            Expression.NewArrayBounds(elementDotnetType, Expression.Constant(0))
        let createFromElementValues (elementValues: Expression) =
            Expression.Call(elementValues, "ToArray", [])
        Deserializer.list
            dotnetType elementDeserializer createEmpty createFromElementValues

    // Deserializer for optional values, i.e. those that could have null values
    // according to the source schema. Since we usually don't want null values
    // in F#, we just wrap as a non-nullable type. This means an exception will
    // be thrown if a null value is encountered in the data.
    let createOptionalDeserializer schema dotnetType =
        createRequiredDeserializer schema dotnetType
        |> Deserializer.optionalNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if isArray1dType sourceType
            then Option.Some (createSerializer sourceType)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if not (isArray1dType targetType)
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.List listSchema ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer listSchema targetType)
                    else Option.Some (createRequiredDeserializer listSchema targetType)
                | _ -> Option.None

type internal DefaultResizeArrayConverter() =
    let isResizeArrayType = DotnetType.isGenericType<ResizeArray<_>>

    let createSerializer (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementSerializer = Serializer.resolve elementDotnetType
        let getEnumerator (list: Expression) =
            // if isNull list then
            //     raise SerializationException(...)
            // let enumerable = list :> IEnumerable<'Element>
            // enumerable.GetEnumerator()
            let enumerable =
                Expression.Variable(
                    typedefof<IEnumerable<_>>.MakeGenericType(elementDotnetType),
                    "enumerable")
            Expression.Block(
                [ enumerable ],
                Serializer.throwIfNull list,
                Expression.Assign(enumerable, Expression.Convert(list, enumerable.Type)),
                Expression.Call(enumerable, "GetEnumerator", []))
            :> Expression
        Serializer.list dotnetType elementSerializer getEnumerator

    // Deserializer for required values, i.e. those that will never have null
    // values according to the source schema.
    let createRequiredDeserializer (schema: ListTypeSchema) (dotnetType: Type) =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementDeserializer =
            Deserializer.resolve schema.Element elementDotnetType
        let createEmpty = Expression.New(dotnetType)
        let createFromElementValues = id
        Deserializer.list
            dotnetType elementDeserializer createEmpty createFromElementValues

    // Deserializer for optional values, i.e. those that could have null values
    // according to the source schema. Since we usually don't want null values
    // in F#, we just wrap as a non-nullable type. This means an exception will
    // be thrown if a null value is encountered in the data.
    let createOptionalDeserializer schema dotnetType =
        createRequiredDeserializer schema dotnetType
        |> Deserializer.optionalNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if isResizeArrayType sourceType
            then Option.Some (createSerializer sourceType)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if not (isResizeArrayType targetType)
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.List listSchema ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer listSchema targetType)
                    else Option.Some (createRequiredDeserializer listSchema targetType)
                | _ -> Option.None

type internal DefaultRecordConverter() =
    let isRecordType = FSharpType.IsRecord

    let createSerializer (dotnetType: Type) =
        let fieldSerializers =
            FSharpType.GetRecordFields(dotnetType)
            |> Array.map FieldSerializer.ofProperty
        Serializer.record dotnetType fieldSerializers

    let tryCreateRequiredDeserializer (recordSchema: RecordTypeSchema) (dotnetType: Type) =
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
            Deserializer.record dotnetType fieldDeserializers createFromFieldValues
            |> Option.Some

    let tryCreateOptionalDeserializer recordSchema dotnetType =
        tryCreateRequiredDeserializer recordSchema dotnetType
        |> Option.map Deserializer.optionalNonNullableTypeWrapper

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if isRecordType sourceType
            then Option.Some (createSerializer sourceType)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if not (isRecordType targetType)
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Record recordSchema ->
                    if sourceSchema.IsOptional
                    then tryCreateOptionalDeserializer recordSchema targetType
                    else tryCreateRequiredDeserializer recordSchema targetType
                | _ -> Option.None

type internal DefaultUnionConverter() =
    let isUnionType = FSharpType.IsUnion

    let createSimpleUnionSerializer (unionInfo: UnionInfo) =
        let dotnetType = unionInfo.DotnetType
        // Unions in which all cases have no fields can be represented as a
        // simple string value containing the case name. Since a union value
        // can't be null and must be one of the possible cases, this value is
        // not optional.
        let dataDotnetType = typeof<string>
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = unionInfo.GetCaseName
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let createUnionCaseSerializer (unionInfo: UnionInfo) (unionCase: UnionCaseInfo) =
        // Union case data is represented as an optional record containing the
        // field values for that case. The record needs to be optional since
        // only one case from the union can be set and the others will be NULL.
        let dotnetType = unionInfo.DotnetType
        let valueSerializer =
            let dotnetType = unionInfo.DotnetType
            let fieldSerializers =
                unionCase.Fields
                |> Array.map (FieldSerializer.ofUnionCaseField unionCase)
            Serializer.record dotnetType fieldSerializers
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
        let typeFieldSerializer =
            // TODO: The name of this field could be configurable via an attribute.
            let name = "Type"
            let valueSerializer =
                // TODO: We should really delegate this to Serializer.ofType,
                // but this would require having a string converter that
                // can treat strings as required.
                let dotnetType = typeof<string>
                let dataDotnetType = dotnetType
                let schema = ValueTypeSchema.primitive dataDotnetType
                let getDataValue = id
                Serializer.atomic schema dotnetType dataDotnetType getDataValue
            let getValue = unionInfo.GetCaseName
            FieldSerializer.create name valueSerializer getValue
        // Each union case with one or more fields is assigned an additional
        // field within the record to hold its associated data. The name of this
        // field matches the case name and the value is a record that contains
        // the case's field values.
        let caseFieldSerializers =
            unionCasesWithFields
            |> Array.map (fun unionCase ->
                let name = unionCase.Name
                // Note that there's a chance the case name is the same as the
                // field name chosen to store the union case name, in which case
                // we'd have two fields with the same name. We could add a level
                // of nesting to the object structure to avoid this potential
                // name conflict, but this adds extra complexity.
                if name = typeFieldSerializer.Name then
                    failwith <|
                        $"case name '{typeFieldSerializer.Name}' is not supported"
                        + $" for union type '{dotnetType.FullName}'"
                let valueSerializer = createUnionCaseSerializer unionInfo unionCase
                let getValue = id
                FieldSerializer.create name valueSerializer getValue)
        let fieldSerializers = Array.append [| typeFieldSerializer |] caseFieldSerializers
        Serializer.record dotnetType fieldSerializers

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
        let schema = ValueTypeSchema.primitive dataDotnetType
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
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let tryCreateUnionCaseDeserializer
        (unionInfo: UnionInfo) (unionCase: UnionCaseInfo) (schema: RecordTypeSchema) =
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

    let tryCreateComplexUnionDeserializer (unionInfo: UnionInfo) (schema: RecordTypeSchema) =
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
        let typeFieldDeserializer =
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
                    let schema = ValueTypeSchema.primitive dataDotnetType
                    let createFromDataValue = id
                    Deserializer.atomic
                        schema dotnetType dataDotnetType createFromDataValue
                FieldDeserializer.create name deserializer)
        // Each union case with one or more fields is assigned an additional
        // field within the record to hold its associated data. The name of this
        // field matches the case name and the value is a record that contains
        // the case's field values.
        let caseFieldDeserializers =
            unionCasesWithFields
            |> Array.choose (fun unionCase ->
                let name = unionCase.Name
                // Note that there's a chance the case name is the same as the
                // field name chosen to store the union case name, in which case
                // we'd have two fields with the same name. We could add a level
                // of nesting to the object structure to avoid this potential
                // name conflict, but this adds extra complexity.
                if typeFieldDeserializer.IsSome && name = typeFieldDeserializer.Value.Name
                then Option.None
                else
                    schema.Fields
                    |> Array.tryFind (fun fieldSchema -> fieldSchema.Name = name)
                    |> Option.bind (fun fieldSchema ->
                        match fieldSchema.Value.Type with
                        | ValueTypeSchema.Record recordSchema ->
                            tryCreateUnionCaseDeserializer unionInfo unionCase recordSchema
                            |> Option.map (FieldDeserializer.create name)
                        | _ -> Option.None))
        if typeFieldDeserializer.IsNone
            || caseFieldDeserializers.Length < unionCasesWithFields.Length
        then Option.None
        else
            let fieldDeserializers =
                Array.append [| typeFieldDeserializer.Value |] caseFieldDeserializers
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
                                        caseFieldDeserializers
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
            Deserializer.record dotnetType fieldDeserializers createFromFieldValues
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
                    match sourceSchema.Type with
                    | ValueTypeSchema.Record recordSchema ->
                        tryCreateComplexUnionDeserializer unionInfo recordSchema
                    | _ -> Option.None

type internal DefaultOptionConverter() =
    let isOptionType = DotnetType.isGenericType<option<_>>
    
    // TODO: Quite a lot of duplicated reflection going on in here!

    let tryCreateSerializer (sourceType: Type) =
        let valueDotnetType = sourceType.GetGenericArguments()[0]
        let valueSerializer = Serializer.resolve valueDotnetType
        // Parquet doesn't support nested optional values, so if the value is
        // optional then we can't serialize it.
        if valueSerializer.IsOptional
        then Option.None
        else
            let dotnetType = sourceType
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
            |> Option.Some

    // Create a deserializer for a required field value. There's no need to wrap
    // the value deserializer in an {OptionalDeserializer} in this case since
    // there will never be any NULL values, but we do need to wrap any values we
    // deserialize in {Option.Some} cases so that they can be assigned to the
    // target option field.
    let createRequiredDeserializer (sourceSchema: ValueSchema) (targetType: Type) =
        // TODO: Can we just use UnionInfo for this?
        let unionCases = FSharpType.GetUnionCases(targetType)
        // Create an expression builder that will take a value and wrap it in an
        // {Option.Some} case.
        let createSome =
            let someCase = unionCases |> Array.find _.Name.Equals("Some")
            let constructorMethod = FSharpValue.PreComputeUnionConstructorInfo(someCase)
            fun (value: Expression) ->
                Expression.Call(constructorMethod, value) :> Expression
        // Resolve the value deserializer. The value schema is just the same as
        // the source schema since we're dealing with a required field value.
        let valueDotnetType = targetType.GetGenericArguments()[0]
        match Deserializer.resolve sourceSchema valueDotnetType with
        | Deserializer.Atomic atomicDeserializer ->
            let dotnetType = targetType
            let dataDotnetType = atomicDeserializer.DataDotnetType
            // TODO: This probably isn't right for DateTime values.
            let schema = ValueTypeSchema.primitive dataDotnetType
            let createFromDataValue dataValue =
                let value = atomicDeserializer.CreateFromDataValue dataValue
                createSome value
            Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue
        | Deserializer.List listDeserializer ->
            let dotnetType = targetType
            let elementDeserializer = listDeserializer.ElementDeserializer
            let createEmpty = createSome listDeserializer.CreateEmpty
            let createFromElementValues elementValues =
                let list = listDeserializer.CreateFromElementValues elementValues
                createSome list
            Deserializer.list
                dotnetType elementDeserializer createEmpty createFromElementValues
        | Deserializer.Record recordDeserializer ->
            let dotnetType = targetType
            let fieldDeserializers = recordDeserializer.FieldDeserializers
            let createFromFieldValues fieldValues =
                let record = recordDeserializer.CreateFromFieldValues fieldValues
                createSome record
            Deserializer.record dotnetType fieldDeserializers createFromFieldValues
        | Deserializer.Optional _ ->
            failwith <|
                "expected non-optional deserializer to be resolved for required"
                + $" schema '{sourceSchema}'"

    // Create a deserializer for an optional field value. In this situation we
    // need to wrap the value deserializer in an {OptionalDeserializer} to
    // handle NULL values. When we read a NULL value we convert it to the
    // {Option.None} case. When we read a NOTNULL value we wrap it in the
    // {Option.Some} case.
    let createOptionalDeserializer (sourceSchema: ValueSchema) targetType =
        // TODO: Can we just use UnionInfo for this?
        let unionCases = FSharpType.GetUnionCases(targetType)
        // Resolve the value deserializer. Since we're dealing with an optional
        // field value and we're going to deal with this optionality by wrapping
        // the value deserializer in an {OptionalDeserializer}, we want to pass
        // down an equivalent non-optional value schema.
        let valueDotnetType = targetType.GetGenericArguments()[0]
        let valueSchema = sourceSchema.MakeRequired()
        let valueDeserializer = Deserializer.resolve valueSchema valueDotnetType
        // Build the {OptionalDeserializer} wrapper.
        let dotnetType = targetType
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

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if isOptionType sourceType
            then tryCreateSerializer sourceType
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if not (isOptionType targetType)
            then Option.None
            else
                if sourceSchema.IsOptional
                then Option.Some (createOptionalDeserializer sourceSchema targetType)
                else Option.Some (createRequiredDeserializer sourceSchema targetType)

type internal DefaultNullableConverter() =
    let isNullableType = DotnetType.isGenericType<Nullable<_>>
    
    // TODO: Quite a lot of duplicated reflection going on in here!

    let tryCreateSerializer (sourceType: Type) =
        let valueDotnetType = Nullable.GetUnderlyingType(sourceType)
        let valueSerializer = Serializer.resolve valueDotnetType
        // Parquet doesn't support nested optional values, so if the value is
        // optional then we can't serialize it.
        if valueSerializer.IsOptional
        then Option.None
        else
            let dotnetType = sourceType
            let isNull (nullable: Expression) =
                Expression.Not(Expression.Property(nullable, "HasValue"))
                :> Expression
            let getValue (nullable: Expression) =
                Expression.Property(nullable, "Value")
                :> Expression
            Serializer.optional dotnetType valueSerializer isNull getValue
            |> Option.Some

    // Create a deserializer for a required field value. There's no need to wrap
    // the value deserializer in an {OptionalDeserializer} in this case since
    // there will never be any NULL values, but we do need to wrap any values we
    // deserialize as {Nullable} values so that they can be assigned to the
    // target field.
    let createRequiredDeserializer (sourceSchema: ValueSchema) (targetType: Type) =
        let valueDotnetType = Nullable.GetUnderlyingType(targetType)
        // Create an expression builder that will take a value and create a
        // {Nullable} value from it.
        let createFromValue =
            let constructor = targetType.GetConstructor([| valueDotnetType |])
            fun (value: Expression) ->
                Expression.New(constructor, value)
                :> Expression
        // Resolve the value deserializer. The value schema is just the same as
        // the source schema since we're dealing with a required field value.
        match Deserializer.resolve sourceSchema valueDotnetType with
        | Deserializer.Atomic atomicDeserializer ->
            let dotnetType = targetType
            let dataDotnetType = atomicDeserializer.DataDotnetType
            // TODO: This probably isn't right for DateTime values.
            let schema = ValueTypeSchema.primitive dataDotnetType
            let createFromDataValue dataValue =
                let value = atomicDeserializer.CreateFromDataValue dataValue
                createFromValue value
            Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue
        | _ ->
            failwith "not implemented!"

    // Create a deserializer for an optional field value. In this situation we
    // need to wrap the value deserializer in an {OptionalDeserializer} to
    // handle NULL values. When we read a NULL value we create a NULL valued
    // {Nullable}. When we read a NOTNULL value we wrap it as a {Nullable}.
    let createOptionalDeserializer (sourceSchema: ValueSchema) targetType =
        // Resolve the value deserializer. Since we're dealing with an optional
        // field value and we're going to deal with this optionality by wrapping
        // the value deserializer in an {OptionalDeserializer}, we want to pass
        // down an equivalent non-optional value schema.
        let valueDotnetType = Nullable.GetUnderlyingType(targetType)
        let valueSchema = sourceSchema.MakeRequired()
        let valueDeserializer = Deserializer.resolve valueSchema valueDotnetType
        // Build the {OptionalDeserializer} wrapper.
        let dotnetType = targetType
        let createNull = Expression.Null(dotnetType)
        let createFromValue =
            let constructor = dotnetType.GetConstructor([| valueDotnetType |])
            fun (value: Expression) ->
                Expression.New(constructor, value)
                :> Expression
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType) =
            if isNullableType sourceType
            then tryCreateSerializer sourceType
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType) =
            if not (isNullableType targetType)
            then Option.None
            else
                if sourceSchema.IsOptional
                then Option.Some (createOptionalDeserializer sourceSchema targetType)
                else Option.Some (createRequiredDeserializer sourceSchema targetType)

type internal DefaultClassConverter() =
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

    let tryCreateDeserializer (dotnetType: Type) (recordSchema: RecordTypeSchema) =
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
                match sourceSchema.Type with
                | ValueTypeSchema.Record recordSchema ->
                    tryCreateDeserializer targetType recordSchema
                | _ -> Option.None
