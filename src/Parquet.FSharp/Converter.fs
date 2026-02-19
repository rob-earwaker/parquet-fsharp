namespace rec Parquet.FSharp

open System
open System.Linq.Expressions

type SerializationException(message) =
    inherit Exception(message)

// TODO: Attribute ideas:
//   - ParquetAttribute (base class)
//
//   - ParquetField(name: string, required: bool, optional: bool, allowNulls: bool)
//   - ParquetDecimalField(<inherited>, scale: int, precision: int)
//   - ParquetDateTimeField(<inherited>, isAdjustedToUtc: bool, unit: <enum TimeUnit>)
//   - ParquetDateTimeOffsetField(<inherited>, unit: <enum TimeUnit>)
//   - ParquetUnionField(<inherited>, enum: bool, caseTypeFieldName: string)

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

// TODO: Replace 'failwith' with 'SerializationException'.

// TODO: Can we cache anything, e.g. reflected info?

// TODO: Attribute to select specific serializer type to use? Alternatively could
// be part of the serializer configuration?

// TODO: Add converter type to serializer/deserializer so we can catch exceptions
// that occur when calling the compiled lambda functions and enrich with info about
// which converter they originated from and which lambda function they originated from.

type internal Settings = {
    ValueConverters: IValueConverter[] }

type internal IValueConverter =
    abstract member TryCreateSerializer
        : sourceType:Type * settings:Settings -> Serializer option
    abstract member TryCreateDeserializer
        : sourceSchema:ValueSchema * targetType:Type * settings:Settings -> Deserializer option

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

    let wrapAs dotnetType (serializer: Serializer) unwrapValue =
        // Modify an existing serializer such that it instead serializes a
        // wrapper type, providing an expression builder that converts from the
        // wrapper type into the wrapped type.
        match serializer with
        | Serializer.Atomic atomicSerializer ->
            let schema = atomicSerializer.Schema.Type
            let dataDotnetType = atomicSerializer.DataDotnetType
            let getDataValue (value: Expression) =
                let unwrappedValue = unwrapValue value
                atomicSerializer.GetDataValue unwrappedValue
            Serializer.atomic schema dotnetType dataDotnetType getDataValue
        | Serializer.List listSerializer ->
            let elementSerializer = listSerializer.ElementSerializer
            let getEnumerator (list: Expression) =
                let unwrappedList = unwrapValue list
                listSerializer.GetEnumerator unwrappedList
            Serializer.list dotnetType elementSerializer getEnumerator
        | Serializer.Record recordSerializer ->
            let fieldSerializers =
                recordSerializer.FieldSerializers
                |> Array.map (fun fieldSerializer ->
                    let name = fieldSerializer.Name
                    let valueSerializer = fieldSerializer.ValueSerializer
                    let getValue (record: Expression) =
                        let unwrappedRecord = unwrapValue record
                        fieldSerializer.GetValue unwrappedRecord
                    FieldSerializer.create name valueSerializer getValue)
            Serializer.record dotnetType fieldSerializers
        | Serializer.Optional optionalSerializer ->
            let valueSerializer = optionalSerializer.ValueSerializer
            let isNull (optional: Expression) =
                let unwrappedOptional = unwrapValue optional
                optionalSerializer.IsNull unwrappedOptional
            let getValue (optional: Expression) =
                let unwrappedOptional = unwrapValue optional
                optionalSerializer.GetValue unwrappedOptional
            Serializer.optional dotnetType valueSerializer isNull getValue

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

    let resolve (sourceType: Type) (settings: Settings) =
        settings.ValueConverters
        |> Array.tryPick _.TryCreateSerializer(sourceType, settings)
        |> Option.defaultWith (fun () ->
            // TODO: This will likely end up depending on attributes as well,
            // so probably will want to make the exception more generic to
            // avoid confusion if there is a converter registered to support the
            // specified type.
            failwith <|
                "could not find converter to serialize type"
                + $" '{sourceType.FullName}'")

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

    let wrapAs dotnetType (deserializer: Deserializer) wrapValue =
        // Modify an existing deserializer such that it instead deserializes
        // into a wrapper type, providing an expression builder that converts
        // a value from the original type into the wrapper type.
        match deserializer with
        | Deserializer.Atomic atomicDeserializer ->
            let schema = atomicDeserializer.Schema.Type
            let dataDotnetType = atomicDeserializer.DataDotnetType
            let createFromDataValue dataValue =
                let value = atomicDeserializer.CreateFromDataValue dataValue
                wrapValue value
            Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue
        | Deserializer.List listDeserializer ->
            let elementDeserializer = listDeserializer.ElementDeserializer
            let createEmpty = wrapValue listDeserializer.CreateEmpty
            let createFromElementValues elementValues =
                let list = listDeserializer.CreateFromElementValues elementValues
                wrapValue list
            Deserializer.list
                dotnetType elementDeserializer createEmpty createFromElementValues
        | Deserializer.Record recordDeserializer ->
            let fieldDeserializers = recordDeserializer.FieldDeserializers
            let createFromFieldValues fieldValues =
                let record = recordDeserializer.CreateFromFieldValues fieldValues
                wrapValue record
            Deserializer.record dotnetType fieldDeserializers createFromFieldValues
        | Deserializer.Optional optionalDeserializer ->
            let valueDeserializer = optionalDeserializer.ValueDeserializer
            let createNull = wrapValue optionalDeserializer.CreateNull
            let createFromValue value =
                let optional = optionalDeserializer.CreateFromValue value
                wrapValue optional
            Deserializer.optional
                dotnetType valueDeserializer createNull createFromValue

    let referenceTypeWrapper (valueDeserializer: Deserializer) =
        let dotnetType = valueDeserializer.DotnetType
        let createNull = Expression.Null(dotnetType)
        let createFromValue = id
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    let throwNullValueEncounteredForNonNullableType (dotnetType: Type) =
        Expression.Block(
            Expression.FailWith<SerializationException>(
                "null value encountered during deserialization for"
                + $" non-nullable type '{dotnetType.FullName}'"),
            Expression.Default(dotnetType))
        :> Expression

    let optionalNonNullableTypeWrapper (valueDeserializer: Deserializer) =
        let dotnetType = valueDeserializer.DotnetType
        let createNull = throwNullValueEncounteredForNonNullableType dotnetType
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

    let resolve sourceSchema targetType (settings: Settings) =
        settings.ValueConverters
        |> Array.tryPick _.TryCreateDeserializer(sourceSchema, targetType, settings)
        |> Option.defaultWith (fun () ->
            // TODO: This will likely end up depending on attributes as well,
            // so probably will want to make the exception more generic to
            // avoid confusion if there is a converter registered to support the
            // specified type.
            failwith <|
                "could not find converter to deserialize from schema"
                + $" '{sourceSchema}' to type '{targetType.FullName}'")

module internal FieldSerializer =
    let create name (valueSerializer: Serializer) getValue =
        let schema = FieldSchema.create name valueSerializer.Schema
        { FieldSerializer.Schema = schema
          FieldSerializer.Name = name
          FieldSerializer.ValueSerializer = valueSerializer
          FieldSerializer.GetValue = getValue }

    let ofField (field: FieldInfo) settings =
        let name = field.Name
        let valueSerializer = Serializer.resolve field.Type settings
        let getValue = field.GetValue
        create name valueSerializer getValue

module internal FieldDeserializer =
    let create name (valueDeserializer: Deserializer) =
        let schema = FieldSchema.create name valueDeserializer.Schema
        { FieldDeserializer.Schema = schema
          FieldDeserializer.Name = name
          FieldDeserializer.ValueDeserializer = valueDeserializer }

    let ofField schema (field: FieldInfo) settings =
        let name = field.Name
        let deserializer = Deserializer.resolve schema field.Type settings
        create name deserializer
