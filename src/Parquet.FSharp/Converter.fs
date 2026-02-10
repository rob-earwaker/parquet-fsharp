namespace rec Parquet.FSharp

open System
open System.Collections.Generic
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

module private FieldSerializer =
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

module private FieldDeserializer =
    let create name (valueDeserializer: Deserializer) =
        let schema = FieldSchema.create name valueDeserializer.Schema
        { FieldDeserializer.Schema = schema
          FieldDeserializer.Name = name
          FieldDeserializer.ValueDeserializer = valueDeserializer }

    let ofField schema (field: FieldInfo) settings =
        let name = field.Name
        let deserializer = Deserializer.resolve schema field.Type settings
        create name deserializer

type internal DefaultBoolConverter private () =
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

    static member Instance = DefaultBoolConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultInt8Converter private () =
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

    static member Instance = DefaultInt8Converter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultInt16Converter private () =
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

    static member Instance = DefaultInt16Converter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultInt32Converter private () =
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

    static member Instance = DefaultInt32Converter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultInt64Converter private () =
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

    static member Instance = DefaultInt64Converter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultUInt8Converter private () =
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

    static member Instance = DefaultUInt8Converter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultUInt16Converter private () =
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

    static member Instance = DefaultUInt16Converter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultUInt32Converter private () =
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

    static member Instance = DefaultUInt32Converter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultUInt64Converter private () =
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

    static member Instance = DefaultUInt64Converter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultFloat32Converter private () =
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

    static member Instance = DefaultFloat32Converter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultFloat64Converter private () =
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

    static member Instance = DefaultFloat64Converter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultDecimalConverter private () =
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

    static member Instance = DefaultDecimalConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultGuidConverter private () =
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

    static member Instance = DefaultGuidConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultDateTimeConverter private () =
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

    static member Instance = DefaultDateTimeConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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
type internal DefaultDateTimeOffsetConverter private () =
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

    static member Instance = DefaultDateTimeOffsetConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultStringConverter private () =
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

    static member Instance = DefaultStringConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultByteArrayConverter private () =
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

    static member Instance = DefaultByteArrayConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType = dotnetType
            then Option.Some serializer
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
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

type internal DefaultListConverter private () =
    let isListType = DotnetType.isGenericType<list<_>>

    let createSerializer (dotnetType: Type) settings =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementSerializer = Serializer.resolve elementDotnetType settings
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
    let createRequiredDeserializer (schema: ListTypeSchema) (dotnetType: Type) settings =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementDeserializer =
            Deserializer.resolve schema.Element elementDotnetType settings
        let createEmpty =
            Expression.Property(null, dotnetType.GetProperty("Empty"))
        let createFromElementValues (elementValues: Expression) =
            let seqModuleType =
                System.Reflection.Assembly.Load("FSharp.Core").GetTypes()
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
    let createOptionalDeserializer schema dotnetType settings =
        createRequiredDeserializer schema dotnetType settings
        |> Deserializer.optionalNonNullableTypeWrapper

    static member Instance = DefaultListConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if isListType sourceType
            then Option.Some (createSerializer sourceType settings)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            if not (isListType targetType)
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.List listSchema ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer listSchema targetType settings)
                    else Option.Some (createRequiredDeserializer listSchema targetType settings)
                | _ -> Option.None

type internal DefaultArray1dConverter private () =
    let isArray1dType (dotnetType: Type) =
        dotnetType.IsArray
        && dotnetType.GetArrayRank() = 1

    let createSerializer (dotnetType: Type) settings =
        let elementDotnetType = dotnetType.GetElementType()
        let elementSerializer = Serializer.resolve elementDotnetType settings
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
    let createRequiredDeserializer (schema: ListTypeSchema) (dotnetType: Type) settings =
        let elementDotnetType = dotnetType.GetElementType()
        let elementDeserializer =
            Deserializer.resolve schema.Element elementDotnetType settings
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
    let createOptionalDeserializer schema dotnetType settings =
        createRequiredDeserializer schema dotnetType settings
        |> Deserializer.optionalNullableTypeWrapper

    static member Instance = DefaultArray1dConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if isArray1dType sourceType
            then Option.Some (createSerializer sourceType settings)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            if not (isArray1dType targetType)
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.List listSchema ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer listSchema targetType settings)
                    else Option.Some (createRequiredDeserializer listSchema targetType settings)
                | _ -> Option.None

type internal DefaultResizeArrayConverter private () =
    let isResizeArrayType = DotnetType.isGenericType<ResizeArray<_>>

    let createSerializer (dotnetType: Type) settings =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementSerializer = Serializer.resolve elementDotnetType settings
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
    let createRequiredDeserializer (schema: ListTypeSchema) (dotnetType: Type) settings =
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementDeserializer =
            Deserializer.resolve schema.Element elementDotnetType settings
        let createEmpty = Expression.New(dotnetType)
        let createFromElementValues = id
        Deserializer.list
            dotnetType elementDeserializer createEmpty createFromElementValues

    // Deserializer for optional values, i.e. those that could have null values
    // according to the source schema. Since we usually don't want null values
    // in F#, we just wrap as a non-nullable type. This means an exception will
    // be thrown if a null value is encountered in the data.
    let createOptionalDeserializer schema dotnetType settings =
        createRequiredDeserializer schema dotnetType settings
        |> Deserializer.optionalNullableTypeWrapper

    static member Instance = DefaultResizeArrayConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if isResizeArrayType sourceType
            then Option.Some (createSerializer sourceType settings)
            else Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            if not (isResizeArrayType targetType)
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.List listSchema ->
                    if sourceSchema.IsOptional
                    then Option.Some (createOptionalDeserializer listSchema targetType settings)
                    else Option.Some (createRequiredDeserializer listSchema targetType settings)
                | _ -> Option.None

type internal DefaultRecordConverter private () =
    let createSerializer (recordInfo: RecordInfo) settings =
        let fieldSerializers =
            recordInfo.Fields
            |> Array.map (fun fieldInfo ->
                FieldSerializer.ofField fieldInfo settings)
        Serializer.record recordInfo.Type fieldSerializers

    let tryCreateRequiredDeserializer
        (recordSchema: RecordTypeSchema) (recordInfo: RecordInfo) settings =
        let fieldDeserializers =
            recordInfo.Fields
            |> Array.choose (fun fieldInfo ->
                recordSchema.Fields
                |> Array.tryFind (fun fieldSchema -> fieldSchema.Name = fieldInfo.Name)
                |> Option.map (fun fieldSchema ->
                    FieldDeserializer.ofField fieldSchema.Value fieldInfo settings))
        if fieldDeserializers.Length < recordInfo.Fields.Length
        then Option.None
        else
            Deserializer.record
                recordInfo.Type fieldDeserializers recordInfo.CreateFromFieldValues
            |> Option.Some

    let tryCreateOptionalDeserializer recordSchema recordInfo settings =
        tryCreateRequiredDeserializer recordSchema recordInfo settings
        |> Option.map Deserializer.optionalNonNullableTypeWrapper

    static member Instance = DefaultRecordConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            match sourceType with
            | DotnetType.Record recordInfo ->
                Option.Some (createSerializer recordInfo settings)
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            match targetType with
            | DotnetType.Record recordInfo ->
                match sourceSchema.Type with
                | ValueTypeSchema.Record recordSchema ->
                    if sourceSchema.IsOptional
                    then tryCreateOptionalDeserializer recordSchema recordInfo settings
                    else tryCreateRequiredDeserializer recordSchema recordInfo settings
                | _ -> Option.None
            | _ -> Option.None

// TODO: Should we have separate converters for the different union types? Seems
// like they are fairly independent, particularly as common functionality lives in
// the {UnionInfo} type(s).
type internal DefaultUnionConverter private () =
    let createEnumUnionSerializer (unionInfo: UnionInfo) settings =
        let dotnetType = unionInfo.Type
        let caseNameSerializer = Serializer.resolve typeof<string> settings
        let unwrapValue = unionInfo.GetCaseName
        Serializer.wrapAs dotnetType caseNameSerializer unwrapValue

    let createSingleCaseUnionSerializer (unionInfo: UnionInfo) settings =
        // Unions with a single case are most likely being used to enable
        // stricter type checking and to allow encapsulation of any associated
        // field values. We serialize single case unions as a record using the
        // case field names and types.
        let dotnetType = unionInfo.Type
        let unionCase = unionInfo.Cases[0]
        let fieldSerializers =
            unionCase.Fields
            |> Array.map (fun fieldInfo ->
                FieldSerializer.ofField fieldInfo settings)
        Serializer.record dotnetType fieldSerializers

    let createUnionCaseSerializer (unionInfo: UnionInfo) (unionCase: UnionCaseInfo) settings =
        // Union case data is represented as an optional record containing the
        // field values for that case. The record needs to be optional since
        // only one case from the union can be set and the others will be NULL.
        let dotnetType = unionInfo.Type
        let valueSerializer =
            let dotnetType = unionInfo.Type
            let fieldSerializers =
                unionCase.Fields
                |> Array.map (fun fieldInfo ->
                    FieldSerializer.ofField fieldInfo settings)
            Serializer.record dotnetType fieldSerializers
        // The data for this case is NULL if the union tag does not match the
        // tag for this case.
        let isNull (union: Expression) =
            Expression.NotEqual(unionInfo.GetTag union, unionCase.Tag)
            :> Expression
        let getValue = id
        Serializer.optional dotnetType valueSerializer isNull getValue

    let createMultiCaseUnionSerializer (unionInfo: UnionInfo) settings =
        // Unions that have one or more cases with one or more fields can not be
        // represented as a simple string value. Instead, we have to model the
        // union as a record with a field to capture the case name and
        // additional fields to hold any associated case data.
        let dotnetType = unionInfo.Type
        let unionCasesWithFields =
            unionInfo.Cases
            |> Array.filter (fun unionCase -> unionCase.Fields.Length > 0)
        // The 'Type' field holds the case name. Since unions are not nullable
        // there must always be a case name present. We therefore model this
        // as a non-optional string value.
        let typeFieldSerializer =
            // TODO: The name of this field could be configurable via an attribute.
            let name = "Type"
            let valueSerializer = Serializer.resolve typeof<string> settings
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
                let valueSerializer = createUnionCaseSerializer unionInfo unionCase settings
                let getValue = id
                FieldSerializer.create name valueSerializer getValue)
        let fieldSerializers = Array.append [| typeFieldSerializer |] caseFieldSerializers
        Serializer.record dotnetType fieldSerializers

    let createEnumUnionDeserializer (sourceSchema: ValueSchema) (unionInfo: UnionInfo) settings =
        // Unions in which all cases have no fields are be represented as a
        // simple string value containing the case name. Since a union value
        // can't be null and must be one of the possible cases, this value is
        // not optional.
        let dotnetType = unionInfo.Type
        // TODO: Could catch exception that occurs if this isn't resolved and
        // raise a more descriptive exception, or event add a tryResolve function.
        // Also applies to other places where we use 'resolve' for both serializers
        // and deserializers. Or maybe these should just return None when it can't
        // be resolved?
        let caseNameDeserializer =
            Deserializer.resolve sourceSchema typeof<string> settings
        let wrapValue caseName =
            let returnLabel = Expression.Label(dotnetType, "union")
            Expression.Block(
                seq<Expression> {
                    yield! unionInfo.Cases
                        |> Array.map (fun caseInfo ->
                            Expression.IfThen(
                                Expression.Equal(caseName, Expression.Constant(caseInfo.Name)),
                                Expression.Return(returnLabel, caseInfo.CreateFromFieldValues [||]))
                            :> Expression)
                    yield Expression.FailWith<SerializationException>(
                        Expression.Constant("encountered invalid case name '"),
                        // TODO: Could detect null values here and print '<null>' instead of ''
                        caseName,
                        Expression.Constant(
                            "' during deserialization of enum union type"
                            + $" '{dotnetType.FullName}'"))
                    yield Expression.Label(returnLabel, Expression.Default(dotnetType))
                })
            :> Expression
        Deserializer.wrapAs dotnetType caseNameDeserializer wrapValue

    let tryCreateSingleCaseUnionDeserializer
        (sourceSchema: ValueSchema) (unionInfo: UnionInfo) settings =
        match sourceSchema.Type with
        | ValueTypeSchema.Record recordSchema ->
            let dotnetType = unionInfo.Type
            let unionCase = unionInfo.Cases[0]
            let fieldDeserializers =
                unionCase.Fields
                |> Array.choose (fun fieldInfo ->
                    recordSchema.Fields
                    |> Array.tryFind (fun fieldSchema -> fieldSchema.Name = fieldInfo.Name)
                    |> Option.map (fun fieldSchema ->
                        FieldDeserializer.ofField fieldSchema.Value fieldInfo settings))
            let createFromFieldValues = unionCase.CreateFromFieldValues
            if fieldDeserializers.Length < unionCase.Fields.Length
            then Option.None
            else
                let requiredValueDeserializer =
                    Deserializer.record dotnetType fieldDeserializers createFromFieldValues
                let deserializer =
                    if sourceSchema.IsOptional
                    then requiredValueDeserializer |> Deserializer.optionalNonNullableTypeWrapper
                    else requiredValueDeserializer
                Option.Some deserializer
        | _ -> Option.None

    let tryCreateUnionCaseDeserializer
        (unionInfo: UnionInfo) (unionCase: UnionCaseInfo) (schema: RecordTypeSchema) settings =
        // Union case data is represented as an optional record containing the
        // field values for that case. The record needs to be optional since
        // only one case from the union can be set and the others will be NULL.
        let dotnetType = unionInfo.Type
        let deserializer =
            let dotnetType = unionInfo.Type
            let fieldDeserializers =
                unionCase.Fields
                |> Array.choose (fun fieldInfo ->
                    schema.Fields
                    |> Array.tryFind (fun fieldSchema -> fieldSchema.Name = fieldInfo.Name)
                    |> Option.map (fun fieldSchema ->
                        FieldDeserializer.ofField fieldSchema.Value fieldInfo settings))
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

    let tryCreateMultiCaseUnionDeserializer
        (sourceSchema: ValueSchema) (unionInfo: UnionInfo) settings =
        // For unions that have one or more cases with one or more fields, we
        // model as a record, with a field to capture the case name and
        // additional fields to hold any associated case data.
        match sourceSchema.Type with
        | ValueTypeSchema.Record recordSchema ->
            let dotnetType = unionInfo.Type
            let unionCasesWithFields =
                unionInfo.Cases
                |> Array.filter (fun unionCase -> unionCase.Fields.Length > 0)
            // The 'Type' field holds the case name as a string.
            let typeFieldDeserializer =
                // TODO: The name of this field could be configurable via an attribute.
                let name = "Type"
                recordSchema.Fields
                |> Array.tryFind (fun fieldSchema -> fieldSchema.Name = name)
                |> Option.map (fun fieldSchema ->
                    Deserializer.resolve fieldSchema.Value typeof<string> settings
                    |> FieldDeserializer.create name)
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
                        recordSchema.Fields
                        |> Array.tryFind (fun fieldSchema -> fieldSchema.Name = name)
                        |> Option.bind (fun fieldSchema ->
                            match fieldSchema.Value.Type with
                            | ValueTypeSchema.Record recordSchema ->
                                tryCreateUnionCaseDeserializer unionInfo unionCase recordSchema settings
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
                            for caseInfo in unionInfo.Cases do
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
                let requiredValueDeserializer =
                    Deserializer.record dotnetType fieldDeserializers createFromFieldValues
                let deserializer =
                    if sourceSchema.IsOptional
                    then requiredValueDeserializer |> Deserializer.optionalNonNullableTypeWrapper
                    else requiredValueDeserializer
                Option.Some deserializer
        | _ -> Option.None

    static member Instance = DefaultUnionConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            match sourceType with
            | DotnetType.Union unionInfo ->
                let serializer =
                    match unionInfo.UnionCategory with
                    | UnionCategory.Enum -> createEnumUnionSerializer unionInfo settings
                    | UnionCategory.SingleCase -> createSingleCaseUnionSerializer unionInfo settings
                    | UnionCategory.MultiCase -> createMultiCaseUnionSerializer unionInfo settings
                Option.Some serializer
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            match targetType with
            | DotnetType.Union unionInfo ->
                match unionInfo.UnionCategory with
                | UnionCategory.Enum ->
                    Option.Some (createEnumUnionDeserializer sourceSchema unionInfo settings)
                | UnionCategory.SingleCase ->
                    tryCreateSingleCaseUnionDeserializer sourceSchema unionInfo settings
                | UnionCategory.MultiCase ->
                    tryCreateMultiCaseUnionDeserializer sourceSchema unionInfo settings
            | _ -> Option.None

type internal DefaultOptionConverter private () =
    let tryCreateSerializer (optionInfo: OptionInfo) settings =
        let valueSerializer = Serializer.resolve optionInfo.ValueType settings
        // Parquet doesn't support nested optional values, so if the value is
        // optional then we can't serialize it.
        if valueSerializer.IsOptional
        then Option.None
        else
            let dotnetType = optionInfo.Type
            let isNull = optionInfo.IsNull
            let getValue = optionInfo.GetValue
            Serializer.optional dotnetType valueSerializer isNull getValue
            |> Option.Some

    // Create a deserializer for a required field value. There's no need to wrap
    // the value deserializer in an {OptionalDeserializer} in this case since
    // there will never be any NULL values, but we do need to wrap any values we
    // deserialize in {Option.Some} cases so that they can be assigned to the
    // target option field.
    let createRequiredDeserializer
        sourceSchema (optionInfo: OptionInfo) settings =
        let wrapValue = optionInfo.CreateFromValue
        // Resolve the value deserializer. The value schema is just the same as
        // the source schema since we're dealing with a required field value.
        let valueDeserializer =
            Deserializer.resolve sourceSchema optionInfo.ValueType settings
        Deserializer.wrapAs optionInfo.Type valueDeserializer wrapValue

    // Create a deserializer for an optional field value. In this situation we
    // need to wrap the value deserializer in an {OptionalDeserializer} to
    // handle NULL values. When we read a NULL value we convert it to the
    // {Option.None} case. When we read a NOTNULL value we wrap it in the
    // {Option.Some} case.
    let createOptionalDeserializer
        (sourceSchema: ValueSchema) (optionInfo: OptionInfo) settings =
        // Resolve the value deserializer. Since we're dealing with an optional
        // field value and we're going to deal with this optionality by wrapping
        // the value deserializer in an {OptionalDeserializer}, we want to pass
        // down an equivalent non-optional value schema.
        let valueSchema = sourceSchema.MakeRequired()
        let valueDeserializer =
            Deserializer.resolve valueSchema optionInfo.ValueType settings
        // Build the {OptionalDeserializer} wrapper.
        let dotnetType = optionInfo.Type
        let createNull = optionInfo.CreateNull
        let createFromValue = optionInfo.CreateFromValue
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    static member Instance = DefaultOptionConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            match sourceType with
            | DotnetType.Option optionInfo ->
                tryCreateSerializer optionInfo settings
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            match targetType with
            | DotnetType.Option optionInfo ->
                let deserializer =
                    if sourceSchema.IsOptional
                    then createOptionalDeserializer sourceSchema optionInfo settings
                    else createRequiredDeserializer sourceSchema optionInfo settings
                Option.Some deserializer
            | _ -> Option.None

type internal DefaultNullableConverter private () =
    let tryCreateSerializer (nullableInfo: NullableInfo) settings =
        let valueSerializer = Serializer.resolve nullableInfo.ValueType settings
        // Parquet doesn't support nested optional values, so if the value is
        // optional then we can't serialize it.
        if valueSerializer.IsOptional
        then Option.None
        else
            let dotnetType = nullableInfo.Type
            let isNull = nullableInfo.IsNull
            let getValue = nullableInfo.GetValue
            Serializer.optional dotnetType valueSerializer isNull getValue
            |> Option.Some

    // Create a deserializer for a required field value. There's no need to wrap
    // the value deserializer in an {OptionalDeserializer} in this case since
    // there will never be any NULL values, but we do need to wrap any values we
    // deserialize as {Nullable} values so that they can be assigned to the
    // target field.
    let createRequiredDeserializer
        sourceSchema (nullableInfo: NullableInfo) settings =
        let wrapValue = nullableInfo.CreateFromValue
        // Resolve the value deserializer. The value schema is just the same as
        // the source schema since we're dealing with a required field value.
        let valueDeserializer =
            Deserializer.resolve sourceSchema nullableInfo.ValueType settings
        Deserializer.wrapAs nullableInfo.Type valueDeserializer wrapValue

    // Create a deserializer for an optional field value. In this situation we
    // need to wrap the value deserializer in an {OptionalDeserializer} to
    // handle NULL values. When we read a NULL value we create a NULL valued
    // {Nullable}. When we read a NOTNULL value we wrap it as a {Nullable}.
    let createOptionalDeserializer
        (sourceSchema: ValueSchema) (nullableInfo: NullableInfo) settings =
        // Resolve the value deserializer. Since we're dealing with an optional
        // field value and we're going to deal with this optionality by wrapping
        // the value deserializer in an {OptionalDeserializer}, we want to pass
        // down an equivalent non-optional value schema.
        let valueSchema = sourceSchema.MakeRequired()
        let valueDeserializer =
            Deserializer.resolve valueSchema nullableInfo.ValueType settings
        // Build the {OptionalDeserializer} wrapper.
        let dotnetType = nullableInfo.Type
        let createNull = nullableInfo.CreateNull
        let createFromValue = nullableInfo.CreateFromValue
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    static member Instance = DefaultNullableConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            match sourceType with
            | DotnetType.Nullable nullableInfo ->
                tryCreateSerializer nullableInfo settings
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            match targetType with
            | DotnetType.Nullable nullableInfo ->
                let deserializer =
                    if sourceSchema.IsOptional
                    then createOptionalDeserializer sourceSchema nullableInfo settings
                    else createRequiredDeserializer sourceSchema nullableInfo settings
                Option.Some deserializer
            | _ -> Option.None
