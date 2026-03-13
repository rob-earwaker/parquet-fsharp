namespace rec Parquet.FSharp

open System
open System.Linq.Expressions
open System.Reflection

// TODO: Attribute ideas:
//   - ParquetAttribute (base class)
//
//   - ParquetField(name: string, required: bool, optional: bool, allowNullValues: bool)
//   - ParquetDecimalField(<inherited>, scale: int, precision: int)
//   - ParquetDateTimeField(<inherited>, isAdjustedToUtc: bool, unit: <enum TimeUnit>)
//   - ParquetDateTimeOffsetField(<inherited>, unit: <enum TimeUnit>)
//   - ParquetUnionField(<inherited>, enum: bool, caseTypeFieldName: string)

//   - ParquetType(required: bool, optional: bool, allowNullValues: bool)
//   - ParquetUnion(caseTypeFieldName: string)
//   - ParquetUnionCase(typeName: string, dataFieldName: string)

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

// TODO: Attribute to select specific serializer type to use? Alternatively could
// be part of the serializer configuration?

// TODO: Add converter type to serializer/deserializer so we can catch exceptions
// that occur when calling the compiled lambda functions and enrich with info about
// which converter they originated from and which lambda function they originated from.

type SerializationException(message) =
    inherit Exception(message)

type internal Settings = {
    ValueConverters: IValueConverter list
    ValuePolicies: IValueSettingsPolicy list
    FieldPolicies: IFieldSettingsPolicy list }

type internal FieldSettings = {
    NameOverride: string option
    ValueSettings: ValueSettings }

type internal ValueSettings = {
    ForceOptional: bool
    ForceRequired: bool
    AllowNullValues: bool
    DecimalScale: int
    DecimalPrecision: int
    UseLocalDateTime: bool
    IgnoreDateTimeKind: bool
    DateTimeUnit: TimeUnit
    // TODO: Is this even necessary?
    UnionCaseTypeFieldName: string
    AlwaysIncludeUnionCaseTypeField: bool
    AlwaysIncludeNestedUnionCaseRecord: bool }

type internal IFieldSettingsPolicy =
    // TODO: Maybe these should be functions?
    // TODO: Should this be PropertyInfo instead? Consider unions carefully!
    abstract member RecordType : Type
    abstract member FieldName : string
    abstract member ApplyFieldSettings : fieldSettings:FieldSettings -> FieldSettings

type internal IValueSettingsPolicy =
    // TODO: Maybe these should be functions?
    abstract member ValueType : Type
    abstract member ApplyValueSettings : valueSettings:ValueSettings -> ValueSettings

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
