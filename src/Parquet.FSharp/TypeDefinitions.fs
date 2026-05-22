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
//     - Enums
//     - TimeSpan

//   Not implemented:
//     - BigInteger
//     - DateOnly, TimeOnly
//     - Interval

// TODO: Replace 'failwith' with 'SerializationException'.

// TODO: Attribute to select specific serializer type to use? Alternatively could
// be part of the serializer configuration?

// TODO: Add converter type to serializer/deserializer so we can catch exceptions
// that occur when calling the compiled lambda functions and enrich with info about
// which converter they originated from and which lambda function they originated from.

type SerializationException(message) =
    inherit Exception(message)

// TODO: Add UnionCasePolicies?
type Settings = {
    ValueConverters: IValueConverter list
    FieldPolicies: IFieldSettingsPolicy list
    ValuePolicies: IValueSettingsPolicy list }

type FieldSettings = {
    Name: string option
    ValueSettings: ValueSettings }

// TODO: Add ListElementSettings?
type ValueSettings = {
    Converter: IValueConverter option }

type IFieldSettingsPolicy =
    abstract member IsValidFor : field:PropertyInfo -> bool
    abstract member ApplyFieldSettings : fieldSettings:FieldSettings -> FieldSettings

type IValueSettingsPolicy =
    abstract member IsValidFor : valueType:Type -> bool
    abstract member ApplyValueSettings : valueSettings:ValueSettings -> ValueSettings

type IValueConverter =
    abstract member TryCreateSerializer
        : sourceType:Type * settings:Settings -> Serializer option
    abstract member TryCreateDeserializer
        : sourceSchema:ValueSchema * targetType:Type * settings:Settings -> Deserializer option

type Serializer =
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

type AtomicSerializer = {
    Schema: ValueSchema
    DotnetType: Type
    DataDotnetType: Type
    GetDataValue: Expression -> Expression }

type ListSerializer = {
    Schema: ValueSchema
    DotnetType: Type
    ElementSerializer: Serializer
    GetEnumerator: Expression -> Expression }

type FieldSerializer = {
    Schema: FieldSchema
    Name: string
    ValueSerializer: Serializer
    GetValue: Expression -> Expression }

type RecordSerializer = {
    Schema: ValueSchema
    DotnetType: Type
    FieldSerializers: FieldSerializer[] }

type OptionalSerializer = {
    Schema: ValueSchema
    DotnetType: Type
    ValueSerializer: Serializer
    IsNull: Expression -> Expression
    GetValue: Expression -> Expression }

type Deserializer =
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

type AtomicDeserializer = {
    Schema: ValueSchema
    DotnetType: Type
    DataDotnetType: Type
    CreateFromDataValue: Expression -> Expression }

type ListDeserializer = {
    Schema: ValueSchema
    DotnetType: Type
    ElementDeserializer: Deserializer
    CreateEmpty: Expression
    CreateFromElementValues: Expression -> Expression }

type FieldDeserializer = {
    Schema: FieldSchema
    Name: string
    ValueDeserializer: Deserializer }

type RecordDeserializer = {
    Schema: ValueSchema
    DotnetType: Type
    FieldDeserializers: FieldDeserializer[]
    CreateFromFieldValues: Expression[] -> Expression }

type OptionalDeserializer = {
    Schema: ValueSchema
    DotnetType: Type
    ValueDeserializer: Deserializer
    CreateNull: Expression
    CreateFromValue: Expression -> Expression }
