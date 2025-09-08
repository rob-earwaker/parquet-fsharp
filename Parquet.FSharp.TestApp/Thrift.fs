namespace Parquet.FSharp.Thrift

type Type =
    | BOOLEAN = 0
    | INT32 = 1
    | INT64 = 2
    | INT96 = 3
    | FLOAT = 4
    | DOUBLE = 5
    | BYTE_ARRAY = 6
    | FIXED_LEN_BYTE_ARRAY = 7

type ConvertedType =
    | UTF8 = 0
    | MAP = 1
    | MAP_KEY_VALUE = 2
    | LIST = 3
    | ENUM = 4
    | DECIMAL = 5
    | DATE = 6
    | TIME_MILLIS = 7
    | TIME_MICROS = 8
    | TIMESTAMP_MILLIS = 9
    | TIMESTAMP_MICROS = 10
    | UINT_8 = 11
    | UINT_16 = 12
    | UINT_32 = 13
    | UINT_64 = 14
    | INT_8 = 15
    | INT_16 = 16
    | INT_32 = 17
    | INT_64 = 18
    | JSON = 19
    | BSON = 20
    | INTERVAL = 21

type FieldRepetitionType =
    | REQUIRED = 0
    | OPTIONAL = 1
    | REPEATED = 2

type StringType = unit
type UUIDType = unit
type MapType = unit
type ListType = unit
type EnumType = unit
type DateType = unit
type Float16Type = unit

type NullType = unit

type DecimalType = {
    scale: int
    precision: int }

type MilliSeconds = unit
type MicroSeconds = unit
type NanoSeconds = unit

type TimeUnit =
    | MilliSeconds of MilliSeconds
    | MicroSeconds of MicroSeconds
    | NanoSeconds of NanoSeconds

type TimestampType = {
    isAdjustedToUTC: bool
    unit: TimeUnit }

type TimeType = {
    isAdjustedToUTC: bool
    unit: TimeUnit }

type IntType = {
    bitWidth: sbyte
    isSigned: bool }

type JsonType = unit

type BsonType = unit

type VariantType = {
    specification_version: sbyte option }

type EdgeInterpolationAlgorithm =
    | SPHERICAL = 0
    | VINCENTY = 1
    | THOMAS = 2
    | ANDOYER = 3
    | KARNEY = 4

type GeometryType = {
    crs: string option }

type GeographyType = {
    crs: string option
    algorithm: EdgeInterpolationAlgorithm option }

type LogicalType =
    | STRING of StringType
    | MAP of MapType
    | LIST of ListType
    | ENUM of EnumType
    | DECIMAL of DecimalType
    | DATE of DateType
    | TIME of TimeType
    | TIMESTAMP of TimestampType
    | INTEGER of IntType
    | UNKNOWN of NullType
    | JSON of JsonType
    | BSON of BsonType
    | UUID of UUIDType
    | FLOAT16 of Float16Type
    | VARIANT of VariantType
    | GEOMETRY of GeometryType
    | GEOGRAPHY of GeographyType

type SchemaElement = {
    type': Type option
    type_length: int option
    repetition_type: FieldRepetitionType option
    name: string
    num_children: int option
    converted_type: ConvertedType option
    scale: int option
    precision: int option
    field_id: int option
    logicalType: LogicalType option }

type FileMetaData = {
    version: int
    schema: SchemaElement[] }
