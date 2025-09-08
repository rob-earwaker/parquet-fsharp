namespace Parquet.FSharp.Thrift

open System

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

type SchemaElement() =
    member val type' : Type option = Option.None with get, set
    member val type_length : int option = Option.None with get, set
    member val repetition_type : FieldRepetitionType option = Option.None with get, set
    member val name : string = null with get, set
    member val num_children : int option = Option.None with get, set
    member val converted_type : ConvertedType option = Option.None with get, set
    member val scale : int option = Option.None with get, set
    member val precision : int option = Option.None with get, set
    member val field_id : int option = Option.None with get, set
    member val logicalType : LogicalType option = Option.None with get, set

type FileMetaData = {
    version: int
    schema: SchemaElement[] }

module SchemaElement =
    let root numChildren =
        SchemaElement(
            name = "root",
            num_children = Option.Some numChildren)
        
    let group repetitionType name numChildren =
        SchemaElement(
            repetition_type = Option.Some repetitionType,
            name = name,
            num_children = Option.Some numChildren)

    let primitive repetitionType name logicalType =
        let schemaElement =
            SchemaElement(
                repetition_type = Option.Some repetitionType,
                name = name,
                logicalType = Option.Some logicalType)
        // Where applicable, fill in the {type'}, {type_length},
        // {converted_type}, {scale} and {precision} fields fields based on the
        // logical type.
        match logicalType with
        | LogicalType.STRING () ->
            schemaElement.type' <- Option.Some Type.BYTE_ARRAY
            schemaElement.converted_type <- Option.Some ConvertedType.UTF8
        | LogicalType.MAP () -> failwith $"unsupported logical type %A{logicalType}"
        | LogicalType.LIST () -> failwith $"unsupported logical type %A{logicalType}"
        | LogicalType.ENUM () ->
            schemaElement.type' <- Option.Some Type.BYTE_ARRAY
            schemaElement.converted_type <- Option.Some ConvertedType.ENUM
        | LogicalType.DECIMAL decimalType ->
            if decimalType.precision <= 0 then
                failwith $"invalid decimal precision {decimalType.precision}"
            elif decimalType.precision <= 9 then
                schemaElement.type' <- Option.Some Type.INT32
            elif decimalType.precision <= 18 then
                schemaElement.type' <- Option.Some Type.INT64
            else
                // Length n can store <= floor(log_10(2^(8*n - 1) - 1)) base-10 digits.
                //   => log_10(2^(8*n - 1) - 1) = p
                //   => 2^(8*n - 1) - 1 = 10^p
                //   => 2^(8*n - 1) = 10^p + 1
                //   => 8*n - 1 = log_2(10^p + 1)
                //   => 8*n = log_2(10^p + 1) + 1
                //   => n = (log_2(10^p + 1) + 1)/8
                // Precision p requires >= ceil((log_2(10^p + 1) + 1)/8) bytes to store.
                let n = int (ceil ((Math.Log(10. ** decimalType.precision + 1., 2) + 1.) / 8.))
                schemaElement.type' <- Option.Some Type.FIXED_LEN_BYTE_ARRAY
                schemaElement.type_length <- Option.Some n
            schemaElement.converted_type <- Option.Some ConvertedType.DECIMAL
            schemaElement.scale <- Option.Some decimalType.scale
            schemaElement.precision <- Option.Some decimalType.precision
        | LogicalType.DATE () ->
            schemaElement.type' <- Option.Some Type.INT32
            schemaElement.converted_type <- Option.Some ConvertedType.DATE
        | LogicalType.TIME timeType ->
            match timeType.unit with
            | TimeUnit.MilliSeconds () ->
                schemaElement.type' <- Option.Some Type.INT32
                schemaElement.converted_type <- Option.Some ConvertedType.TIME_MILLIS
            | TimeUnit.MicroSeconds () ->
                schemaElement.type' <- Option.Some Type.INT64
                schemaElement.converted_type <- Option.Some ConvertedType.TIME_MICROS
            | TimeUnit.NanoSeconds () ->
                schemaElement.type' <- Option.Some Type.INT64
        | LogicalType.TIMESTAMP timestampType ->
            schemaElement.type' <- Option.Some Type.INT64
            match timestampType.unit with
            | TimeUnit.MilliSeconds () ->
                schemaElement.converted_type <- Option.Some ConvertedType.TIMESTAMP_MILLIS
            | TimeUnit.MicroSeconds () ->
                schemaElement.converted_type <- Option.Some ConvertedType.TIMESTAMP_MICROS
            | TimeUnit.NanoSeconds () -> ()
        | LogicalType.INTEGER intType ->
            match intType with
            | { bitWidth = 8y; isSigned = true } ->
                schemaElement.type' <- Option.Some Type.INT32
                schemaElement.converted_type <- Option.Some ConvertedType.INT_8
            | { bitWidth = 16y; isSigned = true } ->
                schemaElement.type' <- Option.Some Type.INT32
                schemaElement.converted_type <- Option.Some ConvertedType.INT_16
            | { bitWidth = 32y; isSigned = true } ->
                schemaElement.type' <- Option.Some Type.INT32
                schemaElement.converted_type <- Option.Some ConvertedType.INT_32
            | { bitWidth = 64y; isSigned = true } ->
                schemaElement.type' <- Option.Some Type.INT64
                schemaElement.converted_type <- Option.Some ConvertedType.INT_64
            | { bitWidth = 8y; isSigned = false } ->
                schemaElement.type' <- Option.Some Type.INT32
                schemaElement.converted_type <- Option.Some ConvertedType.UINT_8
            | { bitWidth = 16y; isSigned = false } ->
                schemaElement.type' <- Option.Some Type.INT32
                schemaElement.converted_type <- Option.Some ConvertedType.UINT_16
            | { bitWidth = 32y; isSigned = false } ->
                schemaElement.type' <- Option.Some Type.INT32
                schemaElement.converted_type <- Option.Some ConvertedType.UINT_32
            | { bitWidth = 64y; isSigned = false } ->
                schemaElement.type' <- Option.Some Type.INT64
                schemaElement.converted_type <- Option.Some ConvertedType.UINT_64
            | _ -> failwith $"unsupported int type %A{intType}"
        | LogicalType.UNKNOWN () -> failwith $"unsupported logical type %A{logicalType}"
        | LogicalType.JSON () ->
            schemaElement.type' <- Option.Some Type.BYTE_ARRAY
            schemaElement.converted_type <- Option.Some ConvertedType.JSON
        | LogicalType.BSON () ->
            schemaElement.type' <- Option.Some Type.BYTE_ARRAY
            schemaElement.converted_type <- Option.Some ConvertedType.BSON
        | LogicalType.UUID () ->
            schemaElement.type' <- Option.Some Type.FIXED_LEN_BYTE_ARRAY
            schemaElement.type_length <- Option.Some 16
        | LogicalType.FLOAT16 () ->
            schemaElement.type' <- Option.Some Type.FIXED_LEN_BYTE_ARRAY
            schemaElement.type_length <- Option.Some 2
        | LogicalType.VARIANT variantType -> failwith $"unsupported logical type %A{logicalType}"
        | LogicalType.GEOMETRY geometryType -> failwith $"unsupported logical type %A{logicalType}"
        | LogicalType.GEOGRAPHY geographyType -> failwith $"unsupported logical type %A{logicalType}"
        schemaElement
