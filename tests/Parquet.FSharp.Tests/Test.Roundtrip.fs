module Parquet.FSharp.Tests.Roundtrip

open FsCheck.Xunit
open Parquet.FSharp
open System
open System.IO

type EnumUnion =
    | CaseA
    | CaseB
    | CaseC

type ComplexUnion =
    | CaseA
    | CaseB of field1:int
    | CaseC of field2:float * field3:bool
    | CaseD of bool
    | CaseE of int * decimal

type ComplexUnionWithBoolCase = Case1 of field1:bool
type ComplexUnionWithInt8Case = Case1 of field1:int8
type ComplexUnionWithInt16Case = Case1 of field1:int16
type ComplexUnionWithInt32Case = Case1 of field1:int
type ComplexUnionWithInt64Case = Case1 of field1:int64
type ComplexUnionWithUInt8Case = Case1 of field1:uint8
type ComplexUnionWithUInt16Case = Case1 of field1:uint16
type ComplexUnionWithUInt32Case = Case1 of field1:uint
type ComplexUnionWithUInt64Case = Case1 of field1:uint64
type ComplexUnionWithFloat32Case = Case1 of field1:float32
type ComplexUnionWithFloat64Case = Case1 of field1:float
type ComplexUnionWithDecimalCase = Case1 of field1:decimal
type ComplexUnionWithDateTimeCase = Case1 of field1:DateTime
type ComplexUnionWithDateTimeOffsetCase = Case1 of field1:DateTimeOffset
type ComplexUnionWithStringCase = Case1 of field1:string
type ComplexUnionWithByteArrayCase = Case1 of field1:byte[]
type ComplexUnionWithGuidCase = Case1 of field1:Guid
type ComplexUnionWithArrayCase = Case1 of field1:array<int>
type ComplexUnionWithGenericListCase = Case1 of field1:ResizeArray<int>
type ComplexUnionWithFSharpListCase = Case1 of field1:list<int>
type ComplexUnionWithRecordCase = Case1 of field1:{| Field1: int |}
type ComplexUnionWithEnumUnionCase = Case1 of field1:EnumUnion
type ComplexUnionWithComplexUnionCase = Case1 of field1:ComplexUnion
type ComplexUnionWithNullableBoolCase = Case1 of field1:Nullable<bool>
type ComplexUnionWithNullableInt8Case = Case1 of field1:Nullable<int8>
type ComplexUnionWithNullableInt16Case = Case1 of field1:Nullable<int16>
type ComplexUnionWithNullableInt32Case = Case1 of field1:Nullable<int>
type ComplexUnionWithNullableInt64Case = Case1 of field1:Nullable<int64>
type ComplexUnionWithNullableUInt8Case = Case1 of field1:Nullable<uint8>
type ComplexUnionWithNullableUInt16Case = Case1 of field1:Nullable<uint16>
type ComplexUnionWithNullableUInt32Case = Case1 of field1:Nullable<uint>
type ComplexUnionWithNullableUInt64Case = Case1 of field1:Nullable<uint64>
type ComplexUnionWithNullableFloat32Case = Case1 of field1:Nullable<float32>
type ComplexUnionWithNullableFloat64Case = Case1 of field1:Nullable<float>
type ComplexUnionWithNullableDecimalCase = Case1 of field1:Nullable<decimal>
type ComplexUnionWithNullableDateTimeCase = Case1 of field1:Nullable<DateTime>
type ComplexUnionWithNullableDateTimeOffsetCase = Case1 of field1:Nullable<DateTimeOffset>
type ComplexUnionWithNullableGuidCase = Case1 of field1:Nullable<Guid>
type ComplexUnionWithNullableRecordCase = Case1 of field1:Nullable<struct {| Field1: int |}>
type ComplexUnionWithOptionBoolCase = Case1 of field1:option<bool>
type ComplexUnionWithOptionInt8Case = Case1 of field1:option<int8>
type ComplexUnionWithOptionInt16Case = Case1 of field1:option<int16>
type ComplexUnionWithOptionInt32Case = Case1 of field1:option<int>
type ComplexUnionWithOptionInt64Case = Case1 of field1:option<int64>
type ComplexUnionWithOptionUInt8Case = Case1 of field1:option<uint8>
type ComplexUnionWithOptionUInt16Case = Case1 of field1:option<uint16>
type ComplexUnionWithOptionUInt32Case = Case1 of field1:option<uint>
type ComplexUnionWithOptionUInt64Case = Case1 of field1:option<uint64>
type ComplexUnionWithOptionFloat32Case = Case1 of field1:option<float32>
type ComplexUnionWithOptionFloat64Case = Case1 of field1:option<float>
type ComplexUnionWithOptionDecimalCase = Case1 of field1:option<decimal>
type ComplexUnionWithOptionDateTimeCase = Case1 of field1:option<DateTime>
type ComplexUnionWithOptionDateTimeOffsetCase = Case1 of field1:option<DateTimeOffset>
type ComplexUnionWithOptionStringCase = Case1 of field1:option<string>
type ComplexUnionWithOptionByteArrayCase = Case1 of field1:option<byte[]>
type ComplexUnionWithOptionGuidCase = Case1 of field1:option<Guid>
type ComplexUnionWithOptionArrayCase = Case1 of field1:option<array<int>>
type ComplexUnionWithOptionGenericListCase = Case1 of field1:option<ResizeArray<int>>
type ComplexUnionWithOptionFSharpListCase = Case1 of field1:option<list<int>>
type ComplexUnionWithOptionRecordCase = Case1 of field1:option<{| Field1: int |}>
type ComplexUnionWithOptionEnumUnionCase = Case1 of field1:option<EnumUnion>
type ComplexUnionWithOptionComplexUnionCase = Case1 of field1:option<ComplexUnion>
type ComplexUnionWithOptionNullableCase = Case1 of field1:option<Nullable<int>>
type ComplexUnionWithOptionOptionCase = Case1 of field1:option<option<int>>

let testRoundtrip<'Record> (records: 'Record[]) =
    use stream = new MemoryStream()
    ParquetSerializer.Serialize<'Record>(records, stream)
    stream.Seek(0, SeekOrigin.Begin) |> ignore
    let roundtrippedRecords = ParquetSerializer.Deserialize<'Record>(stream)
    Assert.structurallyEqual records roundtrippedRecords

[<Property>]
let ``bool field`` records =
    testRoundtrip<{|
        Field1: bool |}>
        records

[<Property>]
let ``int8 field`` records =
    testRoundtrip<{|
        Field1: int8 |}>
        records

[<Property>]
let ``int16 field`` records =
    testRoundtrip<{|
        Field1: int16 |}>
        records

[<Property>]
let ``int32 field`` records =
    testRoundtrip<{|
        Field1: int |}>
        records

[<Property>]
let ``int64 field`` records =
    testRoundtrip<{|
        Field1: int64 |}>
        records

[<Property>]
let ``uint8 field`` records =
    testRoundtrip<{|
        Field1: uint8 |}>
        records

[<Property>]
let ``uint16 field`` records =
    testRoundtrip<{|
        Field1: uint16 |}>
        records

[<Property>]
let ``uint32 field`` records =
    testRoundtrip<{|
        Field1: uint |}>
        records

[<Property>]
let ``uint64 field`` records =
    testRoundtrip<{|
        Field1: uint64 |}>
        records

[<Property>]
let ``float32 field`` records =
    testRoundtrip<{|
        Field1: float32 |}>
        records

[<Property>]
let ``float64 field`` records =
    testRoundtrip<{|
        Field1: float |}>
        records

[<Property>]
let ``decimal field`` records =
    testRoundtrip<{|
        Field1: decimal |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``date time field`` records =
    testRoundtrip<{|
        Field1: DateTime |}>
        records

[<Property>]
let ``date time offset field`` records =
    testRoundtrip<{|
        Field1: DateTimeOffset |}>
        records

[<Property>]
let ``string field`` records =
    testRoundtrip<{|
        Field1: string |}>
        records

[<Property>]
let ``byte array field`` records =
    testRoundtrip<{|
        Field1: byte[] |}>
        records

[<Property>]
let ``guid field`` records =
    testRoundtrip<{|
        Field1: Guid |}>
        records

[<Property>]
let ``array field with bool elements`` records =
    testRoundtrip<{|
        Field1: array<bool> |}>
        records

[<Property>]
let ``array field with int8 elements`` records =
    testRoundtrip<{|
        Field1: array<int8> |}>
        records

[<Property>]
let ``array field with int16 elements`` records =
    testRoundtrip<{|
        Field1: array<int16> |}>
        records

[<Property>]
let ``array field with int32 elements`` records =
    testRoundtrip<{|
        Field1: array<int> |}>
        records

[<Property>]
let ``array field with int64 elements`` records =
    testRoundtrip<{|
        Field1: array<int64> |}>
        records

[<Property>]
let ``array field with uint8 elements`` records =
    testRoundtrip<{|
        Field1: array<uint8> |}>
        records

[<Property>]
let ``array field with uint16 elements`` records =
    testRoundtrip<{|
        Field1: array<uint16> |}>
        records

[<Property>]
let ``array field with uint32 elements`` records =
    testRoundtrip<{|
        Field1: array<uint> |}>
        records

[<Property>]
let ``array field with uint64 elements`` records =
    testRoundtrip<{|
        Field1: array<uint64> |}>
        records

[<Property>]
let ``array field with float32 elements`` records =
    testRoundtrip<{|
        Field1: array<float32> |}>
        records

[<Property>]
let ``array field with float64 elements`` records =
    testRoundtrip<{|
        Field1: array<float> |}>
        records

[<Property>]
let ``array field with decimal elements`` records =
    testRoundtrip<{|
        Field1: array<decimal> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``array field with date time elements`` records =
    testRoundtrip<{|
        Field1: array<DateTime> |}>
        records

[<Property>]
let ``array field with date time offset elements`` records =
    testRoundtrip<{|
        Field1: array<DateTimeOffset> |}>
        records

[<Property>]
let ``array field with string elements`` records =
    testRoundtrip<{|
        Field1: array<string> |}>
        records

[<Property>]
let ``array field with byte array elements`` records =
    testRoundtrip<{|
        Field1: array<byte[]> |}>
        records

[<Property>]
let ``array field with guid elements`` records =
    testRoundtrip<{|
        Field1: array<Guid> |}>
        records

[<Property>]
let ``array field with array elements`` records =
    testRoundtrip<{|
        Field1: array<array<int>> |}>
        records

[<Property>]
let ``array field with generic list elements`` records =
    testRoundtrip<{|
        Field1: array<ResizeArray<int>> |}>
        records

[<Property>]
let ``array field with fsharp list elements`` records =
    testRoundtrip<{|
        Field1: array<list<int>> |}>
        records

[<Property>]
let ``array field with record elements`` records =
    testRoundtrip<{|
        Field1: array<{|
            Field2: int |}> |}>
        records

[<Property>]
let ``array field with enum union elements`` records =
    testRoundtrip<{|
        Field1: array<EnumUnion> |}>
        records

[<Property>]
let ``array field with complex union elements`` records =
    testRoundtrip<{|
        Field1: array<ComplexUnion> |}>
        records

[<Property>]
let ``array field with nullable bool elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<bool>> |}>
        records

[<Property>]
let ``array field with nullable int8 elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<int8>> |}>
        records

[<Property>]
let ``array field with nullable int16 elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<int16>> |}>
        records

[<Property>]
let ``array field with nullable int32 elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<int>> |}>
        records

[<Property>]
let ``array field with nullable int64 elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<int64>> |}>
        records

[<Property>]
let ``array field with nullable uint8 elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<uint8>> |}>
        records

[<Property>]
let ``array field with nullable uint16 elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<uint16>> |}>
        records

[<Property>]
let ``array field with nullable uint32 elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<uint>> |}>
        records

[<Property>]
let ``array field with nullable uint64 elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<uint64>> |}>
        records

[<Property>]
let ``array field with nullable float32 elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<float32>> |}>
        records

[<Property>]
let ``array field with nullable float64 elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<float>> |}>
        records

[<Property>]
let ``array field with nullable decimal elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``array field with nullable date time elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<DateTime>> |}>
        records

[<Property>]
let ``array field with nullable date time offset elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<DateTimeOffset>> |}>
        records

[<Property>]
let ``array field with nullable guid elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<Guid>> |}>
        records

[<Property>]
let ``array field with nullable record elements`` records =
    testRoundtrip<{|
        Field1: array<Nullable<struct {|
            Field2: int |}>> |}>
        records

[<Property>]
let ``array field with option bool elements`` records =
    testRoundtrip<{|
        Field1: array<option<bool>> |}>
        records

[<Property>]
let ``array field with option int8 elements`` records =
    testRoundtrip<{|
        Field1: array<option<int8>> |}>
        records

[<Property>]
let ``array field with option int16 elements`` records =
    testRoundtrip<{|
        Field1: array<option<int16>> |}>
        records

[<Property>]
let ``array field with option int32 elements`` records =
    testRoundtrip<{|
        Field1: array<option<int>> |}>
        records

[<Property>]
let ``array field with option int64 elements`` records =
    testRoundtrip<{|
        Field1: array<option<int64>> |}>
        records

[<Property>]
let ``array field with option uint8 elements`` records =
    testRoundtrip<{|
        Field1: array<option<uint8>> |}>
        records

[<Property>]
let ``array field with option uint16 elements`` records =
    testRoundtrip<{|
        Field1: array<option<uint16>> |}>
        records

[<Property>]
let ``array field with option uint32 elements`` records =
    testRoundtrip<{|
        Field1: array<option<uint>> |}>
        records

[<Property>]
let ``array field with option uint64 elements`` records =
    testRoundtrip<{|
        Field1: array<option<uint64>> |}>
        records

[<Property>]
let ``array field with option float32 elements`` records =
    testRoundtrip<{|
        Field1: array<option<float32>> |}>
        records

[<Property>]
let ``array field with option float64 elements`` records =
    testRoundtrip<{|
        Field1: array<option<float>> |}>
        records

[<Property>]
let ``array field with option decimal elements`` records =
    testRoundtrip<{|
        Field1: array<option<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``array field with option date time elements`` records =
    testRoundtrip<{|
        Field1: array<option<DateTime>> |}>
        records

[<Property>]
let ``array field with option date time offset elements`` records =
    testRoundtrip<{|
        Field1: array<option<DateTimeOffset>> |}>
        records

[<Property>]
let ``array field with option string elements`` records =
    testRoundtrip<{|
        Field1: array<option<string>> |}>
        records

[<Property>]
let ``array field with option byte array elements`` records =
    testRoundtrip<{|
        Field1: array<option<byte[]>> |}>
        records

[<Property>]
let ``array field with option guid elements`` records =
    testRoundtrip<{|
        Field1: array<option<Guid>> |}>
        records

[<Property>]
let ``array field with option array elements`` records =
    testRoundtrip<{|
        Field1: array<option<array<int>>> |}>
        records

[<Property>]
let ``array field with option generic list elements`` records =
    testRoundtrip<{|
        Field1: array<option<ResizeArray<int>>> |}>
        records

[<Property>]
let ``array field with option fsharp list elements`` records =
    testRoundtrip<{|
        Field1: array<option<list<int>>> |}>
        records

[<Property>]
let ``array field with option record elements`` records =
    testRoundtrip<{|
        Field1: array<option<{|
            Field2: int |}>> |}>
        records

[<Property>]
let ``array field with option enum union elements`` records =
    testRoundtrip<{|
        Field1: array<option<EnumUnion>> |}>
        records

[<Property>]
let ``array field with option complex union elements`` records =
    testRoundtrip<{|
        Field1: array<option<ComplexUnion>> |}>
        records

[<Property>]
let ``array field with option nullable elements`` records =
    testRoundtrip<{|
        Field1: array<option<Nullable<int>>> |}>
        records

[<Property>]
let ``array field with option option elements`` records =
    testRoundtrip<{|
        Field1: array<option<option<int>>> |}>
        records

[<Property>]
let ``generic list field with bool elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<bool> |}>
        records

[<Property>]
let ``generic list field with int8 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<int8> |}>
        records

[<Property>]
let ``generic list field with int16 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<int16> |}>
        records

[<Property>]
let ``generic list field with int32 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<int> |}>
        records

[<Property>]
let ``generic list field with int64 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<int64> |}>
        records

[<Property>]
let ``generic list field with uint8 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<uint8> |}>
        records

[<Property>]
let ``generic list field with uint16 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<uint16> |}>
        records

[<Property>]
let ``generic list field with uint32 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<uint> |}>
        records

[<Property>]
let ``generic list field with uint64 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<uint64> |}>
        records

[<Property>]
let ``generic list field with float32 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<float32> |}>
        records

[<Property>]
let ``generic list field with float64 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<float> |}>
        records

[<Property>]
let ``generic list field with decimal elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<decimal> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``generic list field with date time elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<DateTime> |}>
        records

[<Property>]
let ``generic list field with date time offset elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<DateTimeOffset> |}>
        records

[<Property>]
let ``generic list field with string elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<string> |}>
        records

[<Property>]
let ``generic list field with byte array elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<byte[]> |}>
        records

[<Property>]
let ``generic list field with guid elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Guid> |}>
        records

[<Property>]
let ``generic list field with array elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<array<int>> |}>
        records

[<Property>]
let ``generic list field with generic list elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<ResizeArray<int>> |}>
        records

[<Property>]
let ``generic list field with fsharp list elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<list<int>> |}>
        records

[<Property>]
let ``generic list field with record elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<{|
            Field2: int |}> |}>
        records

[<Property>]
let ``generic list field with enum union elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<EnumUnion> |}>
        records

[<Property>]
let ``generic list field with complex union elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<ComplexUnion> |}>
        records

[<Property>]
let ``generic list field with nullable bool elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<bool>> |}>
        records

[<Property>]
let ``generic list field with nullable int8 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<int8>> |}>
        records

[<Property>]
let ``generic list field with nullable int16 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<int16>> |}>
        records

[<Property>]
let ``generic list field with nullable int32 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<int>> |}>
        records

[<Property>]
let ``generic list field with nullable int64 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<int64>> |}>
        records

[<Property>]
let ``generic list field with nullable uint8 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<uint8>> |}>
        records

[<Property>]
let ``generic list field with nullable uint16 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<uint16>> |}>
        records

[<Property>]
let ``generic list field with nullable uint32 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<uint>> |}>
        records

[<Property>]
let ``generic list field with nullable uint64 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<uint64>> |}>
        records

[<Property>]
let ``generic list field with nullable float32 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<float32>> |}>
        records

[<Property>]
let ``generic list field with nullable float64 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<float>> |}>
        records

[<Property>]
let ``generic list field with nullable decimal elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``generic list field with nullable date time elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<DateTime>> |}>
        records

[<Property>]
let ``generic list field with nullable date time offset elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<DateTimeOffset>> |}>
        records

[<Property>]
let ``generic list field with nullable guid elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<Guid>> |}>
        records

[<Property>]
let ``generic list field with nullable record elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<struct {|
            Field2: int |}>> |}>
        records

[<Property>]
let ``generic list field with option bool elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<bool>> |}>
        records

[<Property>]
let ``generic list field with option int8 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<int8>> |}>
        records

[<Property>]
let ``generic list field with option int16 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<int16>> |}>
        records

[<Property>]
let ``generic list field with option int32 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<int>> |}>
        records

[<Property>]
let ``generic list field with option int64 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<int64>> |}>
        records

[<Property>]
let ``generic list field with option uint8 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<uint8>> |}>
        records

[<Property>]
let ``generic list field with option uint16 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<uint16>> |}>
        records

[<Property>]
let ``generic list field with option uint32 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<uint>> |}>
        records

[<Property>]
let ``generic list field with option uint64 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<uint64>> |}>
        records

[<Property>]
let ``generic list field with option float32 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<float32>> |}>
        records

[<Property>]
let ``generic list field with option float64 elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<float>> |}>
        records

[<Property>]
let ``generic list field with option decimal elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``generic list field with option date time elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<DateTime>> |}>
        records

[<Property>]
let ``generic list field with option date time offset elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<DateTimeOffset>> |}>
        records

[<Property>]
let ``generic list field with option string elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<string>> |}>
        records

[<Property>]
let ``generic list field with option byte array elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<byte[]>> |}>
        records

[<Property>]
let ``generic list field with option guid elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<Guid>> |}>
        records

[<Property>]
let ``generic list field with option array elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<array<int>>> |}>
        records

[<Property>]
let ``generic list field with option generic list elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<ResizeArray<int>>> |}>
        records

[<Property>]
let ``generic list field with option fsharp list elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<list<int>>> |}>
        records

[<Property>]
let ``generic list field with option record elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<{|
            Field2: int |}>> |}>
        records

[<Property>]
let ``generic list field with option enum union elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<EnumUnion>> |}>
        records

[<Property>]
let ``generic list field with option complex union elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<ComplexUnion>> |}>
        records

[<Property>]
let ``generic list field with option nullable elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<Nullable<int>>> |}>
        records

[<Property>]
let ``generic list field with option option elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<option<int>>> |}>
        records

[<Property>]
let ``fsharp list field with bool elements`` records =
    testRoundtrip<{|
        Field1: list<bool> |}>
        records

[<Property>]
let ``fsharp list field with int8 elements`` records =
    testRoundtrip<{|
        Field1: list<int8> |}>
        records

[<Property>]
let ``fsharp list field with int16 elements`` records =
    testRoundtrip<{|
        Field1: list<int16> |}>
        records

[<Property>]
let ``fsharp list field with int32 elements`` records =
    testRoundtrip<{|
        Field1: list<int> |}>
        records

[<Property>]
let ``fsharp list field with int64 elements`` records =
    testRoundtrip<{|
        Field1: list<int64> |}>
        records

[<Property>]
let ``fsharp list field with uint8 elements`` records =
    testRoundtrip<{|
        Field1: list<uint8> |}>
        records

[<Property>]
let ``fsharp list field with uint16 elements`` records =
    testRoundtrip<{|
        Field1: list<uint16> |}>
        records

[<Property>]
let ``fsharp list field with uint32 elements`` records =
    testRoundtrip<{|
        Field1: list<uint> |}>
        records

[<Property>]
let ``fsharp list field with uint64 elements`` records =
    testRoundtrip<{|
        Field1: list<uint64> |}>
        records

[<Property>]
let ``fsharp list field with float32 elements`` records =
    testRoundtrip<{|
        Field1: list<float32> |}>
        records

[<Property>]
let ``fsharp list field with float64 elements`` records =
    testRoundtrip<{|
        Field1: list<float> |}>
        records

[<Property>]
let ``fsharp list field with decimal elements`` records =
    testRoundtrip<{|
        Field1: list<decimal> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``fsharp list field with date time elements`` records =
    testRoundtrip<{|
        Field1: list<DateTime> |}>
        records

[<Property>]
let ``fsharp list field with date time offset elements`` records =
    testRoundtrip<{|
        Field1: list<DateTimeOffset> |}>
        records

[<Property>]
let ``fsharp list field with string elements`` records =
    testRoundtrip<{|
        Field1: list<string> |}>
        records

[<Property>]
let ``fsharp list field with byte array elements`` records =
    testRoundtrip<{|
        Field1: list<byte[]> |}>
        records

[<Property>]
let ``fsharp list field with guid elements`` records =
    testRoundtrip<{|
        Field1: list<Guid> |}>
        records

[<Property>]
let ``fsharp list field with array elements`` records =
    testRoundtrip<{|
        Field1: list<array<int>> |}>
        records

[<Property>]
let ``fsharp list field with generic list elements`` records =
    testRoundtrip<{|
        Field1: list<ResizeArray<int>> |}>
        records

[<Property>]
let ``fsharp list field with fsharp list elements`` records =
    testRoundtrip<{|
        Field1: list<list<int>> |}>
        records

[<Property>]
let ``fsharp list field with record elements`` records =
    testRoundtrip<{|
        Field1: list<{|
            Field2: int |}> |}>
        records

[<Property>]
let ``fsharp list field with enum union elements`` records =
    testRoundtrip<{|
        Field1: list<EnumUnion> |}>
        records

[<Property>]
let ``fsharp list field with complex union elements`` records =
    testRoundtrip<{|
        Field1: list<ComplexUnion> |}>
        records

[<Property>]
let ``fsharp list field with nullable bool elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<bool>> |}>
        records

[<Property>]
let ``fsharp list field with nullable int8 elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<int8>> |}>
        records

[<Property>]
let ``fsharp list field with nullable int16 elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<int16>> |}>
        records

[<Property>]
let ``fsharp list field with nullable int32 elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<int>> |}>
        records

[<Property>]
let ``fsharp list field with nullable int64 elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<int64>> |}>
        records

[<Property>]
let ``fsharp list field with nullable uint8 elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<uint8>> |}>
        records

[<Property>]
let ``fsharp list field with nullable uint16 elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<uint16>> |}>
        records

[<Property>]
let ``fsharp list field with nullable uint32 elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<uint>> |}>
        records

[<Property>]
let ``fsharp list field with nullable uint64 elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<uint64>> |}>
        records

[<Property>]
let ``fsharp list field with nullable float32 elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<float32>> |}>
        records

[<Property>]
let ``fsharp list field with nullable float64 elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<float>> |}>
        records

[<Property>]
let ``fsharp list field with nullable decimal elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``fsharp list field with nullable date time elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<DateTime>> |}>
        records

[<Property>]
let ``fsharp list field with nullable date time offset elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<DateTimeOffset>> |}>
        records

[<Property>]
let ``fsharp list field with nullable guid elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<Guid>> |}>
        records

[<Property>]
let ``fsharp list field with nullable record elements`` records =
    testRoundtrip<{|
        Field1: list<Nullable<struct {|
            Field2: int |}>> |}>
        records

[<Property>]
let ``fsharp list field with option bool elements`` records =
    testRoundtrip<{|
        Field1: list<option<bool>> |}>
        records

[<Property>]
let ``fsharp list field with option int8 elements`` records =
    testRoundtrip<{|
        Field1: list<option<int8>> |}>
        records

[<Property>]
let ``fsharp list field with option int16 elements`` records =
    testRoundtrip<{|
        Field1: list<option<int16>> |}>
        records

[<Property>]
let ``fsharp list field with option int32 elements`` records =
    testRoundtrip<{|
        Field1: list<option<int>> |}>
        records

[<Property>]
let ``fsharp list field with option int64 elements`` records =
    testRoundtrip<{|
        Field1: list<option<int64>> |}>
        records

[<Property>]
let ``fsharp list field with option uint8 elements`` records =
    testRoundtrip<{|
        Field1: list<option<uint8>> |}>
        records

[<Property>]
let ``fsharp list field with option uint16 elements`` records =
    testRoundtrip<{|
        Field1: list<option<uint16>> |}>
        records

[<Property>]
let ``fsharp list field with option uint32 elements`` records =
    testRoundtrip<{|
        Field1: list<option<uint>> |}>
        records

[<Property>]
let ``fsharp list field with option uint64 elements`` records =
    testRoundtrip<{|
        Field1: list<option<uint64>> |}>
        records

[<Property>]
let ``fsharp list field with option float32 elements`` records =
    testRoundtrip<{|
        Field1: list<option<float32>> |}>
        records

[<Property>]
let ``fsharp list field with option float64 elements`` records =
    testRoundtrip<{|
        Field1: list<option<float>> |}>
        records

[<Property>]
let ``fsharp list field with option decimal elements`` records =
    testRoundtrip<{|
        Field1: list<option<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``fsharp list field with option date time elements`` records =
    testRoundtrip<{|
        Field1: list<option<DateTime>> |}>
        records

[<Property>]
let ``fsharp list field with option date time offset elements`` records =
    testRoundtrip<{|
        Field1: list<option<DateTimeOffset>> |}>
        records

[<Property>]
let ``fsharp list field with option string elements`` records =
    testRoundtrip<{|
        Field1: list<option<string>> |}>
        records

[<Property>]
let ``fsharp list field with option byte array elements`` records =
    testRoundtrip<{|
        Field1: list<option<byte[]>> |}>
        records

[<Property>]
let ``fsharp list field with option guid elements`` records =
    testRoundtrip<{|
        Field1: list<option<Guid>> |}>
        records

[<Property>]
let ``fsharp list field with option array elements`` records =
    testRoundtrip<{|
        Field1: list<option<array<int>>> |}>
        records

[<Property>]
let ``fsharp list field with option generic list elements`` records =
    testRoundtrip<{|
        Field1: list<option<ResizeArray<int>>> |}>
        records

[<Property>]
let ``fsharp list field with option fsharp list elements`` records =
    testRoundtrip<{|
        Field1: list<option<list<int>>> |}>
        records

[<Property>]
let ``fsharp list field with option record elements`` records =
    testRoundtrip<{|
        Field1: list<option<{|
            Field2: int |}>> |}>
        records

[<Property>]
let ``fsharp list field with option enum union elements`` records =
    testRoundtrip<{|
        Field1: list<option<EnumUnion>> |}>
        records

[<Property>]
let ``fsharp list field with option complex union elements`` records =
    testRoundtrip<{|
        Field1: list<option<ComplexUnion>> |}>
        records

[<Property>]
let ``fsharp list field with option nullable elements`` records =
    testRoundtrip<{|
        Field1: list<option<Nullable<int>>> |}>
        records

[<Property>]
let ``fsharp list field with option option elements`` records =
    testRoundtrip<{|
        Field1: list<option<option<int>>> |}>
        records

[<Property>]
let ``record field with bool field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: bool |} |}>
        records

[<Property>]
let ``record field with int8 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: int8 |} |}>
        records

[<Property>]
let ``record field with int16 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: int16 |} |}>
        records

[<Property>]
let ``record field with int32 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: int |} |}>
        records

[<Property>]
let ``record field with int64 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: int64 |} |}>
        records

[<Property>]
let ``record field with uint8 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: uint8 |} |}>
        records

[<Property>]
let ``record field with uint16 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: uint16 |} |}>
        records

[<Property>]
let ``record field with uint32 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: uint |} |}>
        records

[<Property>]
let ``record field with uint64 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: uint64 |} |}>
        records

[<Property>]
let ``record field with float32 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: float32 |} |}>
        records

[<Property>]
let ``record field with float64 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: float |} |}>
        records

[<Property>]
let ``record field with decimal field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: decimal |} |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``record field with date time field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: DateTime |} |}>
        records

[<Property>]
let ``record field with date time offset field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: DateTimeOffset |} |}>
        records

[<Property>]
let ``record field with string field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: string |} |}>
        records

[<Property>]
let ``record field with byte array field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: byte[] |} |}>
        records

[<Property>]
let ``record field with guid field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Guid |} |}>
        records

[<Property>]
let ``record field with array field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: array<int> |} |}>
        records

[<Property>]
let ``record field with generic list field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: ResizeArray<int> |} |}>
        records

[<Property>]
let ``record field with fsharp list field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: list<int> |} |}>
        records

[<Property>]
let ``record field with record field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: {|
                Field3: int |} |} |}>
        records

[<Property>]
let ``record field with enum union field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: EnumUnion |} |}>
        records

[<Property>]
let ``record field with complex union field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: ComplexUnion |} |}>
        records

[<Property>]
let ``record field with nullable bool field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<bool> |} |}>
        records

[<Property>]
let ``record field with nullable int8 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<int8> |} |}>
        records

[<Property>]
let ``record field with nullable int16 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<int16> |} |}>
        records

[<Property>]
let ``record field with nullable int32 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<int> |} |}>
        records

[<Property>]
let ``record field with nullable int64 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<int64> |} |}>
        records

[<Property>]
let ``record field with nullable uint8 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<uint8> |} |}>
        records

[<Property>]
let ``record field with nullable uint16 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<uint16> |} |}>
        records

[<Property>]
let ``record field with nullable uint32 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<uint> |} |}>
        records

[<Property>]
let ``record field with nullable uint64 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<uint64> |} |}>
        records

[<Property>]
let ``record field with nullable float32 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<float32> |} |}>
        records

[<Property>]
let ``record field with nullable float64 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<float> |} |}>
        records

[<Property>]
let ``record field with nullable decimal field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<decimal> |} |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``record field with nullable date time field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<DateTime> |} |}>
        records

[<Property>]
let ``record field with nullable date time offset field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<DateTimeOffset> |} |}>
        records

[<Property>]
let ``record field with nullable guid field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<Guid> |} |}>
        records

[<Property>]
let ``record field with nullable record field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<struct {|
                Field3: int |}> |} |}>
        records

[<Property>]
let ``record field with option bool field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<bool> |} |}>
        records

[<Property>]
let ``record field with option int8 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<int8> |} |}>
        records

[<Property>]
let ``record field with option int16 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<int16> |} |}>
        records

[<Property>]
let ``record field with option int32 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<int> |} |}>
        records

[<Property>]
let ``record field with option int64 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<int64> |} |}>
        records

[<Property>]
let ``record field with option uint8 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<uint8> |} |}>
        records

[<Property>]
let ``record field with option uint16 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<uint16> |} |}>
        records

[<Property>]
let ``record field with option uint32 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<uint> |} |}>
        records

[<Property>]
let ``record field with option uint64 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<uint64> |} |}>
        records

[<Property>]
let ``record field with option float32 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<float32> |} |}>
        records

[<Property>]
let ``record field with option float64 field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<float> |} |}>
        records

[<Property>]
let ``record field with option decimal field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<decimal> |} |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``record field with option date time field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<DateTime> |} |}>
        records

[<Property>]
let ``record field with option date time offset field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<DateTimeOffset> |} |}>
        records

[<Property>]
let ``record field with option string field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<string> |} |}>
        records

[<Property>]
let ``record field with option byte array field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<byte[]> |} |}>
        records

[<Property>]
let ``record field with option guid field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<Guid> |} |}>
        records

[<Property>]
let ``record field with option array field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<array<int>> |} |}>
        records

[<Property>]
let ``record field with option generic list field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<ResizeArray<int>> |} |}>
        records

[<Property>]
let ``record field with option fsharp list field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<list<int>> |} |}>
        records

[<Property>]
let ``record field with option record field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<{|
                Field3: int |}> |} |}>
        records

[<Property>]
let ``record field with option enum union field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<EnumUnion> |} |}>
        records

[<Property>]
let ``record field with option complex union field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<ComplexUnion> |} |}>
        records

[<Property>]
let ``record field with option nullable field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<Nullable<int>> |} |}>
        records

[<Property>]
let ``record field with option option field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<option<int>> |} |}>
        records

[<Property>]
let ``enum union field`` records =
    testRoundtrip<{|
        Field1: EnumUnion |}>
        records

[<Property>]
let ``complex union field`` records =
    testRoundtrip<{|
        Field1: ComplexUnion |}>
        records

[<Property>]
let ``complex union field with bool case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithBoolCase |}>
        records

[<Property>]
let ``complex union field with int8 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithInt8Case |}>
        records

[<Property>]
let ``complex union field with int16 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithInt16Case |}>
        records

[<Property>]
let ``complex union field with int32 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithInt32Case |}>
        records

[<Property>]
let ``complex union field with int64 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithInt64Case |}>
        records

[<Property>]
let ``complex union field with uint8 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithUInt8Case |}>
        records

[<Property>]
let ``complex union field with uint16 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithUInt16Case |}>
        records

[<Property>]
let ``complex union field with uint32 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithUInt32Case |}>
        records

[<Property>]
let ``complex union field with uint64 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithUInt64Case |}>
        records

[<Property>]
let ``complex union field with float32 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithFloat32Case |}>
        records

[<Property>]
let ``complex union field with float64 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithFloat64Case |}>
        records

[<Property>]
let ``complex union field with decimal case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithDecimalCase |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``complex union field with date time case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithDateTimeCase |}>
        records

[<Property>]
let ``complex union field with date time offset case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithDateTimeOffsetCase |}>
        records

[<Property>]
let ``complex union field with string case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithStringCase |}>
        records

[<Property>]
let ``complex union field with byte array case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithByteArrayCase |}>
        records

[<Property>]
let ``complex union field with guid case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithGuidCase |}>
        records

[<Property>]
let ``complex union field with array case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithArrayCase |}>
        records

[<Property>]
let ``complex union field with generic list case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithGenericListCase |}>
        records

[<Property>]
let ``complex union field with fsharp list case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithFSharpListCase |}>
        records

[<Property>]
let ``complex union field with record case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithRecordCase |}>
        records

[<Property>]
let ``complex union field with enum union case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithEnumUnionCase |}>
        records

[<Property>]
let ``complex union field with complex union case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithComplexUnionCase |}>
        records

[<Property>]
let ``complex union field with nullable bool case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableBoolCase |}>
        records

[<Property>]
let ``complex union field with nullable int8 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableInt8Case |}>
        records

[<Property>]
let ``complex union field with nullable int16 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableInt16Case |}>
        records

[<Property>]
let ``complex union field with nullable int32 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableInt32Case |}>
        records

[<Property>]
let ``complex union field with nullable int64 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableInt64Case |}>
        records

[<Property>]
let ``complex union field with nullable uint8 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableUInt8Case |}>
        records

[<Property>]
let ``complex union field with nullable uint16 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableUInt16Case |}>
        records

[<Property>]
let ``complex union field with nullable uint32 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableUInt32Case |}>
        records

[<Property>]
let ``complex union field with nullable uint64 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableUInt64Case |}>
        records

[<Property>]
let ``complex union field with nullable float32 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableFloat32Case |}>
        records

[<Property>]
let ``complex union field with nullable float64 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableFloat64Case |}>
        records

[<Property>]
let ``complex union field with nullable decimal case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableDecimalCase |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``complex union field with nullable date time case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableDateTimeCase |}>
        records

[<Property>]
let ``complex union field with nullable date time offset case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableDateTimeOffsetCase |}>
        records

[<Property>]
let ``complex union field with nullable guid case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableGuidCase |}>
        records

[<Property>]
let ``complex union field with nullable record case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableRecordCase |}>
        records

[<Property>]
let ``complex union field with option bool case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionBoolCase |}>
        records

[<Property>]
let ``complex union field with option int8 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionInt8Case |}>
        records

[<Property>]
let ``complex union field with option int16 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionInt16Case |}>
        records

[<Property>]
let ``complex union field with option int32 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionInt32Case |}>
        records

[<Property>]
let ``complex union field with option int64 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionInt64Case |}>
        records

[<Property>]
let ``complex union field with option uint8 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionUInt8Case |}>
        records

[<Property>]
let ``complex union field with option uint16 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionUInt16Case |}>
        records

[<Property>]
let ``complex union field with option uint32 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionUInt32Case |}>
        records

[<Property>]
let ``complex union field with option uint64 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionUInt64Case |}>
        records

[<Property>]
let ``complex union field with option float32 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionFloat32Case |}>
        records

[<Property>]
let ``complex union field with option float64 case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionFloat64Case |}>
        records

[<Property>]
let ``complex union field with option decimal case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionDecimalCase |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``complex union field with option date time case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionDateTimeCase |}>
        records

[<Property>]
let ``complex union field with option date time offset case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionDateTimeOffsetCase |}>
        records

[<Property>]
let ``complex union field with option string case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionStringCase |}>
        records

[<Property>]
let ``complex union field with option byte array case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionByteArrayCase |}>
        records

[<Property>]
let ``complex union field with option guid case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionGuidCase |}>
        records

[<Property>]
let ``complex union field with option array case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionArrayCase |}>
        records

[<Property>]
let ``complex union field with option generic list case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionGenericListCase |}>
        records

[<Property>]
let ``complex union field with option fsharp list case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionFSharpListCase |}>
        records

[<Property>]
let ``complex union field with option record case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionRecordCase |}>
        records

[<Property>]
let ``complex union field with option enum union case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionEnumUnionCase |}>
        records

[<Property>]
let ``complex union field with option complex union case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionComplexUnionCase |}>
        records

[<Property>]
let ``complex union field with option nullable case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionNullableCase |}>
        records

[<Property>]
let ``complex union field with option option case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionOptionCase |}>
        records

[<Property>]
let ``nullable bool field`` records =
    testRoundtrip<{|
        Field1: Nullable<bool> |}>
        records

[<Property>]
let ``nullable int8 field`` records =
    testRoundtrip<{|
        Field1: Nullable<int8> |}>
        records

[<Property>]
let ``nullable int16 field`` records =
    testRoundtrip<{|
        Field1: Nullable<int16> |}>
        records

[<Property>]
let ``nullable int32 field`` records =
    testRoundtrip<{|
        Field1: Nullable<int> |}>
        records

[<Property>]
let ``nullable int64 field`` records =
    testRoundtrip<{|
        Field1: Nullable<int64> |}>
        records

[<Property>]
let ``nullable uint8 field`` records =
    testRoundtrip<{|
        Field1: Nullable<uint8> |}>
        records

[<Property>]
let ``nullable uint16 field`` records =
    testRoundtrip<{|
        Field1: Nullable<uint16> |}>
        records

[<Property>]
let ``nullable uint32 field`` records =
    testRoundtrip<{|
        Field1: Nullable<uint> |}>
        records

[<Property>]
let ``nullable uint64 field`` records =
    testRoundtrip<{|
        Field1: Nullable<uint64> |}>
        records

[<Property>]
let ``nullable float32 field`` records =
    testRoundtrip<{|
        Field1: Nullable<float32> |}>
        records

[<Property>]
let ``nullable float64 field`` records =
    testRoundtrip<{|
        Field1: Nullable<float> |}>
        records

[<Property>]
let ``nullable decimal field`` records =
    testRoundtrip<{|
        Field1: Nullable<decimal> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``nullable date time field`` records =
    testRoundtrip<{|
        Field1: Nullable<DateTime> |}>
        records

[<Property>]
let ``nullable date time offset field`` records =
    testRoundtrip<{|
        Field1: Nullable<DateTimeOffset> |}>
        records

[<Property>]
let ``nullable guid field`` records =
    testRoundtrip<{|
        Field1: Nullable<Guid> |}>
        records

[<Property>]
let ``nullable record field with bool field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: bool |}> |}>
        records

[<Property>]
let ``nullable record field with int8 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: int8 |}> |}>
        records

[<Property>]
let ``nullable record field with int16 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: int16 |}> |}>
        records

[<Property>]
let ``nullable record field with int32 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: int |}> |}>
        records

[<Property>]
let ``nullable record field with int64 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: int64 |}> |}>
        records

[<Property>]
let ``nullable record field with uint8 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: uint8 |}> |}>
        records

[<Property>]
let ``nullable record field with uint16 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: uint16 |}> |}>
        records

[<Property>]
let ``nullable record field with uint32 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: uint |}> |}>
        records

[<Property>]
let ``nullable record field with uint64 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: uint64 |}> |}>
        records

[<Property>]
let ``nullable record field with float32 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: float32 |}> |}>
        records

[<Property>]
let ``nullable record field with float64 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: float |}> |}>
        records

[<Property>]
let ``nullable record field with decimal field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: decimal |}> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``nullable record field with date time field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: DateTime |}> |}>
        records

[<Property>]
let ``nullable record field with date time offset field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: DateTimeOffset |}> |}>
        records

[<Property>]
let ``nullable record field with string field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: string |}> |}>
        records

[<Property>]
let ``nullable record field with byte array field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: byte[] |}> |}>
        records

[<Property>]
let ``nullable record field with guid field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Guid |}> |}>
        records

[<Property>]
let ``nullable record field with array field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: array<int> |}> |}>
        records

[<Property>]
let ``nullable record field with generic list field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: ResizeArray<int> |}> |}>
        records

[<Property>]
let ``nullable record field with record field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: struct {|
                Field3: int |} |}> |}>
        records

[<Property>]
let ``nullable record field with nullable bool field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<bool> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable int8 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<int8> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable int16 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<int16> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable int32 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<int> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable int64 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<int64> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable uint8 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<uint8> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable uint16 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<uint16> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable uint32 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<uint> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable uint64 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<uint64> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable float32 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<float32> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable float64 field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<float> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable decimal field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<decimal> |}> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``nullable record field with nullable date time field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<DateTime> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable date time offset field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<DateTimeOffset> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable guid field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<Guid> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable record field`` records =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<struct {|
                Field3: int |}> |}> |}>
        records

[<Property>]
let ``option bool field`` records =
    testRoundtrip<{|
        Field1: option<bool> |}>
        records

[<Property>]
let ``option int8 field`` records =
    testRoundtrip<{|
        Field1: option<int8> |}>
        records

[<Property>]
let ``option int16 field`` records =
    testRoundtrip<{|
        Field1: option<int16> |}>
        records

[<Property>]
let ``option int32 field`` records =
    testRoundtrip<{|
        Field1: option<int> |}>
        records

[<Property>]
let ``option int64 field`` records =
    testRoundtrip<{|
        Field1: option<int64> |}>
        records

[<Property>]
let ``option uint8 field`` records =
    testRoundtrip<{|
        Field1: option<uint8> |}>
        records

[<Property>]
let ``option uint16 field`` records =
    testRoundtrip<{|
        Field1: option<uint16> |}>
        records

[<Property>]
let ``option uint32 field`` records =
    testRoundtrip<{|
        Field1: option<uint> |}>
        records

[<Property>]
let ``option uint64 field`` records =
    testRoundtrip<{|
        Field1: option<uint64> |}>
        records

[<Property>]
let ``option float32 field`` records =
    testRoundtrip<{|
        Field1: option<float32> |}>
        records

[<Property>]
let ``option float64 field`` records =
    testRoundtrip<{|
        Field1: option<float> |}>
        records

[<Property>]
let ``option decimal field`` records =
    testRoundtrip<{|
        Field1: option<decimal> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option date time field`` records =
    testRoundtrip<{|
        Field1: option<DateTime> |}>
        records

[<Property>]
let ``option date time offset field`` records =
    testRoundtrip<{|
        Field1: option<DateTimeOffset> |}>
        records

[<Property>]
let ``option string field`` records =
    testRoundtrip<{|
        Field1: option<string> |}>
        records

[<Property>]
let ``option byte array field`` records =
    testRoundtrip<{|
        Field1: option<byte[]> |}>
        records

[<Property>]
let ``option guid field`` records =
    testRoundtrip<{|
        Field1: option<Guid> |}>
        records

[<Property>]
let ``option array field with bool elements`` records =
    testRoundtrip<{|
        Field1: option<array<bool>> |}>
        records

[<Property>]
let ``option array field with int8 elements`` records =
    testRoundtrip<{|
        Field1: option<array<int8>> |}>
        records

[<Property>]
let ``option array field with int16 elements`` records =
    testRoundtrip<{|
        Field1: option<array<int16>> |}>
        records

[<Property>]
let ``option array field with int32 elements`` records =
    testRoundtrip<{|
        Field1: option<array<int>> |}>
        records

[<Property>]
let ``option array field with int64 elements`` records =
    testRoundtrip<{|
        Field1: option<array<int64>> |}>
        records

[<Property>]
let ``option array field with uint8 elements`` records =
    testRoundtrip<{|
        Field1: option<array<uint8>> |}>
        records

[<Property>]
let ``option array field with uint16 elements`` records =
    testRoundtrip<{|
        Field1: option<array<uint16>> |}>
        records

[<Property>]
let ``option array field with uint32 elements`` records =
    testRoundtrip<{|
        Field1: option<array<uint>> |}>
        records

[<Property>]
let ``option array field with uint64 elements`` records =
    testRoundtrip<{|
        Field1: option<array<uint64>> |}>
        records

[<Property>]
let ``option array field with float32 elements`` records =
    testRoundtrip<{|
        Field1: option<array<float32>> |}>
        records

[<Property>]
let ``option array field with float64 elements`` records =
    testRoundtrip<{|
        Field1: option<array<float>> |}>
        records

[<Property>]
let ``option array field with decimal elements`` records =
    testRoundtrip<{|
        Field1: option<array<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option array field with date time elements`` records =
    testRoundtrip<{|
        Field1: option<array<DateTime>> |}>
        records

[<Property>]
let ``option array field with date time offset elements`` records =
    testRoundtrip<{|
        Field1: option<array<DateTimeOffset>> |}>
        records

[<Property>]
let ``option array field with string elements`` records =
    testRoundtrip<{|
        Field1: option<array<string>> |}>
        records

[<Property>]
let ``option array field with byte array elements`` records =
    testRoundtrip<{|
        Field1: option<array<byte[]>> |}>
        records

[<Property>]
let ``option array field with guid elements`` records =
    testRoundtrip<{|
        Field1: option<array<Guid>> |}>
        records

[<Property>]
let ``option array field with array elements`` records =
    testRoundtrip<{|
        Field1: option<array<array<int>>> |}>
        records

[<Property>]
let ``option array field with generic list elements`` records =
    testRoundtrip<{|
        Field1: option<array<ResizeArray<int>>> |}>
        records

[<Property>]
let ``option array field with fsharp list elements`` records =
    testRoundtrip<{|
        Field1: option<array<list<int>>> |}>
        records

[<Property>]
let ``option array field with record elements`` records =
    testRoundtrip<{|
        Field1: option<array<{|
            Field2: int |}>> |}>
        records

[<Property>]
let ``option array field with enum union elements`` records =
    testRoundtrip<{|
        Field1: option<array<EnumUnion>> |}>
        records

[<Property>]
let ``option array field with complex union elements`` records =
    testRoundtrip<{|
        Field1: option<array<ComplexUnion>> |}>
        records

[<Property>]
let ``option array field with nullable bool elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<bool>>> |}>
        records

[<Property>]
let ``option array field with nullable int8 elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<int8>>> |}>
        records

[<Property>]
let ``option array field with nullable int16 elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<int16>>> |}>
        records

[<Property>]
let ``option array field with nullable int32 elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<int>>> |}>
        records

[<Property>]
let ``option array field with nullable int64 elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<int64>>> |}>
        records

[<Property>]
let ``option array field with nullable uint8 elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<uint8>>> |}>
        records

[<Property>]
let ``option array field with nullable uint16 elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<uint16>>> |}>
        records

[<Property>]
let ``option array field with nullable uint32 elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<uint>>> |}>
        records

[<Property>]
let ``option array field with nullable uint64 elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<uint64>>> |}>
        records

[<Property>]
let ``option array field with nullable float32 elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<float32>>> |}>
        records

[<Property>]
let ``option array field with nullable float64 elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<float>>> |}>
        records

[<Property>]
let ``option array field with nullable decimal elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<decimal>>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option array field with nullable date time elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<DateTime>>> |}>
        records

[<Property>]
let ``option array field with nullable date time offset elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<DateTimeOffset>>> |}>
        records

[<Property>]
let ``option array field with nullable guid elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<Guid>>> |}>
        records

[<Property>]
let ``option array field with nullable record elements`` records =
    testRoundtrip<{|
        Field1: option<array<Nullable<struct {|
            Field2: int |}>>> |}>
        records

[<Property>]
let ``option array field with option bool elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<bool>>> |}>
        records

[<Property>]
let ``option array field with option int8 elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<int8>>> |}>
        records

[<Property>]
let ``option array field with option int16 elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<int16>>> |}>
        records

[<Property>]
let ``option array field with option int32 elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<int>>> |}>
        records

[<Property>]
let ``option array field with option int64 elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<int64>>> |}>
        records

[<Property>]
let ``option array field with option uint8 elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<uint8>>> |}>
        records

[<Property>]
let ``option array field with option uint16 elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<uint16>>> |}>
        records

[<Property>]
let ``option array field with option uint32 elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<uint>>> |}>
        records

[<Property>]
let ``option array field with option uint64 elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<uint64>>> |}>
        records

[<Property>]
let ``option array field with option float32 elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<float32>>> |}>
        records

[<Property>]
let ``option array field with option float64 elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<float>>> |}>
        records

[<Property>]
let ``option array field with option decimal elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<decimal>>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option array field with option date time elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<DateTime>>> |}>
        records

[<Property>]
let ``option array field with option date time offset elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<DateTimeOffset>>> |}>
        records

[<Property>]
let ``option array field with option string elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<string>>> |}>
        records

[<Property>]
let ``option array field with option byte array elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<byte[]>>> |}>
        records

[<Property>]
let ``option array field with option guid elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<Guid>>> |}>
        records

[<Property>]
let ``option array field with option array elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<array<int>>>> |}>
        records

[<Property>]
let ``option array field with option generic list elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<ResizeArray<int>>>> |}>
        records

[<Property>]
let ``option array field with option fsharp list elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<list<int>>>> |}>
        records

[<Property>]
let ``option array field with option record elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<{|
            Field2: int |}>>> |}>
        records

[<Property>]
let ``option array field with option enum union elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<EnumUnion>>> |}>
        records

[<Property>]
let ``option array field with option complex union elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<ComplexUnion>>> |}>
        records

[<Property>]
let ``option array field with option nullable elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<Nullable<int>>>> |}>
        records

[<Property>]
let ``option array field with option option elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<option<int>>>> |}>
        records

[<Property>]
let ``option generic list field with bool elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<bool>> |}>
        records

[<Property>]
let ``option generic list field with int8 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<int8>> |}>
        records

[<Property>]
let ``option generic list field with int16 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<int16>> |}>
        records

[<Property>]
let ``option generic list field with int32 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<int>> |}>
        records

[<Property>]
let ``option generic list field with int64 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<int64>> |}>
        records

[<Property>]
let ``option generic list field with uint8 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<uint8>> |}>
        records

[<Property>]
let ``option generic list field with uint16 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<uint16>> |}>
        records

[<Property>]
let ``option generic list field with uint32 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<uint>> |}>
        records

[<Property>]
let ``option generic list field with uint64 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<uint64>> |}>
        records

[<Property>]
let ``option generic list field with float32 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<float32>> |}>
        records

[<Property>]
let ``option generic list field with float64 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<float>> |}>
        records

[<Property>]
let ``option generic list field with decimal elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option generic list field with date time elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<DateTime>> |}>
        records

[<Property>]
let ``option generic list field with date time offset elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<DateTimeOffset>> |}>
        records

[<Property>]
let ``option generic list field with string elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<string>> |}>
        records

[<Property>]
let ``option generic list field with byte array elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<byte[]>> |}>
        records

[<Property>]
let ``option generic list field with guid elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Guid>> |}>
        records

[<Property>]
let ``option generic list field with array elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<array<int>>> |}>
        records

[<Property>]
let ``option generic list field with generic list elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<ResizeArray<int>>> |}>
        records

[<Property>]
let ``option generic list field with fsharp list elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<list<int>>> |}>
        records

[<Property>]
let ``option generic list field with record elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<{|
            Field2: int |}>> |}>
        records

[<Property>]
let ``option generic list field with enum union elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<EnumUnion>> |}>
        records

[<Property>]
let ``option generic list field with complex union elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<ComplexUnion>> |}>
        records

[<Property>]
let ``option generic list field with nullable bool elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<bool>>> |}>
        records

[<Property>]
let ``option generic list field with nullable int8 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<int8>>> |}>
        records

[<Property>]
let ``option generic list field with nullable int16 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<int16>>> |}>
        records

[<Property>]
let ``option generic list field with nullable int32 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<int>>> |}>
        records

[<Property>]
let ``option generic list field with nullable int64 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<int64>>> |}>
        records

[<Property>]
let ``option generic list field with nullable uint8 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<uint8>>> |}>
        records

[<Property>]
let ``option generic list field with nullable uint16 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<uint16>>> |}>
        records

[<Property>]
let ``option generic list field with nullable uint32 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<uint>>> |}>
        records

[<Property>]
let ``option generic list field with nullable uint64 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<uint64>>> |}>
        records

[<Property>]
let ``option generic list field with nullable float32 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<float32>>> |}>
        records

[<Property>]
let ``option generic list field with nullable float64 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<float>>> |}>
        records

[<Property>]
let ``option generic list field with nullable decimal elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<decimal>>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option generic list field with nullable date time elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<DateTime>>> |}>
        records

[<Property>]
let ``option generic list field with nullable date time offset elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<DateTimeOffset>>> |}>
        records

[<Property>]
let ``option generic list field with nullable guid elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<Guid>>> |}>
        records

[<Property>]
let ``option generic list field with nullable record elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<struct {|
            Field2: int |}>>> |}>
        records

[<Property>]
let ``option generic list field with option bool elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<bool>>> |}>
        records

[<Property>]
let ``option generic list field with option int8 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<int8>>> |}>
        records

[<Property>]
let ``option generic list field with option int16 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<int16>>> |}>
        records

[<Property>]
let ``option generic list field with option int32 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<int>>> |}>
        records

[<Property>]
let ``option generic list field with option int64 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<int64>>> |}>
        records

[<Property>]
let ``option generic list field with option uint8 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<uint8>>> |}>
        records

[<Property>]
let ``option generic list field with option uint16 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<uint16>>> |}>
        records

[<Property>]
let ``option generic list field with option uint32 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<uint>>> |}>
        records

[<Property>]
let ``option generic list field with option uint64 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<uint64>>> |}>
        records

[<Property>]
let ``option generic list field with option float32 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<float32>>> |}>
        records

[<Property>]
let ``option generic list field with option float64 elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<float>>> |}>
        records

[<Property>]
let ``option generic list field with option decimal elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<decimal>>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option generic list field with option date time elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<DateTime>>> |}>
        records

[<Property>]
let ``option generic list field with option date time offset elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<DateTimeOffset>>> |}>
        records

[<Property>]
let ``option generic list field with option string elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<string>>> |}>
        records

[<Property>]
let ``option generic list field with option byte array elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<byte[]>>> |}>
        records

[<Property>]
let ``option generic list field with option guid elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<Guid>>> |}>
        records

[<Property>]
let ``option generic list field with option array elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<array<int>>>> |}>
        records

[<Property>]
let ``option generic list field with option generic list elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<ResizeArray<int>>>> |}>
        records

[<Property>]
let ``option generic list field with option fsharp list elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<list<int>>>> |}>
        records

[<Property>]
let ``option generic list field with option record elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<{|
            Field2: int |}>>> |}>
        records

[<Property>]
let ``option generic list field with option enum union elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<EnumUnion>>> |}>
        records

[<Property>]
let ``option generic list field with option complex union elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<ComplexUnion>>> |}>
        records

[<Property>]
let ``option generic list field with option nullable elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<Nullable<int>>>> |}>
        records

[<Property>]
let ``option generic list field with option option elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<option<int>>>> |}>
        records

[<Property>]
let ``option fsharp list field with bool elements`` records =
    testRoundtrip<{|
        Field1: option<list<bool>> |}>
        records

[<Property>]
let ``option fsharp list field with int8 elements`` records =
    testRoundtrip<{|
        Field1: option<list<int8>> |}>
        records

[<Property>]
let ``option fsharp list field with int16 elements`` records =
    testRoundtrip<{|
        Field1: option<list<int16>> |}>
        records

[<Property>]
let ``option fsharp list field with int32 elements`` records =
    testRoundtrip<{|
        Field1: option<list<int>> |}>
        records

[<Property>]
let ``option fsharp list field with int64 elements`` records =
    testRoundtrip<{|
        Field1: option<list<int64>> |}>
        records

[<Property>]
let ``option fsharp list field with uint8 elements`` records =
    testRoundtrip<{|
        Field1: option<list<uint8>> |}>
        records

[<Property>]
let ``option fsharp list field with uint16 elements`` records =
    testRoundtrip<{|
        Field1: option<list<uint16>> |}>
        records

[<Property>]
let ``option fsharp list field with uint32 elements`` records =
    testRoundtrip<{|
        Field1: option<list<uint>> |}>
        records

[<Property>]
let ``option fsharp list field with uint64 elements`` records =
    testRoundtrip<{|
        Field1: option<list<uint64>> |}>
        records

[<Property>]
let ``option fsharp list field with float32 elements`` records =
    testRoundtrip<{|
        Field1: option<list<float32>> |}>
        records

[<Property>]
let ``option fsharp list field with float64 elements`` records =
    testRoundtrip<{|
        Field1: option<list<float>> |}>
        records

[<Property>]
let ``option fsharp list field with decimal elements`` records =
    testRoundtrip<{|
        Field1: option<list<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option fsharp list field with date time elements`` records =
    testRoundtrip<{|
        Field1: option<list<DateTime>> |}>
        records

[<Property>]
let ``option fsharp list field with date time offset elements`` records =
    testRoundtrip<{|
        Field1: option<list<DateTimeOffset>> |}>
        records

[<Property>]
let ``option fsharp list field with string elements`` records =
    testRoundtrip<{|
        Field1: option<list<string>> |}>
        records

[<Property>]
let ``option fsharp list field with byte array elements`` records =
    testRoundtrip<{|
        Field1: option<list<byte[]>> |}>
        records

[<Property>]
let ``option fsharp list field with guid elements`` records =
    testRoundtrip<{|
        Field1: option<list<Guid>> |}>
        records

[<Property>]
let ``option fsharp list field with array elements`` records =
    testRoundtrip<{|
        Field1: option<list<array<int>>> |}>
        records

[<Property>]
let ``option fsharp list field with generic list elements`` records =
    testRoundtrip<{|
        Field1: option<list<ResizeArray<int>>> |}>
        records

[<Property>]
let ``option fsharp list field with fsharp list elements`` records =
    testRoundtrip<{|
        Field1: option<list<list<int>>> |}>
        records

[<Property>]
let ``option fsharp list field with record elements`` records =
    testRoundtrip<{|
        Field1: option<list<{|
            Field2: int |}>> |}>
        records

[<Property>]
let ``option fsharp list field with enum union elements`` records =
    testRoundtrip<{|
        Field1: option<list<EnumUnion>> |}>
        records

[<Property>]
let ``option fsharp list field with complex union elements`` records =
    testRoundtrip<{|
        Field1: option<list<ComplexUnion>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable bool elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<bool>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable int8 elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<int8>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable int16 elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<int16>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable int32 elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<int>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable int64 elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<int64>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable uint8 elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<uint8>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable uint16 elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<uint16>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable uint32 elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<uint>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable uint64 elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<uint64>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable float32 elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<float32>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable float64 elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<float>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable decimal elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<decimal>>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option fsharp list field with nullable date time elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<DateTime>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable date time offset elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<DateTimeOffset>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable guid elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<Guid>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable record elements`` records =
    testRoundtrip<{|
        Field1: option<list<Nullable<struct {|
            Field2: int |}>>> |}>
        records

[<Property>]
let ``option fsharp list field with option bool elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<bool>>> |}>
        records

[<Property>]
let ``option fsharp list field with option int8 elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<int8>>> |}>
        records

[<Property>]
let ``option fsharp list field with option int16 elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<int16>>> |}>
        records

[<Property>]
let ``option fsharp list field with option int32 elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<int>>> |}>
        records

[<Property>]
let ``option fsharp list field with option int64 elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<int64>>> |}>
        records

[<Property>]
let ``option fsharp list field with option uint8 elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<uint8>>> |}>
        records

[<Property>]
let ``option fsharp list field with option uint16 elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<uint16>>> |}>
        records

[<Property>]
let ``option fsharp list field with option uint32 elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<uint>>> |}>
        records

[<Property>]
let ``option fsharp list field with option uint64 elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<uint64>>> |}>
        records

[<Property>]
let ``option fsharp list field with option float32 elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<float32>>> |}>
        records

[<Property>]
let ``option fsharp list field with option float64 elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<float>>> |}>
        records

[<Property>]
let ``option fsharp list field with option decimal elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<decimal>>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option fsharp list field with option date time elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<DateTime>>> |}>
        records

[<Property>]
let ``option fsharp list field with option date time offset elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<DateTimeOffset>>> |}>
        records

[<Property>]
let ``option fsharp list field with option string elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<string>>> |}>
        records

[<Property>]
let ``option fsharp list field with option byte array elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<byte[]>>> |}>
        records

[<Property>]
let ``option fsharp list field with option guid elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<Guid>>> |}>
        records

[<Property>]
let ``option fsharp list field with option array elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<array<int>>>> |}>
        records

[<Property>]
let ``option fsharp list field with option generic list elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<ResizeArray<int>>>> |}>
        records

[<Property>]
let ``option fsharp list field with option fsharp list elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<list<int>>>> |}>
        records

[<Property>]
let ``option fsharp list field with option record elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<{|
            Field2: int |}>>> |}>
        records

[<Property>]
let ``option fsharp list field with option enum union elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<EnumUnion>>> |}>
        records

[<Property>]
let ``option fsharp list field with option complex union elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<ComplexUnion>>> |}>
        records

[<Property>]
let ``option fsharp list field with option nullable elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<Nullable<int>>>> |}>
        records

[<Property>]
let ``option fsharp list field with option option elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<option<int>>>> |}>
        records

[<Property>]
let ``option record field with bool field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: bool |}> |}>
        records

[<Property>]
let ``option record field with int8 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: int8 |}> |}>
        records

[<Property>]
let ``option record field with int16 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: int16 |}> |}>
        records

[<Property>]
let ``option record field with int32 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: int |}> |}>
        records

[<Property>]
let ``option record field with int64 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: int64 |}> |}>
        records

[<Property>]
let ``option record field with uint8 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: uint8 |}> |}>
        records

[<Property>]
let ``option record field with uint16 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: uint16 |}> |}>
        records

[<Property>]
let ``option record field with uint32 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: uint |}> |}>
        records

[<Property>]
let ``option record field with uint64 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: uint64 |}> |}>
        records

[<Property>]
let ``option record field with float32 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: float32 |}> |}>
        records

[<Property>]
let ``option record field with float64 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: float |}> |}>
        records

[<Property>]
let ``option record field with decimal field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: decimal |}> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option record field with date time field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: DateTime |}> |}>
        records

[<Property>]
let ``option record field with date time offset field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: DateTimeOffset |}> |}>
        records

[<Property>]
let ``option record field with string field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: string |}> |}>
        records

[<Property>]
let ``option record field with byte array field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: byte[] |}> |}>
        records

[<Property>]
let ``option record field with guid field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Guid |}> |}>
        records

[<Property>]
let ``option record field with array field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: array<int> |}> |}>
        records

[<Property>]
let ``option record field with generic list field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: ResizeArray<int> |}> |}>
        records

[<Property>]
let ``option record field with fsharp list field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: list<int> |}> |}>
        records

[<Property>]
let ``option record field with record field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: {|
                Field3: int |} |}> |}>
        records

[<Property>]
let ``option record field with enum union field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: EnumUnion |}> |}>
        records

[<Property>]
let ``option record field with complex union field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: ComplexUnion |}> |}>
        records

[<Property>]
let ``option record field with nullable bool field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<bool> |}> |}>
        records

[<Property>]
let ``option record field with nullable int8 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<int8> |}> |}>
        records

[<Property>]
let ``option record field with nullable int16 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<int16> |}> |}>
        records

[<Property>]
let ``option record field with nullable int32 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<int> |}> |}>
        records

[<Property>]
let ``option record field with nullable int64 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<int64> |}> |}>
        records

[<Property>]
let ``option record field with nullable uint8 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<uint8> |}> |}>
        records

[<Property>]
let ``option record field with nullable uint16 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<uint16> |}> |}>
        records

[<Property>]
let ``option record field with nullable uint32 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<uint> |}> |}>
        records

[<Property>]
let ``option record field with nullable uint64 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<uint64> |}> |}>
        records

[<Property>]
let ``option record field with nullable float32 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<float32> |}> |}>
        records

[<Property>]
let ``option record field with nullable float64 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<float> |}> |}>
        records

[<Property>]
let ``option record field with nullable decimal field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<decimal> |}> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option record field with nullable date time field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<DateTime> |}> |}>
        records

[<Property>]
let ``option record field with nullable date time offset field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<DateTimeOffset> |}> |}>
        records

[<Property>]
let ``option record field with nullable guid field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<Guid> |}> |}>
        records

[<Property>]
let ``option record field with nullable record field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<struct {|
                Field3: int |}> |}> |}>
        records

[<Property>]
let ``option record field with option bool field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<bool> |}> |}>
        records

[<Property>]
let ``option record field with option int8 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<int8> |}> |}>
        records

[<Property>]
let ``option record field with option int16 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<int16> |}> |}>
        records

[<Property>]
let ``option record field with option int32 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<int> |}> |}>
        records

[<Property>]
let ``option record field with option int64 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<int64> |}> |}>
        records

[<Property>]
let ``option record field with option uint8 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<uint8> |}> |}>
        records

[<Property>]
let ``option record field with option uint16 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<uint16> |}> |}>
        records

[<Property>]
let ``option record field with option uint32 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<uint> |}> |}>
        records

[<Property>]
let ``option record field with option uint64 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<uint64> |}> |}>
        records

[<Property>]
let ``option record field with option float32 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<float32> |}> |}>
        records

[<Property>]
let ``option record field with option float64 field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<float> |}> |}>
        records

[<Property>]
let ``option record field with option decimal field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<decimal> |}> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option record field with option date time field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<DateTime> |}> |}>
        records

[<Property>]
let ``option record field with option date time offset field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<DateTimeOffset> |}> |}>
        records

[<Property>]
let ``option record field with option string field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<string> |}> |}>
        records

[<Property>]
let ``option record field with option byte array field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<byte[]> |}> |}>
        records

[<Property>]
let ``option record field with option guid field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<Guid> |}> |}>
        records

[<Property>]
let ``option record field with option array field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<array<int>> |}> |}>
        records

[<Property>]
let ``option record field with option generic list field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<ResizeArray<int>> |}> |}>
        records

[<Property>]
let ``option record field with option fsharp list field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<list<int>> |}> |}>
        records

[<Property>]
let ``option record field with option record field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<{|
                Field3: int |}> |}> |}>
        records

[<Property>]
let ``option record field with option enum union field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<EnumUnion> |}> |}>
        records

[<Property>]
let ``option record field with option complex union field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<ComplexUnion> |}> |}>
        records

[<Property>]
let ``option record field with option nullable field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<Nullable<int>> |}> |}>
        records

[<Property>]
let ``option record field with option option field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<option<int>> |}> |}>
        records

[<Property>]
let ``option enum union field`` records =
    testRoundtrip<{|
        Field1: option<EnumUnion> |}>
        records

[<Property>]
let ``option complex union field`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnion> |}>
        records

[<Property>]
let ``option complex union field with bool case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithBoolCase> |}>
        records

[<Property>]
let ``option complex union field with int8 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithInt8Case> |}>
        records

[<Property>]
let ``option complex union field with int16 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithInt16Case> |}>
        records

[<Property>]
let ``option complex union field with int32 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithInt32Case> |}>
        records

[<Property>]
let ``option complex union field with int64 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithInt64Case> |}>
        records

[<Property>]
let ``option complex union field with uint8 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithUInt8Case> |}>
        records

[<Property>]
let ``option complex union field with uint16 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithUInt16Case> |}>
        records

[<Property>]
let ``option complex union field with uint32 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithUInt32Case> |}>
        records

[<Property>]
let ``option complex union field with uint64 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithUInt64Case> |}>
        records

[<Property>]
let ``option complex union field with float32 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithFloat32Case> |}>
        records

[<Property>]
let ``option complex union field with float64 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithFloat64Case> |}>
        records

[<Property>]
let ``option complex union field with decimal case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithDecimalCase> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option complex union field with date time case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithDateTimeCase> |}>
        records

[<Property>]
let ``option complex union field with date time offset case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithDateTimeOffsetCase> |}>
        records

[<Property>]
let ``option complex union field with string case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithStringCase> |}>
        records

[<Property>]
let ``option complex union field with byte array case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithByteArrayCase> |}>
        records

[<Property>]
let ``option complex union field with guid case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithGuidCase> |}>
        records

[<Property>]
let ``option complex union field with array case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithArrayCase> |}>
        records

[<Property>]
let ``option complex union field with generic list case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithGenericListCase> |}>
        records

[<Property>]
let ``option complex union field with fsharp list case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithFSharpListCase> |}>
        records

[<Property>]
let ``option complex union field with record case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithRecordCase> |}>
        records

[<Property>]
let ``option complex union field with enum union case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithEnumUnionCase> |}>
        records

[<Property>]
let ``option complex union field with complex union case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithComplexUnionCase> |}>
        records

[<Property>]
let ``option complex union field with nullable bool case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableBoolCase> |}>
        records

[<Property>]
let ``option complex union field with nullable int8 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableInt8Case> |}>
        records

[<Property>]
let ``option complex union field with nullable int16 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableInt16Case> |}>
        records

[<Property>]
let ``option complex union field with nullable int32 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableInt32Case> |}>
        records

[<Property>]
let ``option complex union field with nullable int64 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableInt64Case> |}>
        records

[<Property>]
let ``option complex union field with nullable uint8 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableUInt8Case> |}>
        records

[<Property>]
let ``option complex union field with nullable uint16 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableUInt16Case> |}>
        records

[<Property>]
let ``option complex union field with nullable uint32 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableUInt32Case> |}>
        records

[<Property>]
let ``option complex union field with nullable uint64 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableUInt64Case> |}>
        records

[<Property>]
let ``option complex union field with nullable float32 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableFloat32Case> |}>
        records

[<Property>]
let ``option complex union field with nullable float64 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableFloat64Case> |}>
        records

[<Property>]
let ``option complex union field with nullable decimal case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableDecimalCase> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option complex union field with nullable date time case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableDateTimeCase> |}>
        records

[<Property>]
let ``option complex union field with nullable date time offset case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableDateTimeOffsetCase> |}>
        records

[<Property>]
let ``option complex union field with nullable guid case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableGuidCase> |}>
        records

[<Property>]
let ``option complex union field with nullable record case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableRecordCase> |}>
        records

[<Property>]
let ``option complex union field with option bool case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionBoolCase> |}>
        records

[<Property>]
let ``option complex union field with option int8 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionInt8Case> |}>
        records

[<Property>]
let ``option complex union field with option int16 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionInt16Case> |}>
        records

[<Property>]
let ``option complex union field with option int32 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionInt32Case> |}>
        records

[<Property>]
let ``option complex union field with option int64 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionInt64Case> |}>
        records

[<Property>]
let ``option complex union field with option uint8 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionUInt8Case> |}>
        records

[<Property>]
let ``option complex union field with option uint16 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionUInt16Case> |}>
        records

[<Property>]
let ``option complex union field with option uint32 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionUInt32Case> |}>
        records

[<Property>]
let ``option complex union field with option uint64 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionUInt64Case> |}>
        records

[<Property>]
let ``option complex union field with option float32 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionFloat32Case> |}>
        records

[<Property>]
let ``option complex union field with option float64 case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionFloat64Case> |}>
        records

[<Property>]
let ``option complex union field with option decimal case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionDecimalCase> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option complex union field with option date time case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionDateTimeCase> |}>
        records

[<Property>]
let ``option complex union field with option date time offset case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionDateTimeOffsetCase> |}>
        records

[<Property>]
let ``option complex union field with option string case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionStringCase> |}>
        records

[<Property>]
let ``option complex union field with option byte array case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionByteArrayCase> |}>
        records

[<Property>]
let ``option complex union field with option guid case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionGuidCase> |}>
        records

[<Property>]
let ``option complex union field with option array case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionArrayCase> |}>
        records

[<Property>]
let ``option complex union field with option generic list case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionGenericListCase> |}>
        records

[<Property>]
let ``option complex union field with option fsharp list case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionFSharpListCase> |}>
        records

[<Property>]
let ``option complex union field with option record case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionRecordCase> |}>
        records

[<Property>]
let ``option complex union field with option enum union case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionEnumUnionCase> |}>
        records

[<Property>]
let ``option complex union field with option complex union case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionComplexUnionCase> |}>
        records

[<Property>]
let ``option complex union field with option nullable case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionNullableCase> |}>
        records

[<Property>]
let ``option complex union field with option option case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionOptionCase> |}>
        records

[<Property>]
let ``option nullable field`` records =
    testRoundtrip<{|
        Field1: option<Nullable<int>> |}>
        records

[<Property>]
let ``option option field`` records =
    testRoundtrip<{|
        Field1: option<option<int>> |}>
        records
