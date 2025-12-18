module rec Parquet.FSharp.Tests.Roundtrip

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
type ComplexUnionWithClassCase = Case1 of field1:Class
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
type ComplexUnionWithOptionClassCase = Case1 of field1:option<Class>
type ComplexUnionWithOptionNullableCase = Case1 of field1:option<Nullable<int>>
type ComplexUnionWithOptionOptionCase = Case1 of field1:option<option<int>>

type Class() =
    member val Field1 = Unchecked.defaultof<bool> with get, set
    member val Field2 = Unchecked.defaultof<int> with get, set
    member val Field3 = Unchecked.defaultof<float> with get, set

type ClassWithBoolField() =
    member val Field1 = Unchecked.defaultof<bool> with get, set

type ClassWithInt8Field() =
    member val Field1 = Unchecked.defaultof<int8> with get, set

type ClassWithInt16Field() =
    member val Field1 = Unchecked.defaultof<int16> with get, set

type ClassWithInt32Field() =
    member val Field1 = Unchecked.defaultof<int> with get, set

type ClassWithInt64Field() =
    member val Field1 = Unchecked.defaultof<int64> with get, set

type ClassWithUInt8Field() =
    member val Field1 = Unchecked.defaultof<uint8> with get, set

type ClassWithUInt16Field() =
    member val Field1 = Unchecked.defaultof<uint16> with get, set

type ClassWithUInt32Field() =
    member val Field1 = Unchecked.defaultof<uint> with get, set

type ClassWithUInt64Field() =
    member val Field1 = Unchecked.defaultof<uint64> with get, set

type ClassWithFloat32Field() =
    member val Field1 = Unchecked.defaultof<float32> with get, set

type ClassWithFloat64Field() =
    member val Field1 = Unchecked.defaultof<float> with get, set

type ClassWithDecimalField() =
    member val Field1 = Unchecked.defaultof<decimal> with get, set

type ClassWithDateTimeField() =
    member val Field1 = Unchecked.defaultof<DateTime> with get, set

type ClassWithDateTimeOffsetField() =
    member val Field1 = Unchecked.defaultof<DateTimeOffset> with get, set

type ClassWithStringField() =
    member val Field1 = Unchecked.defaultof<string> with get, set

type ClassWithByteArrayField() =
    member val Field1 = Unchecked.defaultof<byte[]> with get, set

type ClassWithGuidField() =
    member val Field1 = Unchecked.defaultof<Guid> with get, set

type ClassWithArrayField() =
    member val Field1 = Unchecked.defaultof<array<int>> with get, set

type ClassWithGenericListField() =
    member val Field1 = Unchecked.defaultof<ResizeArray<int>> with get, set

type ClassWithFSharpListField() =
    member val Field1 = Unchecked.defaultof<list<int>> with get, set

type ClassWithRecordField() =
    member val Field1 = Unchecked.defaultof<{| Field1: int |}> with get, set

type ClassWithEnumUnionField() =
    member val Field1 = Unchecked.defaultof<EnumUnion> with get, set

type ClassWithComplexUnionField() =
    member val Field1 = Unchecked.defaultof<ComplexUnion> with get, set

type ClassWithClassField() =
    member val Field1 = Unchecked.defaultof<Class> with get, set

type ClassWithNullableBoolField() =
    member val Field1 = Unchecked.defaultof<Nullable<bool>> with get, set

type ClassWithNullableInt8Field() =
    member val Field1 = Unchecked.defaultof<Nullable<int8>> with get, set

type ClassWithNullableInt16Field() =
    member val Field1 = Unchecked.defaultof<Nullable<int16>> with get, set

type ClassWithNullableInt32Field() =
    member val Field1 = Unchecked.defaultof<Nullable<int>> with get, set

type ClassWithNullableInt64Field() =
    member val Field1 = Unchecked.defaultof<Nullable<int64>> with get, set

type ClassWithNullableUInt8Field() =
    member val Field1 = Unchecked.defaultof<Nullable<uint8>> with get, set

type ClassWithNullableUInt16Field() =
    member val Field1 = Unchecked.defaultof<Nullable<uint16>> with get, set

type ClassWithNullableUInt32Field() =
    member val Field1 = Unchecked.defaultof<Nullable<uint>> with get, set

type ClassWithNullableUInt64Field() =
    member val Field1 = Unchecked.defaultof<Nullable<uint64>> with get, set

type ClassWithNullableFloat32Field() =
    member val Field1 = Unchecked.defaultof<Nullable<float32>> with get, set

type ClassWithNullableFloat64Field() =
    member val Field1 = Unchecked.defaultof<Nullable<float>> with get, set

type ClassWithNullableDecimalField() =
    member val Field1 = Unchecked.defaultof<Nullable<decimal>> with get, set

type ClassWithNullableDateTimeField() =
    member val Field1 = Unchecked.defaultof<Nullable<DateTime>> with get, set

type ClassWithNullableDateTimeOffsetField() =
    member val Field1 = Unchecked.defaultof<Nullable<DateTimeOffset>> with get, set

type ClassWithNullableGuidField() =
    member val Field1 = Unchecked.defaultof<Nullable<Guid>> with get, set

type ClassWithNullableRecordField() =
    member val Field1 = Unchecked.defaultof<Nullable<struct {| Field1: int |}>> with get, set

type ClassWithOptionBoolField() =
    member val Field1 = Unchecked.defaultof<option<bool>> with get, set

type ClassWithOptionInt8Field() =
    member val Field1 = Unchecked.defaultof<option<int8>> with get, set

type ClassWithOptionInt16Field() =
    member val Field1 = Unchecked.defaultof<option<int16>> with get, set

type ClassWithOptionInt32Field() =
    member val Field1 = Unchecked.defaultof<option<int>> with get, set

type ClassWithOptionInt64Field() =
    member val Field1 = Unchecked.defaultof<option<int64>> with get, set

type ClassWithOptionUInt8Field() =
    member val Field1 = Unchecked.defaultof<option<uint8>> with get, set

type ClassWithOptionUInt16Field() =
    member val Field1 = Unchecked.defaultof<option<uint16>> with get, set

type ClassWithOptionUInt32Field() =
    member val Field1 = Unchecked.defaultof<option<uint>> with get, set

type ClassWithOptionUInt64Field() =
    member val Field1 = Unchecked.defaultof<option<uint64>> with get, set

type ClassWithOptionFloat32Field() =
    member val Field1 = Unchecked.defaultof<option<float32>> with get, set

type ClassWithOptionFloat64Field() =
    member val Field1 = Unchecked.defaultof<option<float>> with get, set

type ClassWithOptionDecimalField() =
    member val Field1 = Unchecked.defaultof<option<decimal>> with get, set

type ClassWithOptionDateTimeField() =
    member val Field1 = Unchecked.defaultof<option<DateTime>> with get, set

type ClassWithOptionDateTimeOffsetField() =
    member val Field1 = Unchecked.defaultof<option<DateTimeOffset>> with get, set

type ClassWithOptionStringField() =
    member val Field1 = Unchecked.defaultof<option<string>> with get, set

type ClassWithOptionByteArrayField() =
    member val Field1 = Unchecked.defaultof<option<byte[]>> with get, set

type ClassWithOptionGuidField() =
    member val Field1 = Unchecked.defaultof<option<Guid>> with get, set

type ClassWithOptionArrayField() =
    member val Field1 = Unchecked.defaultof<option<array<int>>> with get, set

type ClassWithOptionGenericListField() =
    member val Field1 = Unchecked.defaultof<option<ResizeArray<int>>> with get, set

type ClassWithOptionFSharpListField() =
    member val Field1 = Unchecked.defaultof<option<list<int>>> with get, set

type ClassWithOptionRecordField() =
    member val Field1 = Unchecked.defaultof<option<{| Field1: int |}>> with get, set

type ClassWithOptionEnumUnionField() =
    member val Field1 = Unchecked.defaultof<option<EnumUnion>> with get, set

type ClassWithOptionComplexUnionField() =
    member val Field1 = Unchecked.defaultof<option<ComplexUnion>> with get, set

type ClassWithOptionClassField() =
    member val Field1 = Unchecked.defaultof<option<Class>> with get, set

type ClassWithOptionNullableField() =
    member val Field1 = Unchecked.defaultof<option<Nullable<int>>> with get, set

type ClassWithOptionOptionField() =
    member val Field1 = Unchecked.defaultof<option<option<int>>> with get, set

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
let ``array field with class elements`` records =
    testRoundtrip<{|
        Field1: array<Class> |}>
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
let ``array field with option class elements`` records =
    testRoundtrip<{|
        Field1: array<option<Class>> |}>
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
let ``generic list field with class elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<Class> |}>
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
let ``generic list field with option class elements`` records =
    testRoundtrip<{|
        Field1: ResizeArray<option<Class>> |}>
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
let ``fsharp list field with class elements`` records =
    testRoundtrip<{|
        Field1: list<Class> |}>
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
let ``fsharp list field with option class elements`` records =
    testRoundtrip<{|
        Field1: list<option<Class>> |}>
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
let ``record field with class field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: Class |} |}>
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
let ``record field with option class field`` records =
    testRoundtrip<{|
        Field1: {|
            Field2: option<Class> |} |}>
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
let ``complex union field with class case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithClassCase |}>
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
let ``complex union field with option class case`` records =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionClassCase |}>
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
let ``class field`` records =
    testRoundtrip<{|
        Field1: Class |}>
        records

[<Property>]
let ``class field with bool field`` records =
    testRoundtrip<{|
        Field1: ClassWithBoolField |}>
        records

[<Property>]
let ``class field with int8 field`` records =
    testRoundtrip<{|
        Field1: ClassWithInt8Field |}>
        records

[<Property>]
let ``class field with int16 field`` records =
    testRoundtrip<{|
        Field1: ClassWithInt16Field |}>
        records

[<Property>]
let ``class field with int32 field`` records =
    testRoundtrip<{|
        Field1: ClassWithInt32Field |}>
        records

[<Property>]
let ``class field with int64 field`` records =
    testRoundtrip<{|
        Field1: ClassWithInt64Field |}>
        records

[<Property>]
let ``class field with uint8 field`` records =
    testRoundtrip<{|
        Field1: ClassWithUInt8Field |}>
        records

[<Property>]
let ``class field with uint16 field`` records =
    testRoundtrip<{|
        Field1: ClassWithUInt16Field |}>
        records

[<Property>]
let ``class field with uint32 field`` records =
    testRoundtrip<{|
        Field1: ClassWithUInt32Field |}>
        records

[<Property>]
let ``class field with uint64 field`` records =
    testRoundtrip<{|
        Field1: ClassWithUInt64Field |}>
        records

[<Property>]
let ``class field with float32 field`` records =
    testRoundtrip<{|
        Field1: ClassWithFloat32Field |}>
        records

[<Property>]
let ``class field with float64 field`` records =
    testRoundtrip<{|
        Field1: ClassWithFloat64Field |}>
        records

[<Property>]
let ``class field with decimal field`` records =
    testRoundtrip<{|
        Field1: ClassWithDecimalField |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``class field with date time field`` records =
    testRoundtrip<{|
        Field1: ClassWithDateTimeField |}>
        records

[<Property>]
let ``class field with date time offset field`` records =
    testRoundtrip<{|
        Field1: ClassWithDateTimeOffsetField |}>
        records

[<Property>]
let ``class field with string field`` records =
    testRoundtrip<{|
        Field1: ClassWithStringField |}>
        records

[<Property>]
let ``class field with byte array field`` records =
    testRoundtrip<{|
        Field1: ClassWithByteArrayField |}>
        records

[<Property>]
let ``class field with guid field`` records =
    testRoundtrip<{|
        Field1: ClassWithGuidField |}>
        records

[<Property>]
let ``class field with array field`` records =
    testRoundtrip<{|
        Field1: ClassWithArrayField |}>
        records

[<Property>]
let ``class field with generic list field`` records =
    testRoundtrip<{|
        Field1: ClassWithGenericListField |}>
        records

[<Property>]
let ``class field with fsharp list field`` records =
    testRoundtrip<{|
        Field1: ClassWithFSharpListField |}>
        records

[<Property>]
let ``class field with record field`` records =
    testRoundtrip<{|
        Field1: ClassWithRecordField |}>
        records

[<Property>]
let ``class field with enum union field`` records =
    testRoundtrip<{|
        Field1: ClassWithEnumUnionField |}>
        records

[<Property>]
let ``class field with complex union field`` records =
    testRoundtrip<{|
        Field1: ClassWithComplexUnionField |}>
        records

[<Property>]
let ``class field with class field`` records =
    testRoundtrip<{|
        Field1: ClassWithClassField |}>
        records

[<Property>]
let ``class field with nullable bool field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableBoolField |}>
        records

[<Property>]
let ``class field with nullable int8 field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableInt8Field |}>
        records

[<Property>]
let ``class field with nullable int16 field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableInt16Field |}>
        records

[<Property>]
let ``class field with nullable int32 field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableInt32Field |}>
        records

[<Property>]
let ``class field with nullable int64 field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableInt64Field |}>
        records

[<Property>]
let ``class field with nullable uint8 field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableUInt8Field |}>
        records

[<Property>]
let ``class field with nullable uint16 field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableUInt16Field |}>
        records

[<Property>]
let ``class field with nullable uint32 field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableUInt32Field |}>
        records

[<Property>]
let ``class field with nullable uint64 field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableUInt64Field |}>
        records

[<Property>]
let ``class field with nullable float32 field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableFloat32Field |}>
        records

[<Property>]
let ``class field with nullable float64 field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableFloat64Field |}>
        records

[<Property>]
let ``class field with nullable decimal field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableDecimalField |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``class field with nullable date time field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableDateTimeField |}>
        records

[<Property>]
let ``class field with nullable date time offset field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableDateTimeOffsetField |}>
        records

[<Property>]
let ``class field with nullable guid field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableGuidField |}>
        records

[<Property>]
let ``class field with nullable record field`` records =
    testRoundtrip<{|
        Field1: ClassWithNullableRecordField |}>
        records

[<Property>]
let ``class field with option bool field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionBoolField |}>
        records

[<Property>]
let ``class field with option int8 field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionInt8Field |}>
        records

[<Property>]
let ``class field with option int16 field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionInt16Field |}>
        records

[<Property>]
let ``class field with option int32 field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionInt32Field |}>
        records

[<Property>]
let ``class field with option int64 field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionInt64Field |}>
        records

[<Property>]
let ``class field with option uint8 field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionUInt8Field |}>
        records

[<Property>]
let ``class field with option uint16 field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionUInt16Field |}>
        records

[<Property>]
let ``class field with option uint32 field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionUInt32Field |}>
        records

[<Property>]
let ``class field with option uint64 field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionUInt64Field |}>
        records

[<Property>]
let ``class field with option float32 field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionFloat32Field |}>
        records

[<Property>]
let ``class field with option float64 field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionFloat64Field |}>
        records

[<Property>]
let ``class field with option decimal field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionDecimalField |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``class field with option date time field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionDateTimeField |}>
        records

[<Property>]
let ``class field with option date time offset field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionDateTimeOffsetField |}>
        records

[<Property>]
let ``class field with option string field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionStringField |}>
        records

[<Property>]
let ``class field with option byte array field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionByteArrayField |}>
        records

[<Property>]
let ``class field with option guid field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionGuidField |}>
        records

[<Property>]
let ``class field with option array field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionArrayField |}>
        records

[<Property>]
let ``class field with option generic list field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionGenericListField |}>
        records

[<Property>]
let ``class field with option fsharp list field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionFSharpListField |}>
        records

[<Property>]
let ``class field with option record field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionRecordField |}>
        records

[<Property>]
let ``class field with option enum union field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionEnumUnionField |}>
        records

[<Property>]
let ``class field with option complex union field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionComplexUnionField |}>
        records

[<Property>]
let ``class field with option class field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionClassField |}>
        records

[<Property>]
let ``class field with option nullable field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionNullableField |}>
        records

[<Property>]
let ``class field with option option field`` records =
    testRoundtrip<{|
        Field1: ClassWithOptionOptionField |}>
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
let ``option array field with class elements`` records =
    testRoundtrip<{|
        Field1: option<array<Class>> |}>
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
let ``option array field with option class elements`` records =
    testRoundtrip<{|
        Field1: option<array<option<Class>>> |}>
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
let ``option generic list field with class elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<Class>> |}>
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
let ``option generic list field with option class elements`` records =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<Class>>> |}>
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
let ``option fsharp list field with class elements`` records =
    testRoundtrip<{|
        Field1: option<list<Class>> |}>
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
let ``option fsharp list field with option class elements`` records =
    testRoundtrip<{|
        Field1: option<list<option<Class>>> |}>
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
let ``option record field with class field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Class |}> |}>
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
let ``option record field with option class field`` records =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<Class> |}> |}>
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
let ``option complex union field with class case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithClassCase> |}>
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
let ``option complex union field with option class case`` records =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionClassCase> |}>
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
let ``option class field`` records =
    testRoundtrip<{|
        Field1: option<Class> |}>
        records

[<Property>]
let ``option class field with bool field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithBoolField> |}>
        records

[<Property>]
let ``option class field with int8 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithInt8Field> |}>
        records

[<Property>]
let ``option class field with int16 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithInt16Field> |}>
        records

[<Property>]
let ``option class field with int32 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithInt32Field> |}>
        records

[<Property>]
let ``option class field with int64 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithInt64Field> |}>
        records

[<Property>]
let ``option class field with uint8 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithUInt8Field> |}>
        records

[<Property>]
let ``option class field with uint16 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithUInt16Field> |}>
        records

[<Property>]
let ``option class field with uint32 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithUInt32Field> |}>
        records

[<Property>]
let ``option class field with uint64 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithUInt64Field> |}>
        records

[<Property>]
let ``option class field with float32 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithFloat32Field> |}>
        records

[<Property>]
let ``option class field with float64 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithFloat64Field> |}>
        records

[<Property>]
let ``option class field with decimal field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithDecimalField> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option class field with date time field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithDateTimeField> |}>
        records

[<Property>]
let ``option class field with date time offset field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithDateTimeOffsetField> |}>
        records

[<Property>]
let ``option class field with string field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithStringField> |}>
        records

[<Property>]
let ``option class field with byte array field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithByteArrayField> |}>
        records

[<Property>]
let ``option class field with guid field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithGuidField> |}>
        records

[<Property>]
let ``option class field with array field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithArrayField> |}>
        records

[<Property>]
let ``option class field with generic list field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithGenericListField> |}>
        records

[<Property>]
let ``option class field with fsharp list field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithFSharpListField> |}>
        records

[<Property>]
let ``option class field with record field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithRecordField> |}>
        records

[<Property>]
let ``option class field with enum union field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithEnumUnionField> |}>
        records

[<Property>]
let ``option class field with complex union field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithComplexUnionField> |}>
        records

[<Property>]
let ``option class field with class field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithClassField> |}>
        records

[<Property>]
let ``option class field with nullable bool field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableBoolField> |}>
        records

[<Property>]
let ``option class field with nullable int8 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableInt8Field> |}>
        records

[<Property>]
let ``option class field with nullable int16 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableInt16Field> |}>
        records

[<Property>]
let ``option class field with nullable int32 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableInt32Field> |}>
        records

[<Property>]
let ``option class field with nullable int64 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableInt64Field> |}>
        records

[<Property>]
let ``option class field with nullable uint8 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableUInt8Field> |}>
        records

[<Property>]
let ``option class field with nullable uint16 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableUInt16Field> |}>
        records

[<Property>]
let ``option class field with nullable uint32 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableUInt32Field> |}>
        records

[<Property>]
let ``option class field with nullable uint64 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableUInt64Field> |}>
        records

[<Property>]
let ``option class field with nullable float32 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableFloat32Field> |}>
        records

[<Property>]
let ``option class field with nullable float64 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableFloat64Field> |}>
        records

[<Property>]
let ``option class field with nullable decimal field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableDecimalField> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option class field with nullable date time field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableDateTimeField> |}>
        records

[<Property>]
let ``option class field with nullable date time offset field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableDateTimeOffsetField> |}>
        records

[<Property>]
let ``option class field with nullable guid field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableGuidField> |}>
        records

[<Property>]
let ``option class field with nullable record field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithNullableRecordField> |}>
        records

[<Property>]
let ``option class field with option bool field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionBoolField> |}>
        records

[<Property>]
let ``option class field with option int8 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionInt8Field> |}>
        records

[<Property>]
let ``option class field with option int16 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionInt16Field> |}>
        records

[<Property>]
let ``option class field with option int32 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionInt32Field> |}>
        records

[<Property>]
let ``option class field with option int64 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionInt64Field> |}>
        records

[<Property>]
let ``option class field with option uint8 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionUInt8Field> |}>
        records

[<Property>]
let ``option class field with option uint16 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionUInt16Field> |}>
        records

[<Property>]
let ``option class field with option uint32 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionUInt32Field> |}>
        records

[<Property>]
let ``option class field with option uint64 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionUInt64Field> |}>
        records

[<Property>]
let ``option class field with option float32 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionFloat32Field> |}>
        records

[<Property>]
let ``option class field with option float64 field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionFloat64Field> |}>
        records

[<Property>]
let ``option class field with option decimal field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionDecimalField> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option class field with option date time field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionDateTimeField> |}>
        records

[<Property>]
let ``option class field with option date time offset field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionDateTimeOffsetField> |}>
        records

[<Property>]
let ``option class field with option string field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionStringField> |}>
        records

[<Property>]
let ``option class field with option byte array field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionByteArrayField> |}>
        records

[<Property>]
let ``option class field with option guid field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionGuidField> |}>
        records

[<Property>]
let ``option class field with option array field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionArrayField> |}>
        records

[<Property>]
let ``option class field with option generic list field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionGenericListField> |}>
        records

[<Property>]
let ``option class field with option fsharp list field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionFSharpListField> |}>
        records

[<Property>]
let ``option class field with option record field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionRecordField> |}>
        records

[<Property>]
let ``option class field with option enum union field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionEnumUnionField> |}>
        records

[<Property>]
let ``option class field with option complex union field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionComplexUnionField> |}>
        records

[<Property>]
let ``option class field with option class field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionClassField> |}>
        records

[<Property>]
let ``option class field with option nullable field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionNullableField> |}>
        records

[<Property>]
let ``option class field with option option field`` records =
    testRoundtrip<{|
        Field1: option<ClassWithOptionOptionField> |}>
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
