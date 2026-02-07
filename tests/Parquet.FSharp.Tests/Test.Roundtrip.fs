module rec Parquet.FSharp.Tests.Roundtrip

open Xunit
open FsCheck
open FsCheck.FSharp
open FsCheck.Xunit
open Parquet.FSharp
open System

type SimpleUnion =
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
type ComplexUnionWithFSharpRecordCase = Case1 of field1:{| Field1: int |}
type ComplexUnionWithSimpleUnionCase = Case1 of field1:SimpleUnion
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
type ComplexUnionWithNullableFSharpRecordCase = Case1 of field1:Nullable<struct {| Field1: int |}>
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
type ComplexUnionWithOptionFSharpRecordCase = Case1 of field1:option<{| Field1: int |}>
type ComplexUnionWithOptionSimpleUnionCase = Case1 of field1:option<SimpleUnion>
type ComplexUnionWithOptionComplexUnionCase = Case1 of field1:option<ComplexUnion>

type ArbitraryRecordArray<'Record> =
    static member Arbitrary =
        ArbMap.defaults
        |> ArbMap.generate<'Record>
        |> Gen.arrayOfLength 50
        |> Arb.fromGen

let testRoundtrip<'Record> =
    let config =
        Config.Default
            .WithRunner(XunitRunner())
            .WithMaxTest(10)
            .WithArbitrary([ typeof<ArbitraryRecordArray<'Record>> ])
    let property =
        fun records ->
            let bytes = ParquetSerializer.Serialize(records)
            let roundtrippedFSharpRecords = ParquetSerializer.Deserialize<'Record>(bytes)
            Assert.equal records roundtrippedFSharpRecords
    Check.One(config, property)

[<Fact>]
let ``bool field`` () =
    testRoundtrip<{|
        Field1: bool |}>

[<Fact>]
let ``int8 field`` () =
    testRoundtrip<{|
        Field1: int8 |}>

[<Fact>]
let ``int16 field`` () =
    testRoundtrip<{|
        Field1: int16 |}>

[<Fact>]
let ``int32 field`` () =
    testRoundtrip<{|
        Field1: int |}>

[<Fact>]
let ``int64 field`` () =
    testRoundtrip<{|
        Field1: int64 |}>

[<Fact>]
let ``uint8 field`` () =
    testRoundtrip<{|
        Field1: uint8 |}>

[<Fact>]
let ``uint16 field`` () =
    testRoundtrip<{|
        Field1: uint16 |}>

[<Fact>]
let ``uint32 field`` () =
    testRoundtrip<{|
        Field1: uint |}>

[<Fact>]
let ``uint64 field`` () =
    testRoundtrip<{|
        Field1: uint64 |}>

[<Fact>]
let ``float32 field`` () =
    testRoundtrip<{|
        Field1: float32 |}>

[<Fact>]
let ``float64 field`` () =
    testRoundtrip<{|
        Field1: float |}>

[<Fact>]
let ``decimal field`` () =
    testRoundtrip<{|
        Field1: decimal |}>

[<Fact>]
let ``date time field`` () =
    testRoundtrip<{|
        Field1: DateTime |}>

[<Fact>]
let ``date time offset field`` () =
    testRoundtrip<{|
        Field1: DateTimeOffset |}>

[<Fact>]
let ``string field`` () =
    testRoundtrip<{|
        Field1: string |}>

[<Fact>]
let ``byte array field`` () =
    testRoundtrip<{|
        Field1: byte[] |}>

[<Fact>]
let ``guid field`` () =
    testRoundtrip<{|
        Field1: Guid |}>

[<Fact>]
let ``array field with bool elements`` () =
    testRoundtrip<{|
        Field1: array<bool> |}>

[<Fact>]
let ``array field with int8 elements`` () =
    testRoundtrip<{|
        Field1: array<int8> |}>

[<Fact>]
let ``array field with int16 elements`` () =
    testRoundtrip<{|
        Field1: array<int16> |}>

[<Fact>]
let ``array field with int32 elements`` () =
    testRoundtrip<{|
        Field1: array<int> |}>

[<Fact>]
let ``array field with int64 elements`` () =
    testRoundtrip<{|
        Field1: array<int64> |}>

[<Fact>]
let ``array field with uint8 elements`` () =
    testRoundtrip<{|
        Field1: array<uint8> |}>

[<Fact>]
let ``array field with uint16 elements`` () =
    testRoundtrip<{|
        Field1: array<uint16> |}>

[<Fact>]
let ``array field with uint32 elements`` () =
    testRoundtrip<{|
        Field1: array<uint> |}>

[<Fact>]
let ``array field with uint64 elements`` () =
    testRoundtrip<{|
        Field1: array<uint64> |}>

[<Fact>]
let ``array field with float32 elements`` () =
    testRoundtrip<{|
        Field1: array<float32> |}>

[<Fact>]
let ``array field with float64 elements`` () =
    testRoundtrip<{|
        Field1: array<float> |}>

[<Fact>]
let ``array field with decimal elements`` () =
    testRoundtrip<{|
        Field1: array<decimal> |}>

[<Fact>]
let ``array field with date time elements`` () =
    testRoundtrip<{|
        Field1: array<DateTime> |}>

[<Fact>]
let ``array field with date time offset elements`` () =
    testRoundtrip<{|
        Field1: array<DateTimeOffset> |}>

[<Fact>]
let ``array field with string elements`` () =
    testRoundtrip<{|
        Field1: array<string> |}>

[<Fact>]
let ``array field with byte array elements`` () =
    testRoundtrip<{|
        Field1: array<byte[]> |}>

[<Fact>]
let ``array field with guid elements`` () =
    testRoundtrip<{|
        Field1: array<Guid> |}>

[<Fact>]
let ``array field with array elements`` () =
    testRoundtrip<{|
        Field1: array<array<int>> |}>

[<Fact>]
let ``array field with resize array elements`` () =
    testRoundtrip<{|
        Field1: array<ResizeArray<int>> |}>

[<Fact>]
let ``array field with list elements`` () =
    testRoundtrip<{|
        Field1: array<list<int>> |}>

[<Fact>]
let ``array field with record elements`` () =
    testRoundtrip<{|
        Field1: array<{|
            Field2: int |}> |}>

[<Fact>]
let ``array field with simple union elements`` () =
    testRoundtrip<{|
        Field1: array<SimpleUnion> |}>

[<Fact>]
let ``array field with complex union elements`` () =
    testRoundtrip<{|
        Field1: array<ComplexUnion> |}>

[<Fact>]
let ``array field with nullable bool elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<bool>> |}>

[<Fact>]
let ``array field with nullable int8 elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<int8>> |}>

[<Fact>]
let ``array field with nullable int16 elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<int16>> |}>

[<Fact>]
let ``array field with nullable int32 elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<int>> |}>

[<Fact>]
let ``array field with nullable int64 elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<int64>> |}>

[<Fact>]
let ``array field with nullable uint8 elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<uint8>> |}>

[<Fact>]
let ``array field with nullable uint16 elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<uint16>> |}>

[<Fact>]
let ``array field with nullable uint32 elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<uint>> |}>

[<Fact>]
let ``array field with nullable uint64 elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<uint64>> |}>

[<Fact>]
let ``array field with nullable float32 elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<float32>> |}>

[<Fact>]
let ``array field with nullable float64 elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<float>> |}>

[<Fact>]
let ``array field with nullable decimal elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<decimal>> |}>

[<Fact>]
let ``array field with nullable date time elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<DateTime>> |}>

[<Fact>]
let ``array field with nullable date time offset elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<DateTimeOffset>> |}>

[<Fact>]
let ``array field with nullable guid elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<Guid>> |}>

[<Fact>]
let ``array field with nullable record elements`` () =
    testRoundtrip<{|
        Field1: array<Nullable<struct {|
            Field2: int |}>> |}>

[<Fact>]
let ``array field with option bool elements`` () =
    testRoundtrip<{|
        Field1: array<option<bool>> |}>

[<Fact>]
let ``array field with option int8 elements`` () =
    testRoundtrip<{|
        Field1: array<option<int8>> |}>

[<Fact>]
let ``array field with option int16 elements`` () =
    testRoundtrip<{|
        Field1: array<option<int16>> |}>

[<Fact>]
let ``array field with option int32 elements`` () =
    testRoundtrip<{|
        Field1: array<option<int>> |}>

[<Fact>]
let ``array field with option int64 elements`` () =
    testRoundtrip<{|
        Field1: array<option<int64>> |}>

[<Fact>]
let ``array field with option uint8 elements`` () =
    testRoundtrip<{|
        Field1: array<option<uint8>> |}>

[<Fact>]
let ``array field with option uint16 elements`` () =
    testRoundtrip<{|
        Field1: array<option<uint16>> |}>

[<Fact>]
let ``array field with option uint32 elements`` () =
    testRoundtrip<{|
        Field1: array<option<uint>> |}>

[<Fact>]
let ``array field with option uint64 elements`` () =
    testRoundtrip<{|
        Field1: array<option<uint64>> |}>

[<Fact>]
let ``array field with option float32 elements`` () =
    testRoundtrip<{|
        Field1: array<option<float32>> |}>

[<Fact>]
let ``array field with option float64 elements`` () =
    testRoundtrip<{|
        Field1: array<option<float>> |}>

[<Fact>]
let ``array field with option decimal elements`` () =
    testRoundtrip<{|
        Field1: array<option<decimal>> |}>

[<Fact>]
let ``array field with option date time elements`` () =
    testRoundtrip<{|
        Field1: array<option<DateTime>> |}>

[<Fact>]
let ``array field with option date time offset elements`` () =
    testRoundtrip<{|
        Field1: array<option<DateTimeOffset>> |}>

[<Fact>]
let ``array field with option string elements`` () =
    testRoundtrip<{|
        Field1: array<option<string>> |}>

[<Fact>]
let ``array field with option byte array elements`` () =
    testRoundtrip<{|
        Field1: array<option<byte[]>> |}>

[<Fact>]
let ``array field with option guid elements`` () =
    testRoundtrip<{|
        Field1: array<option<Guid>> |}>

[<Fact>]
let ``array field with option array elements`` () =
    testRoundtrip<{|
        Field1: array<option<array<int>>> |}>

[<Fact>]
let ``array field with option resize array elements`` () =
    testRoundtrip<{|
        Field1: array<option<ResizeArray<int>>> |}>

[<Fact>]
let ``array field with option list elements`` () =
    testRoundtrip<{|
        Field1: array<option<list<int>>> |}>

[<Fact>]
let ``array field with option record elements`` () =
    testRoundtrip<{|
        Field1: array<option<{|
            Field2: int |}>> |}>

[<Fact>]
let ``array field with option simple union elements`` () =
    testRoundtrip<{|
        Field1: array<option<SimpleUnion>> |}>

[<Fact>]
let ``array field with option complex union elements`` () =
    testRoundtrip<{|
        Field1: array<option<ComplexUnion>> |}>

[<Fact>]
let ``resize array field with bool elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<bool> |}>

[<Fact>]
let ``resize array field with int8 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<int8> |}>

[<Fact>]
let ``resize array field with int16 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<int16> |}>

[<Fact>]
let ``resize array field with int32 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<int> |}>

[<Fact>]
let ``resize array field with int64 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<int64> |}>

[<Fact>]
let ``resize array field with uint8 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<uint8> |}>

[<Fact>]
let ``resize array field with uint16 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<uint16> |}>

[<Fact>]
let ``resize array field with uint32 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<uint> |}>

[<Fact>]
let ``resize array field with uint64 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<uint64> |}>

[<Fact>]
let ``resize array field with float32 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<float32> |}>

[<Fact>]
let ``resize array field with float64 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<float> |}>

[<Fact>]
let ``resize array field with decimal elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<decimal> |}>

[<Fact>]
let ``resize array field with date time elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<DateTime> |}>

[<Fact>]
let ``resize array field with date time offset elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<DateTimeOffset> |}>

[<Fact>]
let ``resize array field with string elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<string> |}>

[<Fact>]
let ``resize array field with byte array elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<byte[]> |}>

[<Fact>]
let ``resize array field with guid elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Guid> |}>

[<Fact>]
let ``resize array field with array elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<array<int>> |}>

[<Fact>]
let ``resize array field with resize array elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<ResizeArray<int>> |}>

[<Fact>]
let ``resize array field with list elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<list<int>> |}>

[<Fact>]
let ``resize array field with record elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<{|
            Field2: int |}> |}>

[<Fact>]
let ``resize array field with simple union elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<SimpleUnion> |}>

[<Fact>]
let ``resize array field with complex union elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<ComplexUnion> |}>

[<Fact>]
let ``resize array field with nullable bool elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<bool>> |}>

[<Fact>]
let ``resize array field with nullable int8 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<int8>> |}>

[<Fact>]
let ``resize array field with nullable int16 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<int16>> |}>

[<Fact>]
let ``resize array field with nullable int32 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<int>> |}>

[<Fact>]
let ``resize array field with nullable int64 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<int64>> |}>

[<Fact>]
let ``resize array field with nullable uint8 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<uint8>> |}>

[<Fact>]
let ``resize array field with nullable uint16 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<uint16>> |}>

[<Fact>]
let ``resize array field with nullable uint32 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<uint>> |}>

[<Fact>]
let ``resize array field with nullable uint64 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<uint64>> |}>

[<Fact>]
let ``resize array field with nullable float32 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<float32>> |}>

[<Fact>]
let ``resize array field with nullable float64 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<float>> |}>

[<Fact>]
let ``resize array field with nullable decimal elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<decimal>> |}>

[<Fact>]
let ``resize array field with nullable date time elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<DateTime>> |}>

[<Fact>]
let ``resize array field with nullable date time offset elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<DateTimeOffset>> |}>

[<Fact>]
let ``resize array field with nullable guid elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<Guid>> |}>

[<Fact>]
let ``resize array field with nullable record elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<Nullable<struct {|
            Field2: int |}>> |}>

[<Fact>]
let ``resize array field with option bool elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<bool>> |}>

[<Fact>]
let ``resize array field with option int8 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<int8>> |}>

[<Fact>]
let ``resize array field with option int16 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<int16>> |}>

[<Fact>]
let ``resize array field with option int32 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<int>> |}>

[<Fact>]
let ``resize array field with option int64 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<int64>> |}>

[<Fact>]
let ``resize array field with option uint8 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<uint8>> |}>

[<Fact>]
let ``resize array field with option uint16 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<uint16>> |}>

[<Fact>]
let ``resize array field with option uint32 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<uint>> |}>

[<Fact>]
let ``resize array field with option uint64 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<uint64>> |}>

[<Fact>]
let ``resize array field with option float32 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<float32>> |}>

[<Fact>]
let ``resize array field with option float64 elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<float>> |}>

[<Fact>]
let ``resize array field with option decimal elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<decimal>> |}>

[<Fact>]
let ``resize array field with option date time elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<DateTime>> |}>

[<Fact>]
let ``resize array field with option date time offset elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<DateTimeOffset>> |}>

[<Fact>]
let ``resize array field with option string elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<string>> |}>

[<Fact>]
let ``resize array field with option byte array elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<byte[]>> |}>

[<Fact>]
let ``resize array field with option guid elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<Guid>> |}>

[<Fact>]
let ``resize array field with option array elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<array<int>>> |}>

[<Fact>]
let ``resize array field with option resize array elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<ResizeArray<int>>> |}>

[<Fact>]
let ``resize array field with option list elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<list<int>>> |}>

[<Fact>]
let ``resize array field with option record elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<{|
            Field2: int |}>> |}>

[<Fact>]
let ``resize array field with option simple union elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<SimpleUnion>> |}>

[<Fact>]
let ``resize array field with option complex union elements`` () =
    testRoundtrip<{|
        Field1: ResizeArray<option<ComplexUnion>> |}>

[<Fact>]
let ``list field with bool elements`` () =
    testRoundtrip<{|
        Field1: list<bool> |}>

[<Fact>]
let ``list field with int8 elements`` () =
    testRoundtrip<{|
        Field1: list<int8> |}>

[<Fact>]
let ``list field with int16 elements`` () =
    testRoundtrip<{|
        Field1: list<int16> |}>

[<Fact>]
let ``list field with int32 elements`` () =
    testRoundtrip<{|
        Field1: list<int> |}>

[<Fact>]
let ``list field with int64 elements`` () =
    testRoundtrip<{|
        Field1: list<int64> |}>

[<Fact>]
let ``list field with uint8 elements`` () =
    testRoundtrip<{|
        Field1: list<uint8> |}>

[<Fact>]
let ``list field with uint16 elements`` () =
    testRoundtrip<{|
        Field1: list<uint16> |}>

[<Fact>]
let ``list field with uint32 elements`` () =
    testRoundtrip<{|
        Field1: list<uint> |}>

[<Fact>]
let ``list field with uint64 elements`` () =
    testRoundtrip<{|
        Field1: list<uint64> |}>

[<Fact>]
let ``list field with float32 elements`` () =
    testRoundtrip<{|
        Field1: list<float32> |}>

[<Fact>]
let ``list field with float64 elements`` () =
    testRoundtrip<{|
        Field1: list<float> |}>

[<Fact>]
let ``list field with decimal elements`` () =
    testRoundtrip<{|
        Field1: list<decimal> |}>

[<Fact>]
let ``list field with date time elements`` () =
    testRoundtrip<{|
        Field1: list<DateTime> |}>

[<Fact>]
let ``list field with date time offset elements`` () =
    testRoundtrip<{|
        Field1: list<DateTimeOffset> |}>

[<Fact>]
let ``list field with string elements`` () =
    testRoundtrip<{|
        Field1: list<string> |}>

[<Fact>]
let ``list field with byte array elements`` () =
    testRoundtrip<{|
        Field1: list<byte[]> |}>

[<Fact>]
let ``list field with guid elements`` () =
    testRoundtrip<{|
        Field1: list<Guid> |}>

[<Fact>]
let ``list field with array elements`` () =
    testRoundtrip<{|
        Field1: list<array<int>> |}>

[<Fact>]
let ``list field with resize array elements`` () =
    testRoundtrip<{|
        Field1: list<ResizeArray<int>> |}>

[<Fact>]
let ``list field with list elements`` () =
    testRoundtrip<{|
        Field1: list<list<int>> |}>

[<Fact>]
let ``list field with record elements`` () =
    testRoundtrip<{|
        Field1: list<{|
            Field2: int |}> |}>

[<Fact>]
let ``list field with simple union elements`` () =
    testRoundtrip<{|
        Field1: list<SimpleUnion> |}>

[<Fact>]
let ``list field with complex union elements`` () =
    testRoundtrip<{|
        Field1: list<ComplexUnion> |}>

[<Fact>]
let ``list field with nullable bool elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<bool>> |}>

[<Fact>]
let ``list field with nullable int8 elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<int8>> |}>

[<Fact>]
let ``list field with nullable int16 elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<int16>> |}>

[<Fact>]
let ``list field with nullable int32 elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<int>> |}>

[<Fact>]
let ``list field with nullable int64 elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<int64>> |}>

[<Fact>]
let ``list field with nullable uint8 elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<uint8>> |}>

[<Fact>]
let ``list field with nullable uint16 elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<uint16>> |}>

[<Fact>]
let ``list field with nullable uint32 elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<uint>> |}>

[<Fact>]
let ``list field with nullable uint64 elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<uint64>> |}>

[<Fact>]
let ``list field with nullable float32 elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<float32>> |}>

[<Fact>]
let ``list field with nullable float64 elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<float>> |}>

[<Fact>]
let ``list field with nullable decimal elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<decimal>> |}>

[<Fact>]
let ``list field with nullable date time elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<DateTime>> |}>

[<Fact>]
let ``list field with nullable date time offset elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<DateTimeOffset>> |}>

[<Fact>]
let ``list field with nullable guid elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<Guid>> |}>

[<Fact>]
let ``list field with nullable record elements`` () =
    testRoundtrip<{|
        Field1: list<Nullable<struct {|
            Field2: int |}>> |}>

[<Fact>]
let ``list field with option bool elements`` () =
    testRoundtrip<{|
        Field1: list<option<bool>> |}>

[<Fact>]
let ``list field with option int8 elements`` () =
    testRoundtrip<{|
        Field1: list<option<int8>> |}>

[<Fact>]
let ``list field with option int16 elements`` () =
    testRoundtrip<{|
        Field1: list<option<int16>> |}>

[<Fact>]
let ``list field with option int32 elements`` () =
    testRoundtrip<{|
        Field1: list<option<int>> |}>

[<Fact>]
let ``list field with option int64 elements`` () =
    testRoundtrip<{|
        Field1: list<option<int64>> |}>

[<Fact>]
let ``list field with option uint8 elements`` () =
    testRoundtrip<{|
        Field1: list<option<uint8>> |}>

[<Fact>]
let ``list field with option uint16 elements`` () =
    testRoundtrip<{|
        Field1: list<option<uint16>> |}>

[<Fact>]
let ``list field with option uint32 elements`` () =
    testRoundtrip<{|
        Field1: list<option<uint>> |}>

[<Fact>]
let ``list field with option uint64 elements`` () =
    testRoundtrip<{|
        Field1: list<option<uint64>> |}>

[<Fact>]
let ``list field with option float32 elements`` () =
    testRoundtrip<{|
        Field1: list<option<float32>> |}>

[<Fact>]
let ``list field with option float64 elements`` () =
    testRoundtrip<{|
        Field1: list<option<float>> |}>

[<Fact>]
let ``list field with option decimal elements`` () =
    testRoundtrip<{|
        Field1: list<option<decimal>> |}>

[<Fact>]
let ``list field with option date time elements`` () =
    testRoundtrip<{|
        Field1: list<option<DateTime>> |}>

[<Fact>]
let ``list field with option date time offset elements`` () =
    testRoundtrip<{|
        Field1: list<option<DateTimeOffset>> |}>

[<Fact>]
let ``list field with option string elements`` () =
    testRoundtrip<{|
        Field1: list<option<string>> |}>

[<Fact>]
let ``list field with option byte array elements`` () =
    testRoundtrip<{|
        Field1: list<option<byte[]>> |}>

[<Fact>]
let ``list field with option guid elements`` () =
    testRoundtrip<{|
        Field1: list<option<Guid>> |}>

[<Fact>]
let ``list field with option array elements`` () =
    testRoundtrip<{|
        Field1: list<option<array<int>>> |}>

[<Fact>]
let ``list field with option resize array elements`` () =
    testRoundtrip<{|
        Field1: list<option<ResizeArray<int>>> |}>

[<Fact>]
let ``list field with option list elements`` () =
    testRoundtrip<{|
        Field1: list<option<list<int>>> |}>

[<Fact>]
let ``list field with option record elements`` () =
    testRoundtrip<{|
        Field1: list<option<{|
            Field2: int |}>> |}>

[<Fact>]
let ``list field with option simple union elements`` () =
    testRoundtrip<{|
        Field1: list<option<SimpleUnion>> |}>

[<Fact>]
let ``list field with option complex union elements`` () =
    testRoundtrip<{|
        Field1: list<option<ComplexUnion>> |}>

[<Fact>]
let ``record field with bool field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: bool |} |}>

[<Fact>]
let ``record field with int8 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: int8 |} |}>

[<Fact>]
let ``record field with int16 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: int16 |} |}>

[<Fact>]
let ``record field with int32 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: int |} |}>

[<Fact>]
let ``record field with int64 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: int64 |} |}>

[<Fact>]
let ``record field with uint8 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: uint8 |} |}>

[<Fact>]
let ``record field with uint16 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: uint16 |} |}>

[<Fact>]
let ``record field with uint32 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: uint |} |}>

[<Fact>]
let ``record field with uint64 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: uint64 |} |}>

[<Fact>]
let ``record field with float32 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: float32 |} |}>

[<Fact>]
let ``record field with float64 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: float |} |}>

[<Fact>]
let ``record field with decimal field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: decimal |} |}>

[<Fact>]
let ``record field with date time field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: DateTime |} |}>

[<Fact>]
let ``record field with date time offset field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: DateTimeOffset |} |}>

[<Fact>]
let ``record field with string field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: string |} |}>

[<Fact>]
let ``record field with byte array field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: byte[] |} |}>

[<Fact>]
let ``record field with guid field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Guid |} |}>

[<Fact>]
let ``record field with array field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: array<int> |} |}>

[<Fact>]
let ``record field with resize array field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: ResizeArray<int> |} |}>

[<Fact>]
let ``record field with list field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: list<int> |} |}>

[<Fact>]
let ``record field with record field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: {|
                Field3: int |} |} |}>

[<Fact>]
let ``record field with simple union field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: SimpleUnion |} |}>

[<Fact>]
let ``record field with complex union field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: ComplexUnion |} |}>

[<Fact>]
let ``record field with nullable bool field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<bool> |} |}>

[<Fact>]
let ``record field with nullable int8 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<int8> |} |}>

[<Fact>]
let ``record field with nullable int16 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<int16> |} |}>

[<Fact>]
let ``record field with nullable int32 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<int> |} |}>

[<Fact>]
let ``record field with nullable int64 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<int64> |} |}>

[<Fact>]
let ``record field with nullable uint8 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<uint8> |} |}>

[<Fact>]
let ``record field with nullable uint16 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<uint16> |} |}>

[<Fact>]
let ``record field with nullable uint32 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<uint> |} |}>

[<Fact>]
let ``record field with nullable uint64 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<uint64> |} |}>

[<Fact>]
let ``record field with nullable float32 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<float32> |} |}>

[<Fact>]
let ``record field with nullable float64 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<float> |} |}>

[<Fact>]
let ``record field with nullable decimal field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<decimal> |} |}>

[<Fact>]
let ``record field with nullable date time field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<DateTime> |} |}>

[<Fact>]
let ``record field with nullable date time offset field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<DateTimeOffset> |} |}>

[<Fact>]
let ``record field with nullable guid field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<Guid> |} |}>

[<Fact>]
let ``record field with nullable record field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: Nullable<struct {|
                Field3: int |}> |} |}>

[<Fact>]
let ``record field with option bool field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<bool> |} |}>

[<Fact>]
let ``record field with option int8 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<int8> |} |}>

[<Fact>]
let ``record field with option int16 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<int16> |} |}>

[<Fact>]
let ``record field with option int32 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<int> |} |}>

[<Fact>]
let ``record field with option int64 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<int64> |} |}>

[<Fact>]
let ``record field with option uint8 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<uint8> |} |}>

[<Fact>]
let ``record field with option uint16 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<uint16> |} |}>

[<Fact>]
let ``record field with option uint32 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<uint> |} |}>

[<Fact>]
let ``record field with option uint64 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<uint64> |} |}>

[<Fact>]
let ``record field with option float32 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<float32> |} |}>

[<Fact>]
let ``record field with option float64 field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<float> |} |}>

[<Fact>]
let ``record field with option decimal field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<decimal> |} |}>

[<Fact>]
let ``record field with option date time field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<DateTime> |} |}>

[<Fact>]
let ``record field with option date time offset field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<DateTimeOffset> |} |}>

[<Fact>]
let ``record field with option string field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<string> |} |}>

[<Fact>]
let ``record field with option byte array field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<byte[]> |} |}>

[<Fact>]
let ``record field with option guid field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<Guid> |} |}>

[<Fact>]
let ``record field with option array field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<array<int>> |} |}>

[<Fact>]
let ``record field with option resize array field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<ResizeArray<int>> |} |}>

[<Fact>]
let ``record field with option list field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<list<int>> |} |}>

[<Fact>]
let ``record field with option record field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<{|
                Field3: int |}> |} |}>

[<Fact>]
let ``record field with option simple union field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<SimpleUnion> |} |}>

[<Fact>]
let ``record field with option complex union field`` () =
    testRoundtrip<{|
        Field1: {|
            Field2: option<ComplexUnion> |} |}>

[<Fact>]
let ``simple union field`` () =
    testRoundtrip<{|
        Field1: SimpleUnion |}>

[<Fact>]
let ``complex union field`` () =
    testRoundtrip<{|
        Field1: ComplexUnion |}>

[<Fact>]
let ``complex union field with bool case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithBoolCase |}>

[<Fact>]
let ``complex union field with int8 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithInt8Case |}>

[<Fact>]
let ``complex union field with int16 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithInt16Case |}>

[<Fact>]
let ``complex union field with int32 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithInt32Case |}>

[<Fact>]
let ``complex union field with int64 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithInt64Case |}>

[<Fact>]
let ``complex union field with uint8 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithUInt8Case |}>

[<Fact>]
let ``complex union field with uint16 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithUInt16Case |}>

[<Fact>]
let ``complex union field with uint32 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithUInt32Case |}>

[<Fact>]
let ``complex union field with uint64 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithUInt64Case |}>

[<Fact>]
let ``complex union field with float32 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithFloat32Case |}>

[<Fact>]
let ``complex union field with float64 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithFloat64Case |}>

[<Fact>]
let ``complex union field with decimal case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithDecimalCase |}>

[<Fact>]
let ``complex union field with date time case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithDateTimeCase |}>

[<Fact>]
let ``complex union field with date time offset case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithDateTimeOffsetCase |}>

[<Fact>]
let ``complex union field with string case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithStringCase |}>

[<Fact>]
let ``complex union field with byte array case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithByteArrayCase |}>

[<Fact>]
let ``complex union field with guid case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithGuidCase |}>

[<Fact>]
let ``complex union field with array case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithArrayCase |}>

[<Fact>]
let ``complex union field with resize array case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithGenericListCase |}>

[<Fact>]
let ``complex union field with list case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithFSharpListCase |}>

[<Fact>]
let ``complex union field with record case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithFSharpRecordCase |}>

[<Fact>]
let ``complex union field with simple union case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithSimpleUnionCase |}>

[<Fact>]
let ``complex union field with complex union case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithComplexUnionCase |}>

[<Fact>]
let ``complex union field with nullable bool case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableBoolCase |}>

[<Fact>]
let ``complex union field with nullable int8 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableInt8Case |}>

[<Fact>]
let ``complex union field with nullable int16 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableInt16Case |}>

[<Fact>]
let ``complex union field with nullable int32 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableInt32Case |}>

[<Fact>]
let ``complex union field with nullable int64 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableInt64Case |}>

[<Fact>]
let ``complex union field with nullable uint8 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableUInt8Case |}>

[<Fact>]
let ``complex union field with nullable uint16 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableUInt16Case |}>

[<Fact>]
let ``complex union field with nullable uint32 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableUInt32Case |}>

[<Fact>]
let ``complex union field with nullable uint64 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableUInt64Case |}>

[<Fact>]
let ``complex union field with nullable float32 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableFloat32Case |}>

[<Fact>]
let ``complex union field with nullable float64 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableFloat64Case |}>

[<Fact>]
let ``complex union field with nullable decimal case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableDecimalCase |}>

[<Fact>]
let ``complex union field with nullable date time case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableDateTimeCase |}>

[<Fact>]
let ``complex union field with nullable date time offset case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableDateTimeOffsetCase |}>

[<Fact>]
let ``complex union field with nullable guid case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableGuidCase |}>

[<Fact>]
let ``complex union field with nullable record case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithNullableFSharpRecordCase |}>

[<Fact>]
let ``complex union field with option bool case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionBoolCase |}>

[<Fact>]
let ``complex union field with option int8 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionInt8Case |}>

[<Fact>]
let ``complex union field with option int16 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionInt16Case |}>

[<Fact>]
let ``complex union field with option int32 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionInt32Case |}>

[<Fact>]
let ``complex union field with option int64 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionInt64Case |}>

[<Fact>]
let ``complex union field with option uint8 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionUInt8Case |}>

[<Fact>]
let ``complex union field with option uint16 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionUInt16Case |}>

[<Fact>]
let ``complex union field with option uint32 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionUInt32Case |}>

[<Fact>]
let ``complex union field with option uint64 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionUInt64Case |}>

[<Fact>]
let ``complex union field with option float32 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionFloat32Case |}>

[<Fact>]
let ``complex union field with option float64 case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionFloat64Case |}>

[<Fact>]
let ``complex union field with option decimal case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionDecimalCase |}>

[<Fact>]
let ``complex union field with option date time case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionDateTimeCase |}>

[<Fact>]
let ``complex union field with option date time offset case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionDateTimeOffsetCase |}>

[<Fact>]
let ``complex union field with option string case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionStringCase |}>

[<Fact>]
let ``complex union field with option byte array case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionByteArrayCase |}>

[<Fact>]
let ``complex union field with option guid case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionGuidCase |}>

[<Fact>]
let ``complex union field with option array case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionArrayCase |}>

[<Fact>]
let ``complex union field with option resize array case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionGenericListCase |}>

[<Fact>]
let ``complex union field with option list case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionFSharpListCase |}>

[<Fact>]
let ``complex union field with option record case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionFSharpRecordCase |}>

[<Fact>]
let ``complex union field with option simple union case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionSimpleUnionCase |}>

[<Fact>]
let ``complex union field with option complex union case`` () =
    testRoundtrip<{|
        Field1: ComplexUnionWithOptionComplexUnionCase |}>

[<Fact>]
let ``nullable bool field`` () =
    testRoundtrip<{|
        Field1: Nullable<bool> |}>

[<Fact>]
let ``nullable int8 field`` () =
    testRoundtrip<{|
        Field1: Nullable<int8> |}>

[<Fact>]
let ``nullable int16 field`` () =
    testRoundtrip<{|
        Field1: Nullable<int16> |}>

[<Fact>]
let ``nullable int32 field`` () =
    testRoundtrip<{|
        Field1: Nullable<int> |}>

[<Fact>]
let ``nullable int64 field`` () =
    testRoundtrip<{|
        Field1: Nullable<int64> |}>

[<Fact>]
let ``nullable uint8 field`` () =
    testRoundtrip<{|
        Field1: Nullable<uint8> |}>

[<Fact>]
let ``nullable uint16 field`` () =
    testRoundtrip<{|
        Field1: Nullable<uint16> |}>

[<Fact>]
let ``nullable uint32 field`` () =
    testRoundtrip<{|
        Field1: Nullable<uint> |}>

[<Fact>]
let ``nullable uint64 field`` () =
    testRoundtrip<{|
        Field1: Nullable<uint64> |}>

[<Fact>]
let ``nullable float32 field`` () =
    testRoundtrip<{|
        Field1: Nullable<float32> |}>

[<Fact>]
let ``nullable float64 field`` () =
    testRoundtrip<{|
        Field1: Nullable<float> |}>

[<Fact>]
let ``nullable decimal field`` () =
    testRoundtrip<{|
        Field1: Nullable<decimal> |}>

[<Fact>]
let ``nullable date time field`` () =
    testRoundtrip<{|
        Field1: Nullable<DateTime> |}>

[<Fact>]
let ``nullable date time offset field`` () =
    testRoundtrip<{|
        Field1: Nullable<DateTimeOffset> |}>

[<Fact>]
let ``nullable guid field`` () =
    testRoundtrip<{|
        Field1: Nullable<Guid> |}>

[<Fact>]
let ``nullable record field with bool field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: bool |}> |}>

[<Fact>]
let ``nullable record field with int8 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: int8 |}> |}>

[<Fact>]
let ``nullable record field with int16 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: int16 |}> |}>

[<Fact>]
let ``nullable record field with int32 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: int |}> |}>

[<Fact>]
let ``nullable record field with int64 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: int64 |}> |}>

[<Fact>]
let ``nullable record field with uint8 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: uint8 |}> |}>

[<Fact>]
let ``nullable record field with uint16 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: uint16 |}> |}>

[<Fact>]
let ``nullable record field with uint32 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: uint |}> |}>

[<Fact>]
let ``nullable record field with uint64 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: uint64 |}> |}>

[<Fact>]
let ``nullable record field with float32 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: float32 |}> |}>

[<Fact>]
let ``nullable record field with float64 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: float |}> |}>

[<Fact>]
let ``nullable record field with decimal field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: decimal |}> |}>

[<Fact>]
let ``nullable record field with date time field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: DateTime |}> |}>

[<Fact>]
let ``nullable record field with date time offset field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: DateTimeOffset |}> |}>

[<Fact>]
let ``nullable record field with string field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: string |}> |}>

[<Fact>]
let ``nullable record field with byte array field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: byte[] |}> |}>

[<Fact>]
let ``nullable record field with guid field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Guid |}> |}>

[<Fact>]
let ``nullable record field with array field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: array<int> |}> |}>

[<Fact>]
let ``nullable record field with resize array field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: ResizeArray<int> |}> |}>

[<Fact>]
let ``nullable record field with record field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: struct {|
                Field3: int |} |}> |}>

[<Fact>]
let ``nullable record field with nullable bool field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<bool> |}> |}>

[<Fact>]
let ``nullable record field with nullable int8 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<int8> |}> |}>

[<Fact>]
let ``nullable record field with nullable int16 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<int16> |}> |}>

[<Fact>]
let ``nullable record field with nullable int32 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<int> |}> |}>

[<Fact>]
let ``nullable record field with nullable int64 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<int64> |}> |}>

[<Fact>]
let ``nullable record field with nullable uint8 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<uint8> |}> |}>

[<Fact>]
let ``nullable record field with nullable uint16 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<uint16> |}> |}>

[<Fact>]
let ``nullable record field with nullable uint32 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<uint> |}> |}>

[<Fact>]
let ``nullable record field with nullable uint64 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<uint64> |}> |}>

[<Fact>]
let ``nullable record field with nullable float32 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<float32> |}> |}>

[<Fact>]
let ``nullable record field with nullable float64 field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<float> |}> |}>

[<Fact>]
let ``nullable record field with nullable decimal field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<decimal> |}> |}>

[<Fact>]
let ``nullable record field with nullable date time field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<DateTime> |}> |}>

[<Fact>]
let ``nullable record field with nullable date time offset field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<DateTimeOffset> |}> |}>

[<Fact>]
let ``nullable record field with nullable guid field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<Guid> |}> |}>

[<Fact>]
let ``nullable record field with nullable record field`` () =
    testRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<struct {|
                Field3: int |}> |}> |}>

[<Fact>]
let ``option bool field`` () =
    testRoundtrip<{|
        Field1: option<bool> |}>

[<Fact>]
let ``option int8 field`` () =
    testRoundtrip<{|
        Field1: option<int8> |}>

[<Fact>]
let ``option int16 field`` () =
    testRoundtrip<{|
        Field1: option<int16> |}>

[<Fact>]
let ``option int32 field`` () =
    testRoundtrip<{|
        Field1: option<int> |}>

[<Fact>]
let ``option int64 field`` () =
    testRoundtrip<{|
        Field1: option<int64> |}>

[<Fact>]
let ``option uint8 field`` () =
    testRoundtrip<{|
        Field1: option<uint8> |}>

[<Fact>]
let ``option uint16 field`` () =
    testRoundtrip<{|
        Field1: option<uint16> |}>

[<Fact>]
let ``option uint32 field`` () =
    testRoundtrip<{|
        Field1: option<uint> |}>

[<Fact>]
let ``option uint64 field`` () =
    testRoundtrip<{|
        Field1: option<uint64> |}>

[<Fact>]
let ``option float32 field`` () =
    testRoundtrip<{|
        Field1: option<float32> |}>

[<Fact>]
let ``option float64 field`` () =
    testRoundtrip<{|
        Field1: option<float> |}>

[<Fact>]
let ``option decimal field`` () =
    testRoundtrip<{|
        Field1: option<decimal> |}>

[<Fact>]
let ``option date time field`` () =
    testRoundtrip<{|
        Field1: option<DateTime> |}>

[<Fact>]
let ``option date time offset field`` () =
    testRoundtrip<{|
        Field1: option<DateTimeOffset> |}>

[<Fact>]
let ``option string field`` () =
    testRoundtrip<{|
        Field1: option<string> |}>

[<Fact>]
let ``option byte array field`` () =
    testRoundtrip<{|
        Field1: option<byte[]> |}>

[<Fact>]
let ``option guid field`` () =
    testRoundtrip<{|
        Field1: option<Guid> |}>

[<Fact>]
let ``option array field with bool elements`` () =
    testRoundtrip<{|
        Field1: option<array<bool>> |}>

[<Fact>]
let ``option array field with int8 elements`` () =
    testRoundtrip<{|
        Field1: option<array<int8>> |}>

[<Fact>]
let ``option array field with int16 elements`` () =
    testRoundtrip<{|
        Field1: option<array<int16>> |}>

[<Fact>]
let ``option array field with int32 elements`` () =
    testRoundtrip<{|
        Field1: option<array<int>> |}>

[<Fact>]
let ``option array field with int64 elements`` () =
    testRoundtrip<{|
        Field1: option<array<int64>> |}>

[<Fact>]
let ``option array field with uint8 elements`` () =
    testRoundtrip<{|
        Field1: option<array<uint8>> |}>

[<Fact>]
let ``option array field with uint16 elements`` () =
    testRoundtrip<{|
        Field1: option<array<uint16>> |}>

[<Fact>]
let ``option array field with uint32 elements`` () =
    testRoundtrip<{|
        Field1: option<array<uint>> |}>

[<Fact>]
let ``option array field with uint64 elements`` () =
    testRoundtrip<{|
        Field1: option<array<uint64>> |}>

[<Fact>]
let ``option array field with float32 elements`` () =
    testRoundtrip<{|
        Field1: option<array<float32>> |}>

[<Fact>]
let ``option array field with float64 elements`` () =
    testRoundtrip<{|
        Field1: option<array<float>> |}>

[<Fact>]
let ``option array field with decimal elements`` () =
    testRoundtrip<{|
        Field1: option<array<decimal>> |}>

[<Fact>]
let ``option array field with date time elements`` () =
    testRoundtrip<{|
        Field1: option<array<DateTime>> |}>

[<Fact>]
let ``option array field with date time offset elements`` () =
    testRoundtrip<{|
        Field1: option<array<DateTimeOffset>> |}>

[<Fact>]
let ``option array field with string elements`` () =
    testRoundtrip<{|
        Field1: option<array<string>> |}>

[<Fact>]
let ``option array field with byte array elements`` () =
    testRoundtrip<{|
        Field1: option<array<byte[]>> |}>

[<Fact>]
let ``option array field with guid elements`` () =
    testRoundtrip<{|
        Field1: option<array<Guid>> |}>

[<Fact>]
let ``option array field with array elements`` () =
    testRoundtrip<{|
        Field1: option<array<array<int>>> |}>

[<Fact>]
let ``option array field with resize array elements`` () =
    testRoundtrip<{|
        Field1: option<array<ResizeArray<int>>> |}>

[<Fact>]
let ``option array field with list elements`` () =
    testRoundtrip<{|
        Field1: option<array<list<int>>> |}>

[<Fact>]
let ``option array field with record elements`` () =
    testRoundtrip<{|
        Field1: option<array<{|
            Field2: int |}>> |}>

[<Fact>]
let ``option array field with simple union elements`` () =
    testRoundtrip<{|
        Field1: option<array<SimpleUnion>> |}>

[<Fact>]
let ``option array field with complex union elements`` () =
    testRoundtrip<{|
        Field1: option<array<ComplexUnion>> |}>

[<Fact>]
let ``option array field with nullable bool elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<bool>>> |}>

[<Fact>]
let ``option array field with nullable int8 elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<int8>>> |}>

[<Fact>]
let ``option array field with nullable int16 elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<int16>>> |}>

[<Fact>]
let ``option array field with nullable int32 elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<int>>> |}>

[<Fact>]
let ``option array field with nullable int64 elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<int64>>> |}>

[<Fact>]
let ``option array field with nullable uint8 elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<uint8>>> |}>

[<Fact>]
let ``option array field with nullable uint16 elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<uint16>>> |}>

[<Fact>]
let ``option array field with nullable uint32 elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<uint>>> |}>

[<Fact>]
let ``option array field with nullable uint64 elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<uint64>>> |}>

[<Fact>]
let ``option array field with nullable float32 elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<float32>>> |}>

[<Fact>]
let ``option array field with nullable float64 elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<float>>> |}>

[<Fact>]
let ``option array field with nullable decimal elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<decimal>>> |}>

[<Fact>]
let ``option array field with nullable date time elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<DateTime>>> |}>

[<Fact>]
let ``option array field with nullable date time offset elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<DateTimeOffset>>> |}>

[<Fact>]
let ``option array field with nullable guid elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<Guid>>> |}>

[<Fact>]
let ``option array field with nullable record elements`` () =
    testRoundtrip<{|
        Field1: option<array<Nullable<struct {|
            Field2: int |}>>> |}>

[<Fact>]
let ``option array field with option bool elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<bool>>> |}>

[<Fact>]
let ``option array field with option int8 elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<int8>>> |}>

[<Fact>]
let ``option array field with option int16 elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<int16>>> |}>

[<Fact>]
let ``option array field with option int32 elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<int>>> |}>

[<Fact>]
let ``option array field with option int64 elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<int64>>> |}>

[<Fact>]
let ``option array field with option uint8 elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<uint8>>> |}>

[<Fact>]
let ``option array field with option uint16 elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<uint16>>> |}>

[<Fact>]
let ``option array field with option uint32 elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<uint>>> |}>

[<Fact>]
let ``option array field with option uint64 elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<uint64>>> |}>

[<Fact>]
let ``option array field with option float32 elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<float32>>> |}>

[<Fact>]
let ``option array field with option float64 elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<float>>> |}>

[<Fact>]
let ``option array field with option decimal elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<decimal>>> |}>

[<Fact>]
let ``option array field with option date time elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<DateTime>>> |}>

[<Fact>]
let ``option array field with option date time offset elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<DateTimeOffset>>> |}>

[<Fact>]
let ``option array field with option string elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<string>>> |}>

[<Fact>]
let ``option array field with option byte array elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<byte[]>>> |}>

[<Fact>]
let ``option array field with option guid elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<Guid>>> |}>

[<Fact>]
let ``option array field with option array elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<array<int>>>> |}>

[<Fact>]
let ``option array field with option resize array elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<ResizeArray<int>>>> |}>

[<Fact>]
let ``option array field with option list elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<list<int>>>> |}>

[<Fact>]
let ``option array field with option record elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<{|
            Field2: int |}>>> |}>

[<Fact>]
let ``option array field with option simple union elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<SimpleUnion>>> |}>

[<Fact>]
let ``option array field with option complex union elements`` () =
    testRoundtrip<{|
        Field1: option<array<option<ComplexUnion>>> |}>

[<Fact>]
let ``option resize array field with bool elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<bool>> |}>

[<Fact>]
let ``option resize array field with int8 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<int8>> |}>

[<Fact>]
let ``option resize array field with int16 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<int16>> |}>

[<Fact>]
let ``option resize array field with int32 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<int>> |}>

[<Fact>]
let ``option resize array field with int64 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<int64>> |}>

[<Fact>]
let ``option resize array field with uint8 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<uint8>> |}>

[<Fact>]
let ``option resize array field with uint16 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<uint16>> |}>

[<Fact>]
let ``option resize array field with uint32 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<uint>> |}>

[<Fact>]
let ``option resize array field with uint64 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<uint64>> |}>

[<Fact>]
let ``option resize array field with float32 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<float32>> |}>

[<Fact>]
let ``option resize array field with float64 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<float>> |}>

[<Fact>]
let ``option resize array field with decimal elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<decimal>> |}>

[<Fact>]
let ``option resize array field with date time elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<DateTime>> |}>

[<Fact>]
let ``option resize array field with date time offset elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<DateTimeOffset>> |}>

[<Fact>]
let ``option resize array field with string elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<string>> |}>

[<Fact>]
let ``option resize array field with byte array elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<byte[]>> |}>

[<Fact>]
let ``option resize array field with guid elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Guid>> |}>

[<Fact>]
let ``option resize array field with array elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<array<int>>> |}>

[<Fact>]
let ``option resize array field with resize array elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<ResizeArray<int>>> |}>

[<Fact>]
let ``option resize array field with list elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<list<int>>> |}>

[<Fact>]
let ``option resize array field with record elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<{|
            Field2: int |}>> |}>

[<Fact>]
let ``option resize array field with simple union elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<SimpleUnion>> |}>

[<Fact>]
let ``option resize array field with complex union elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<ComplexUnion>> |}>

[<Fact>]
let ``option resize array field with nullable bool elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<bool>>> |}>

[<Fact>]
let ``option resize array field with nullable int8 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<int8>>> |}>

[<Fact>]
let ``option resize array field with nullable int16 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<int16>>> |}>

[<Fact>]
let ``option resize array field with nullable int32 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<int>>> |}>

[<Fact>]
let ``option resize array field with nullable int64 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<int64>>> |}>

[<Fact>]
let ``option resize array field with nullable uint8 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<uint8>>> |}>

[<Fact>]
let ``option resize array field with nullable uint16 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<uint16>>> |}>

[<Fact>]
let ``option resize array field with nullable uint32 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<uint>>> |}>

[<Fact>]
let ``option resize array field with nullable uint64 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<uint64>>> |}>

[<Fact>]
let ``option resize array field with nullable float32 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<float32>>> |}>

[<Fact>]
let ``option resize array field with nullable float64 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<float>>> |}>

[<Fact>]
let ``option resize array field with nullable decimal elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<decimal>>> |}>

[<Fact>]
let ``option resize array field with nullable date time elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<DateTime>>> |}>

[<Fact>]
let ``option resize array field with nullable date time offset elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<DateTimeOffset>>> |}>

[<Fact>]
let ``option resize array field with nullable guid elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<Guid>>> |}>

[<Fact>]
let ``option resize array field with nullable record elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<Nullable<struct {|
            Field2: int |}>>> |}>

[<Fact>]
let ``option resize array field with option bool elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<bool>>> |}>

[<Fact>]
let ``option resize array field with option int8 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<int8>>> |}>

[<Fact>]
let ``option resize array field with option int16 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<int16>>> |}>

[<Fact>]
let ``option resize array field with option int32 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<int>>> |}>

[<Fact>]
let ``option resize array field with option int64 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<int64>>> |}>

[<Fact>]
let ``option resize array field with option uint8 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<uint8>>> |}>

[<Fact>]
let ``option resize array field with option uint16 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<uint16>>> |}>

[<Fact>]
let ``option resize array field with option uint32 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<uint>>> |}>

[<Fact>]
let ``option resize array field with option uint64 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<uint64>>> |}>

[<Fact>]
let ``option resize array field with option float32 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<float32>>> |}>

[<Fact>]
let ``option resize array field with option float64 elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<float>>> |}>

[<Fact>]
let ``option resize array field with option decimal elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<decimal>>> |}>

[<Fact>]
let ``option resize array field with option date time elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<DateTime>>> |}>

[<Fact>]
let ``option resize array field with option date time offset elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<DateTimeOffset>>> |}>

[<Fact>]
let ``option resize array field with option string elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<string>>> |}>

[<Fact>]
let ``option resize array field with option byte array elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<byte[]>>> |}>

[<Fact>]
let ``option resize array field with option guid elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<Guid>>> |}>

[<Fact>]
let ``option resize array field with option array elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<array<int>>>> |}>

[<Fact>]
let ``option resize array field with option resize array elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<ResizeArray<int>>>> |}>

[<Fact>]
let ``option resize array field with option list elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<list<int>>>> |}>

[<Fact>]
let ``option resize array field with option record elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<{|
            Field2: int |}>>> |}>

[<Fact>]
let ``option resize array field with option simple union elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<SimpleUnion>>> |}>

[<Fact>]
let ``option resize array field with option complex union elements`` () =
    testRoundtrip<{|
        Field1: option<ResizeArray<option<ComplexUnion>>> |}>

[<Fact>]
let ``option list field with bool elements`` () =
    testRoundtrip<{|
        Field1: option<list<bool>> |}>

[<Fact>]
let ``option list field with int8 elements`` () =
    testRoundtrip<{|
        Field1: option<list<int8>> |}>

[<Fact>]
let ``option list field with int16 elements`` () =
    testRoundtrip<{|
        Field1: option<list<int16>> |}>

[<Fact>]
let ``option list field with int32 elements`` () =
    testRoundtrip<{|
        Field1: option<list<int>> |}>

[<Fact>]
let ``option list field with int64 elements`` () =
    testRoundtrip<{|
        Field1: option<list<int64>> |}>

[<Fact>]
let ``option list field with uint8 elements`` () =
    testRoundtrip<{|
        Field1: option<list<uint8>> |}>

[<Fact>]
let ``option list field with uint16 elements`` () =
    testRoundtrip<{|
        Field1: option<list<uint16>> |}>

[<Fact>]
let ``option list field with uint32 elements`` () =
    testRoundtrip<{|
        Field1: option<list<uint>> |}>

[<Fact>]
let ``option list field with uint64 elements`` () =
    testRoundtrip<{|
        Field1: option<list<uint64>> |}>

[<Fact>]
let ``option list field with float32 elements`` () =
    testRoundtrip<{|
        Field1: option<list<float32>> |}>

[<Fact>]
let ``option list field with float64 elements`` () =
    testRoundtrip<{|
        Field1: option<list<float>> |}>

[<Fact>]
let ``option list field with decimal elements`` () =
    testRoundtrip<{|
        Field1: option<list<decimal>> |}>

[<Fact>]
let ``option list field with date time elements`` () =
    testRoundtrip<{|
        Field1: option<list<DateTime>> |}>

[<Fact>]
let ``option list field with date time offset elements`` () =
    testRoundtrip<{|
        Field1: option<list<DateTimeOffset>> |}>

[<Fact>]
let ``option list field with string elements`` () =
    testRoundtrip<{|
        Field1: option<list<string>> |}>

[<Fact>]
let ``option list field with byte array elements`` () =
    testRoundtrip<{|
        Field1: option<list<byte[]>> |}>

[<Fact>]
let ``option list field with guid elements`` () =
    testRoundtrip<{|
        Field1: option<list<Guid>> |}>

[<Fact>]
let ``option list field with array elements`` () =
    testRoundtrip<{|
        Field1: option<list<array<int>>> |}>

[<Fact>]
let ``option list field with resize array elements`` () =
    testRoundtrip<{|
        Field1: option<list<ResizeArray<int>>> |}>

[<Fact>]
let ``option list field with list elements`` () =
    testRoundtrip<{|
        Field1: option<list<list<int>>> |}>

[<Fact>]
let ``option list field with record elements`` () =
    testRoundtrip<{|
        Field1: option<list<{|
            Field2: int |}>> |}>

[<Fact>]
let ``option list field with simple union elements`` () =
    testRoundtrip<{|
        Field1: option<list<SimpleUnion>> |}>

[<Fact>]
let ``option list field with complex union elements`` () =
    testRoundtrip<{|
        Field1: option<list<ComplexUnion>> |}>

[<Fact>]
let ``option list field with nullable bool elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<bool>>> |}>

[<Fact>]
let ``option list field with nullable int8 elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<int8>>> |}>

[<Fact>]
let ``option list field with nullable int16 elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<int16>>> |}>

[<Fact>]
let ``option list field with nullable int32 elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<int>>> |}>

[<Fact>]
let ``option list field with nullable int64 elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<int64>>> |}>

[<Fact>]
let ``option list field with nullable uint8 elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<uint8>>> |}>

[<Fact>]
let ``option list field with nullable uint16 elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<uint16>>> |}>

[<Fact>]
let ``option list field with nullable uint32 elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<uint>>> |}>

[<Fact>]
let ``option list field with nullable uint64 elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<uint64>>> |}>

[<Fact>]
let ``option list field with nullable float32 elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<float32>>> |}>

[<Fact>]
let ``option list field with nullable float64 elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<float>>> |}>

[<Fact>]
let ``option list field with nullable decimal elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<decimal>>> |}>

[<Fact>]
let ``option list field with nullable date time elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<DateTime>>> |}>

[<Fact>]
let ``option list field with nullable date time offset elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<DateTimeOffset>>> |}>

[<Fact>]
let ``option list field with nullable guid elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<Guid>>> |}>

[<Fact>]
let ``option list field with nullable record elements`` () =
    testRoundtrip<{|
        Field1: option<list<Nullable<struct {|
            Field2: int |}>>> |}>

[<Fact>]
let ``option list field with option bool elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<bool>>> |}>

[<Fact>]
let ``option list field with option int8 elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<int8>>> |}>

[<Fact>]
let ``option list field with option int16 elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<int16>>> |}>

[<Fact>]
let ``option list field with option int32 elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<int>>> |}>

[<Fact>]
let ``option list field with option int64 elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<int64>>> |}>

[<Fact>]
let ``option list field with option uint8 elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<uint8>>> |}>

[<Fact>]
let ``option list field with option uint16 elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<uint16>>> |}>

[<Fact>]
let ``option list field with option uint32 elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<uint>>> |}>

[<Fact>]
let ``option list field with option uint64 elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<uint64>>> |}>

[<Fact>]
let ``option list field with option float32 elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<float32>>> |}>

[<Fact>]
let ``option list field with option float64 elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<float>>> |}>

[<Fact>]
let ``option list field with option decimal elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<decimal>>> |}>

[<Fact>]
let ``option list field with option date time elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<DateTime>>> |}>

[<Fact>]
let ``option list field with option date time offset elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<DateTimeOffset>>> |}>

[<Fact>]
let ``option list field with option string elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<string>>> |}>

[<Fact>]
let ``option list field with option byte array elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<byte[]>>> |}>

[<Fact>]
let ``option list field with option guid elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<Guid>>> |}>

[<Fact>]
let ``option list field with option array elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<array<int>>>> |}>

[<Fact>]
let ``option list field with option resize array elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<ResizeArray<int>>>> |}>

[<Fact>]
let ``option list field with option list elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<list<int>>>> |}>

[<Fact>]
let ``option list field with option record elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<{|
            Field2: int |}>>> |}>

[<Fact>]
let ``option list field with option simple union elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<SimpleUnion>>> |}>

[<Fact>]
let ``option list field with option complex union elements`` () =
    testRoundtrip<{|
        Field1: option<list<option<ComplexUnion>>> |}>

[<Fact>]
let ``option record field with bool field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: bool |}> |}>

[<Fact>]
let ``option record field with int8 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: int8 |}> |}>

[<Fact>]
let ``option record field with int16 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: int16 |}> |}>

[<Fact>]
let ``option record field with int32 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: int |}> |}>

[<Fact>]
let ``option record field with int64 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: int64 |}> |}>

[<Fact>]
let ``option record field with uint8 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: uint8 |}> |}>

[<Fact>]
let ``option record field with uint16 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: uint16 |}> |}>

[<Fact>]
let ``option record field with uint32 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: uint |}> |}>

[<Fact>]
let ``option record field with uint64 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: uint64 |}> |}>

[<Fact>]
let ``option record field with float32 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: float32 |}> |}>

[<Fact>]
let ``option record field with float64 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: float |}> |}>

[<Fact>]
let ``option record field with decimal field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: decimal |}> |}>

[<Fact>]
let ``option record field with date time field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: DateTime |}> |}>

[<Fact>]
let ``option record field with date time offset field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: DateTimeOffset |}> |}>

[<Fact>]
let ``option record field with string field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: string |}> |}>

[<Fact>]
let ``option record field with byte array field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: byte[] |}> |}>

[<Fact>]
let ``option record field with guid field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Guid |}> |}>

[<Fact>]
let ``option record field with array field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: array<int> |}> |}>

[<Fact>]
let ``option record field with resize array field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: ResizeArray<int> |}> |}>

[<Fact>]
let ``option record field with list field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: list<int> |}> |}>

[<Fact>]
let ``option record field with record field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: {|
                Field3: int |} |}> |}>

[<Fact>]
let ``option record field with simple union field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: SimpleUnion |}> |}>

[<Fact>]
let ``option record field with complex union field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: ComplexUnion |}> |}>

[<Fact>]
let ``option record field with nullable bool field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<bool> |}> |}>

[<Fact>]
let ``option record field with nullable int8 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<int8> |}> |}>

[<Fact>]
let ``option record field with nullable int16 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<int16> |}> |}>

[<Fact>]
let ``option record field with nullable int32 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<int> |}> |}>

[<Fact>]
let ``option record field with nullable int64 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<int64> |}> |}>

[<Fact>]
let ``option record field with nullable uint8 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<uint8> |}> |}>

[<Fact>]
let ``option record field with nullable uint16 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<uint16> |}> |}>

[<Fact>]
let ``option record field with nullable uint32 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<uint> |}> |}>

[<Fact>]
let ``option record field with nullable uint64 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<uint64> |}> |}>

[<Fact>]
let ``option record field with nullable float32 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<float32> |}> |}>

[<Fact>]
let ``option record field with nullable float64 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<float> |}> |}>

[<Fact>]
let ``option record field with nullable decimal field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<decimal> |}> |}>

[<Fact>]
let ``option record field with nullable date time field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<DateTime> |}> |}>

[<Fact>]
let ``option record field with nullable date time offset field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<DateTimeOffset> |}> |}>

[<Fact>]
let ``option record field with nullable guid field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<Guid> |}> |}>

[<Fact>]
let ``option record field with nullable record field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<struct {|
                Field3: int |}> |}> |}>

[<Fact>]
let ``option record field with option bool field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<bool> |}> |}>

[<Fact>]
let ``option record field with option int8 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<int8> |}> |}>

[<Fact>]
let ``option record field with option int16 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<int16> |}> |}>

[<Fact>]
let ``option record field with option int32 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<int> |}> |}>

[<Fact>]
let ``option record field with option int64 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<int64> |}> |}>

[<Fact>]
let ``option record field with option uint8 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<uint8> |}> |}>

[<Fact>]
let ``option record field with option uint16 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<uint16> |}> |}>

[<Fact>]
let ``option record field with option uint32 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<uint> |}> |}>

[<Fact>]
let ``option record field with option uint64 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<uint64> |}> |}>

[<Fact>]
let ``option record field with option float32 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<float32> |}> |}>

[<Fact>]
let ``option record field with option float64 field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<float> |}> |}>

[<Fact>]
let ``option record field with option decimal field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<decimal> |}> |}>

[<Fact>]
let ``option record field with option date time field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<DateTime> |}> |}>

[<Fact>]
let ``option record field with option date time offset field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<DateTimeOffset> |}> |}>

[<Fact>]
let ``option record field with option string field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<string> |}> |}>

[<Fact>]
let ``option record field with option byte array field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<byte[]> |}> |}>

[<Fact>]
let ``option record field with option guid field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<Guid> |}> |}>

[<Fact>]
let ``option record field with option array field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<array<int>> |}> |}>

[<Fact>]
let ``option record field with option resize array field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<ResizeArray<int>> |}> |}>

[<Fact>]
let ``option record field with option list field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<list<int>> |}> |}>

[<Fact>]
let ``option record field with option record field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<{|
                Field3: int |}> |}> |}>

[<Fact>]
let ``option record field with option simple union field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<SimpleUnion> |}> |}>

[<Fact>]
let ``option record field with option complex union field`` () =
    testRoundtrip<{|
        Field1: option<{|
            Field2: option<ComplexUnion> |}> |}>

[<Fact>]
let ``option simple union field`` () =
    testRoundtrip<{|
        Field1: option<SimpleUnion> |}>

[<Fact>]
let ``option complex union field`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnion> |}>

[<Fact>]
let ``option complex union field with bool case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithBoolCase> |}>

[<Fact>]
let ``option complex union field with int8 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithInt8Case> |}>

[<Fact>]
let ``option complex union field with int16 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithInt16Case> |}>

[<Fact>]
let ``option complex union field with int32 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithInt32Case> |}>

[<Fact>]
let ``option complex union field with int64 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithInt64Case> |}>

[<Fact>]
let ``option complex union field with uint8 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithUInt8Case> |}>

[<Fact>]
let ``option complex union field with uint16 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithUInt16Case> |}>

[<Fact>]
let ``option complex union field with uint32 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithUInt32Case> |}>

[<Fact>]
let ``option complex union field with uint64 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithUInt64Case> |}>

[<Fact>]
let ``option complex union field with float32 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithFloat32Case> |}>

[<Fact>]
let ``option complex union field with float64 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithFloat64Case> |}>

[<Fact>]
let ``option complex union field with decimal case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithDecimalCase> |}>

[<Fact>]
let ``option complex union field with date time case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithDateTimeCase> |}>

[<Fact>]
let ``option complex union field with date time offset case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithDateTimeOffsetCase> |}>

[<Fact>]
let ``option complex union field with string case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithStringCase> |}>

[<Fact>]
let ``option complex union field with byte array case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithByteArrayCase> |}>

[<Fact>]
let ``option complex union field with guid case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithGuidCase> |}>

[<Fact>]
let ``option complex union field with array case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithArrayCase> |}>

[<Fact>]
let ``option complex union field with resize array case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithGenericListCase> |}>

[<Fact>]
let ``option complex union field with list case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithFSharpListCase> |}>

[<Fact>]
let ``option complex union field with record case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithFSharpRecordCase> |}>

[<Fact>]
let ``option complex union field with simple union case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithSimpleUnionCase> |}>

[<Fact>]
let ``option complex union field with complex union case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithComplexUnionCase> |}>

[<Fact>]
let ``option complex union field with nullable bool case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableBoolCase> |}>

[<Fact>]
let ``option complex union field with nullable int8 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableInt8Case> |}>

[<Fact>]
let ``option complex union field with nullable int16 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableInt16Case> |}>

[<Fact>]
let ``option complex union field with nullable int32 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableInt32Case> |}>

[<Fact>]
let ``option complex union field with nullable int64 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableInt64Case> |}>

[<Fact>]
let ``option complex union field with nullable uint8 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableUInt8Case> |}>

[<Fact>]
let ``option complex union field with nullable uint16 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableUInt16Case> |}>

[<Fact>]
let ``option complex union field with nullable uint32 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableUInt32Case> |}>

[<Fact>]
let ``option complex union field with nullable uint64 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableUInt64Case> |}>

[<Fact>]
let ``option complex union field with nullable float32 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableFloat32Case> |}>

[<Fact>]
let ``option complex union field with nullable float64 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableFloat64Case> |}>

[<Fact>]
let ``option complex union field with nullable decimal case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableDecimalCase> |}>

[<Fact>]
let ``option complex union field with nullable date time case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableDateTimeCase> |}>

[<Fact>]
let ``option complex union field with nullable date time offset case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableDateTimeOffsetCase> |}>

[<Fact>]
let ``option complex union field with nullable guid case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableGuidCase> |}>

[<Fact>]
let ``option complex union field with nullable record case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithNullableFSharpRecordCase> |}>

[<Fact>]
let ``option complex union field with option bool case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionBoolCase> |}>

[<Fact>]
let ``option complex union field with option int8 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionInt8Case> |}>

[<Fact>]
let ``option complex union field with option int16 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionInt16Case> |}>

[<Fact>]
let ``option complex union field with option int32 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionInt32Case> |}>

[<Fact>]
let ``option complex union field with option int64 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionInt64Case> |}>

[<Fact>]
let ``option complex union field with option uint8 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionUInt8Case> |}>

[<Fact>]
let ``option complex union field with option uint16 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionUInt16Case> |}>

[<Fact>]
let ``option complex union field with option uint32 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionUInt32Case> |}>

[<Fact>]
let ``option complex union field with option uint64 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionUInt64Case> |}>

[<Fact>]
let ``option complex union field with option float32 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionFloat32Case> |}>

[<Fact>]
let ``option complex union field with option float64 case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionFloat64Case> |}>

[<Fact>]
let ``option complex union field with option decimal case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionDecimalCase> |}>

[<Fact>]
let ``option complex union field with option date time case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionDateTimeCase> |}>

[<Fact>]
let ``option complex union field with option date time offset case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionDateTimeOffsetCase> |}>

[<Fact>]
let ``option complex union field with option string case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionStringCase> |}>

[<Fact>]
let ``option complex union field with option byte array case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionByteArrayCase> |}>

[<Fact>]
let ``option complex union field with option guid case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionGuidCase> |}>

[<Fact>]
let ``option complex union field with option array case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionArrayCase> |}>

[<Fact>]
let ``option complex union field with option resize array case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionGenericListCase> |}>

[<Fact>]
let ``option complex union field with option list case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionFSharpListCase> |}>

[<Fact>]
let ``option complex union field with option record case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionFSharpRecordCase> |}>

[<Fact>]
let ``option complex union field with option simple union case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionSimpleUnionCase> |}>

[<Fact>]
let ``option complex union field with option complex union case`` () =
    testRoundtrip<{|
        Field1: option<ComplexUnionWithOptionComplexUnionCase> |}>
