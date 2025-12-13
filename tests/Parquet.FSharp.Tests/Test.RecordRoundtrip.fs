module Parquet.FSharp.Tests.RecordRoundtrip

open FluentAssertions
open FluentAssertions.Equivalency
open FsCheck.Xunit
open Parquet.FSharp
open System
open System.IO

let testRecordRoundtrip<'Record when 'Record : equality> (records: 'Record[]) =
    use stream = new MemoryStream()
    ParquetSerializer.Serialize<'Record>(records, stream)
    stream.Seek(0, SeekOrigin.Begin) |> ignore
    let roundtrippedRecords = ParquetSerializer.Deserialize<'Record>(stream)
    let configureEquivalencyOptions (options: EquivalencyOptions<'Record>) =
        options.AllowingInfiniteRecursion()
            .WithStrictOrdering()
            .WithStrictTyping()
    roundtrippedRecords.Should()
        .BeEquivalentTo(records, configureEquivalencyOptions)
    |> ignore

[<Property>]
let ``bool field`` records =
    testRecordRoundtrip<{|
        Field1: bool |}>
        records

[<Property>]
let ``int8 field`` records =
    testRecordRoundtrip<{|
        Field1: int8 |}>
        records

[<Property>]
let ``int16 field`` records =
    testRecordRoundtrip<{|
        Field1: int16 |}>
        records

[<Property>]
let ``int32 field`` records =
    testRecordRoundtrip<{|
        Field1: int |}>
        records

[<Property>]
let ``int64 field`` records =
    testRecordRoundtrip<{|
        Field1: int64 |}>
        records

[<Property>]
let ``uint8 field`` records =
    testRecordRoundtrip<{|
        Field1: uint8 |}>
        records

[<Property>]
let ``uint16 field`` records =
    testRecordRoundtrip<{|
        Field1: uint16 |}>
        records

[<Property>]
let ``uint32 field`` records =
    testRecordRoundtrip<{|
        Field1: uint |}>
        records

[<Property>]
let ``uint64 field`` records =
    testRecordRoundtrip<{|
        Field1: uint64 |}>
        records

[<Property>]
let ``float32 field`` records =
    testRecordRoundtrip<{|
        Field1: float32 |}>
        records

[<Property>]
let ``float64 field`` records =
    testRecordRoundtrip<{|
        Field1: float |}>
        records

[<Property>]
let ``decimal field`` records =
    testRecordRoundtrip<{|
        Field1: decimal |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``date time field`` records =
    testRecordRoundtrip<{|
        Field1: DateTime |}>
        records

[<Property>]
let ``date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: DateTimeOffset |}>
        records

[<Property>]
let ``string field`` records =
    testRecordRoundtrip<{|
        Field1: string |}>
        records

[<Property>]
let ``guid field`` records =
    testRecordRoundtrip<{|
        Field1: Guid |}>
        records

[<Property>]
let ``array field with bool elements`` records =
    testRecordRoundtrip<{|
        Field1: array<bool> |}>
        records

[<Property>]
let ``array field with int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<int8> |}>
        records

[<Property>]
let ``array field with int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<int16> |}>
        records

[<Property>]
let ``array field with int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<int> |}>
        records

[<Property>]
let ``array field with int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<int64> |}>
        records

[<Property>]
let ``array field with uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<uint8> |}>
        records

[<Property>]
let ``array field with uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<uint16> |}>
        records

[<Property>]
let ``array field with uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<uint> |}>
        records

[<Property>]
let ``array field with uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<uint64> |}>
        records

[<Property>]
let ``array field with float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<float32> |}>
        records

[<Property>]
let ``array field with float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<float> |}>
        records

[<Property>]
let ``array field with decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: array<decimal> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``array field with date time elements`` records =
    testRecordRoundtrip<{|
        Field1: array<DateTime> |}>
        records

[<Property>]
let ``array field with date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: array<DateTimeOffset> |}>
        records

[<Property>]
let ``array field with string elements`` records =
    testRecordRoundtrip<{|
        Field1: array<string> |}>
        records

[<Property>]
let ``array field with guid elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Guid> |}>
        records

[<Property>]
let ``array field with array elements`` records =
    testRecordRoundtrip<{|
        Field1: array<array<int>> |}>
        records

[<Property>]
let ``array field with generic list elements`` records =
    testRecordRoundtrip<{|
        Field1: array<ResizeArray<int>> |}>
        records

[<Property>]
let ``array field with fsharp list elements`` records =
    testRecordRoundtrip<{|
        Field1: array<list<int>> |}>
        records

[<Property>]
let ``array field with record elements`` records =
    testRecordRoundtrip<{|
        Field1: array<{|
            Field2: int |}> |}>
        records

[<Property>]
let ``array field with nullable bool elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<bool>> |}>
        records

[<Property>]
let ``array field with nullable int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<int8>> |}>
        records

[<Property>]
let ``array field with nullable int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<int16>> |}>
        records

[<Property>]
let ``array field with nullable int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<int>> |}>
        records

[<Property>]
let ``array field with nullable int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<int64>> |}>
        records

[<Property>]
let ``array field with nullable uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<uint8>> |}>
        records

[<Property>]
let ``array field with nullable uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<uint16>> |}>
        records

[<Property>]
let ``array field with nullable uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<uint>> |}>
        records

[<Property>]
let ``array field with nullable uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<uint64>> |}>
        records

[<Property>]
let ``array field with nullable float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<float32>> |}>
        records

[<Property>]
let ``array field with nullable float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<float>> |}>
        records

[<Property>]
let ``array field with nullable decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``array field with nullable date time elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<DateTime>> |}>
        records

[<Property>]
let ``array field with nullable date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<DateTimeOffset>> |}>
        records

[<Property>]
let ``array field with nullable guid elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<Guid>> |}>
        records

[<Property>]
let ``array field with nullable record elements`` records =
    testRecordRoundtrip<{|
        Field1: array<Nullable<struct {|
            Field2: int |}>> |}>
        records

[<Property>]
let ``array field with option bool elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<bool>> |}>
        records

[<Property>]
let ``array field with option int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<int8>> |}>
        records

[<Property>]
let ``array field with option int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<int16>> |}>
        records

[<Property>]
let ``array field with option int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<int>> |}>
        records

[<Property>]
let ``array field with option int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<int64>> |}>
        records

[<Property>]
let ``array field with option uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<uint8>> |}>
        records

[<Property>]
let ``array field with option uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<uint16>> |}>
        records

[<Property>]
let ``array field with option uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<uint>> |}>
        records

[<Property>]
let ``array field with option uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<uint64>> |}>
        records

[<Property>]
let ``array field with option float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<float32>> |}>
        records

[<Property>]
let ``array field with option float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<float>> |}>
        records

[<Property>]
let ``array field with option decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``array field with option date time elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<DateTime>> |}>
        records

[<Property>]
let ``array field with option date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<DateTimeOffset>> |}>
        records

[<Property>]
let ``array field with option string elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<string>> |}>
        records

[<Property>]
let ``array field with option guid elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<Guid>> |}>
        records

[<Property>]
let ``array field with option array elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<array<int>>> |}>
        records

[<Property>]
let ``array field with option generic list elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<ResizeArray<int>>> |}>
        records

[<Property>]
let ``array field with option fsharp list elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<list<int>>> |}>
        records

[<Property>]
let ``array field with option record elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<{|
            Field2: int |}>> |}>
        records

[<Property>]
let ``array field with option nullable elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<Nullable<int>>> |}>
        records

[<Property>]
let ``array field with option option elements`` records =
    testRecordRoundtrip<{|
        Field1: array<option<option<int>>> |}>
        records

[<Property>]
let ``generic list field with bool elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<bool> |}>
        records

[<Property>]
let ``generic list field with int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<int8> |}>
        records

[<Property>]
let ``generic list field with int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<int16> |}>
        records

[<Property>]
let ``generic list field with int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<int> |}>
        records

[<Property>]
let ``generic list field with int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<int64> |}>
        records

[<Property>]
let ``generic list field with uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<uint8> |}>
        records

[<Property>]
let ``generic list field with uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<uint16> |}>
        records

[<Property>]
let ``generic list field with uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<uint> |}>
        records

[<Property>]
let ``generic list field with uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<uint64> |}>
        records

[<Property>]
let ``generic list field with float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<float32> |}>
        records

[<Property>]
let ``generic list field with float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<float> |}>
        records

[<Property>]
let ``generic list field with decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<decimal> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``generic list field with date time elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<DateTime> |}>
        records

[<Property>]
let ``generic list field with date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<DateTimeOffset> |}>
        records

[<Property>]
let ``generic list field with string elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<string> |}>
        records

[<Property>]
let ``generic list field with guid elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Guid> |}>
        records

[<Property>]
let ``generic list field with array elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<array<int>> |}>
        records

[<Property>]
let ``generic list field with generic list elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<ResizeArray<int>> |}>
        records

[<Property>]
let ``generic list field with fsharp list elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<list<int>> |}>
        records

[<Property>]
let ``generic list field with record elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<{|
            Field2: int |}> |}>
        records

[<Property>]
let ``generic list field with nullable bool elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<bool>> |}>
        records

[<Property>]
let ``generic list field with nullable int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<int8>> |}>
        records

[<Property>]
let ``generic list field with nullable int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<int16>> |}>
        records

[<Property>]
let ``generic list field with nullable int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<int>> |}>
        records

[<Property>]
let ``generic list field with nullable int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<int64>> |}>
        records

[<Property>]
let ``generic list field with nullable uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<uint8>> |}>
        records

[<Property>]
let ``generic list field with nullable uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<uint16>> |}>
        records

[<Property>]
let ``generic list field with nullable uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<uint>> |}>
        records

[<Property>]
let ``generic list field with nullable uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<uint64>> |}>
        records

[<Property>]
let ``generic list field with nullable float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<float32>> |}>
        records

[<Property>]
let ``generic list field with nullable float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<float>> |}>
        records

[<Property>]
let ``generic list field with nullable decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``generic list field with nullable date time elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<DateTime>> |}>
        records

[<Property>]
let ``generic list field with nullable date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<DateTimeOffset>> |}>
        records

[<Property>]
let ``generic list field with nullable guid elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<Guid>> |}>
        records

[<Property>]
let ``generic list field with nullable record elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<Nullable<struct {|
            Field2: int |}>> |}>
        records

[<Property>]
let ``generic list field with option bool elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<bool>> |}>
        records

[<Property>]
let ``generic list field with option int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<int8>> |}>
        records

[<Property>]
let ``generic list field with option int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<int16>> |}>
        records

[<Property>]
let ``generic list field with option int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<int>> |}>
        records

[<Property>]
let ``generic list field with option int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<int64>> |}>
        records

[<Property>]
let ``generic list field with option uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<uint8>> |}>
        records

[<Property>]
let ``generic list field with option uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<uint16>> |}>
        records

[<Property>]
let ``generic list field with option uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<uint>> |}>
        records

[<Property>]
let ``generic list field with option uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<uint64>> |}>
        records

[<Property>]
let ``generic list field with option float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<float32>> |}>
        records

[<Property>]
let ``generic list field with option float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<float>> |}>
        records

[<Property>]
let ``generic list field with option decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``generic list field with option date time elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<DateTime>> |}>
        records

[<Property>]
let ``generic list field with option date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<DateTimeOffset>> |}>
        records

[<Property>]
let ``generic list field with option string elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<string>> |}>
        records

[<Property>]
let ``generic list field with option guid elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<Guid>> |}>
        records

[<Property>]
let ``generic list field with option array elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<array<int>>> |}>
        records

[<Property>]
let ``generic list field with option generic list elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<ResizeArray<int>>> |}>
        records

[<Property>]
let ``generic list field with option fsharp list elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<list<int>>> |}>
        records

[<Property>]
let ``generic list field with option record elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<{|
            Field2: int |}>> |}>
        records

[<Property>]
let ``generic list field with option nullable elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<Nullable<int>>> |}>
        records

[<Property>]
let ``generic list field with option option elements`` records =
    testRecordRoundtrip<{|
        Field1: ResizeArray<option<option<int>>> |}>
        records

[<Property>]
let ``fsharp list field with bool elements`` records =
    testRecordRoundtrip<{|
        Field1: list<bool> |}>
        records

[<Property>]
let ``fsharp list field with int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<int8> |}>
        records

[<Property>]
let ``fsharp list field with int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<int16> |}>
        records

[<Property>]
let ``fsharp list field with int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<int> |}>
        records

[<Property>]
let ``fsharp list field with int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<int64> |}>
        records

[<Property>]
let ``fsharp list field with uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<uint8> |}>
        records

[<Property>]
let ``fsharp list field with uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<uint16> |}>
        records

[<Property>]
let ``fsharp list field with uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<uint> |}>
        records

[<Property>]
let ``fsharp list field with uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<uint64> |}>
        records

[<Property>]
let ``fsharp list field with float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<float32> |}>
        records

[<Property>]
let ``fsharp list field with float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<float> |}>
        records

[<Property>]
let ``fsharp list field with decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: list<decimal> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``fsharp list field with date time elements`` records =
    testRecordRoundtrip<{|
        Field1: list<DateTime> |}>
        records

[<Property>]
let ``fsharp list field with date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: list<DateTimeOffset> |}>
        records

[<Property>]
let ``fsharp list field with string elements`` records =
    testRecordRoundtrip<{|
        Field1: list<string> |}>
        records

[<Property>]
let ``fsharp list field with guid elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Guid> |}>
        records

[<Property>]
let ``fsharp list field with array elements`` records =
    testRecordRoundtrip<{|
        Field1: list<array<int>> |}>
        records

[<Property>]
let ``fsharp list field with generic list elements`` records =
    testRecordRoundtrip<{|
        Field1: list<ResizeArray<int>> |}>
        records

[<Property>]
let ``fsharp list field with fsharp list elements`` records =
    testRecordRoundtrip<{|
        Field1: list<list<int>> |}>
        records

[<Property>]
let ``fsharp list field with record elements`` records =
    testRecordRoundtrip<{|
        Field1: list<{|
            Field2: int |}> |}>
        records

[<Property>]
let ``fsharp list field with nullable bool elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<bool>> |}>
        records

[<Property>]
let ``fsharp list field with nullable int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<int8>> |}>
        records

[<Property>]
let ``fsharp list field with nullable int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<int16>> |}>
        records

[<Property>]
let ``fsharp list field with nullable int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<int>> |}>
        records

[<Property>]
let ``fsharp list field with nullable int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<int64>> |}>
        records

[<Property>]
let ``fsharp list field with nullable uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<uint8>> |}>
        records

[<Property>]
let ``fsharp list field with nullable uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<uint16>> |}>
        records

[<Property>]
let ``fsharp list field with nullable uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<uint>> |}>
        records

[<Property>]
let ``fsharp list field with nullable uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<uint64>> |}>
        records

[<Property>]
let ``fsharp list field with nullable float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<float32>> |}>
        records

[<Property>]
let ``fsharp list field with nullable float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<float>> |}>
        records

[<Property>]
let ``fsharp list field with nullable decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``fsharp list field with nullable date time elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<DateTime>> |}>
        records

[<Property>]
let ``fsharp list field with nullable date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<DateTimeOffset>> |}>
        records

[<Property>]
let ``fsharp list field with nullable guid elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<Guid>> |}>
        records

[<Property>]
let ``fsharp list field with nullable record elements`` records =
    testRecordRoundtrip<{|
        Field1: list<Nullable<struct {|
            Field2: int |}>> |}>
        records

[<Property>]
let ``fsharp list field with option bool elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<bool>> |}>
        records

[<Property>]
let ``fsharp list field with option int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<int8>> |}>
        records

[<Property>]
let ``fsharp list field with option int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<int16>> |}>
        records

[<Property>]
let ``fsharp list field with option int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<int>> |}>
        records

[<Property>]
let ``fsharp list field with option int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<int64>> |}>
        records

[<Property>]
let ``fsharp list field with option uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<uint8>> |}>
        records

[<Property>]
let ``fsharp list field with option uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<uint16>> |}>
        records

[<Property>]
let ``fsharp list field with option uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<uint>> |}>
        records

[<Property>]
let ``fsharp list field with option uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<uint64>> |}>
        records

[<Property>]
let ``fsharp list field with option float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<float32>> |}>
        records

[<Property>]
let ``fsharp list field with option float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<float>> |}>
        records

[<Property>]
let ``fsharp list field with option decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``fsharp list field with option date time elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<DateTime>> |}>
        records

[<Property>]
let ``fsharp list field with option date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<DateTimeOffset>> |}>
        records

[<Property>]
let ``fsharp list field with option string elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<string>> |}>
        records

[<Property>]
let ``fsharp list field with option guid elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<Guid>> |}>
        records

[<Property>]
let ``fsharp list field with option array elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<array<int>>> |}>
        records

[<Property>]
let ``fsharp list field with option generic list elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<ResizeArray<int>>> |}>
        records

[<Property>]
let ``fsharp list field with option fsharp list elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<list<int>>> |}>
        records

[<Property>]
let ``fsharp list field with option record elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<{|
            Field2: int |}>> |}>
        records

[<Property>]
let ``fsharp list field with option nullable elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<Nullable<int>>> |}>
        records

[<Property>]
let ``fsharp list field with option option elements`` records =
    testRecordRoundtrip<{|
        Field1: list<option<option<int>>> |}>
        records

[<Property>]
let ``record field with bool field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: bool |} |}>
        records

[<Property>]
let ``record field with int8 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: int8 |} |}>
        records

[<Property>]
let ``record field with int16 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: int16 |} |}>
        records

[<Property>]
let ``record field with int32 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: int |} |}>
        records

[<Property>]
let ``record field with int64 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: int64 |} |}>
        records

[<Property>]
let ``record field with uint8 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: uint8 |} |}>
        records

[<Property>]
let ``record field with uint16 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: uint16 |} |}>
        records

[<Property>]
let ``record field with uint32 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: uint |} |}>
        records

[<Property>]
let ``record field with uint64 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: uint64 |} |}>
        records

[<Property>]
let ``record field with float32 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: float32 |} |}>
        records

[<Property>]
let ``record field with float64 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: float |} |}>
        records

[<Property>]
let ``record field with decimal field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: decimal |} |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``record field with date time field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: DateTime |} |}>
        records

[<Property>]
let ``record field with date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: DateTimeOffset |} |}>
        records

[<Property>]
let ``record field with string field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: string |} |}>
        records

[<Property>]
let ``record field with guid field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Guid |} |}>
        records

[<Property>]
let ``record field with array field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: array<int> |} |}>
        records

[<Property>]
let ``record field with generic list field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: ResizeArray<int> |} |}>
        records

[<Property>]
let ``record field with fsharp list field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: list<int> |} |}>
        records

[<Property>]
let ``record field with record field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: {|
                Field3: int |} |} |}>
        records

[<Property>]
let ``record field with nullable bool field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<bool> |} |}>
        records

[<Property>]
let ``record field with nullable int8 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<int8> |} |}>
        records

[<Property>]
let ``record field with nullable int16 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<int16> |} |}>
        records

[<Property>]
let ``record field with nullable int32 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<int> |} |}>
        records

[<Property>]
let ``record field with nullable int64 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<int64> |} |}>
        records

[<Property>]
let ``record field with nullable uint8 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<uint8> |} |}>
        records

[<Property>]
let ``record field with nullable uint16 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<uint16> |} |}>
        records

[<Property>]
let ``record field with nullable uint32 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<uint> |} |}>
        records

[<Property>]
let ``record field with nullable uint64 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<uint64> |} |}>
        records

[<Property>]
let ``record field with nullable float32 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<float32> |} |}>
        records

[<Property>]
let ``record field with nullable float64 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<float> |} |}>
        records

[<Property>]
let ``record field with nullable decimal field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<decimal> |} |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``record field with nullable date time field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<DateTime> |} |}>
        records

[<Property>]
let ``record field with nullable date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<DateTimeOffset> |} |}>
        records

[<Property>]
let ``record field with nullable guid field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<Guid> |} |}>
        records

[<Property>]
let ``record field with nullable record field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<struct {|
                Field3: int |}> |} |}>
        records

[<Property>]
let ``record field with option bool field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<bool> |} |}>
        records

[<Property>]
let ``record field with option int8 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<int8> |} |}>
        records

[<Property>]
let ``record field with option int16 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<int16> |} |}>
        records

[<Property>]
let ``record field with option int32 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<int> |} |}>
        records

[<Property>]
let ``record field with option int64 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<int64> |} |}>
        records

[<Property>]
let ``record field with option uint8 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<uint8> |} |}>
        records

[<Property>]
let ``record field with option uint16 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<uint16> |} |}>
        records

[<Property>]
let ``record field with option uint32 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<uint> |} |}>
        records

[<Property>]
let ``record field with option uint64 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<uint64> |} |}>
        records

[<Property>]
let ``record field with option float32 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<float32> |} |}>
        records

[<Property>]
let ``record field with option float64 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<float> |} |}>
        records

[<Property>]
let ``record field with option decimal field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<decimal> |} |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``record field with option date time field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<DateTime> |} |}>
        records

[<Property>]
let ``record field with option date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<DateTimeOffset> |} |}>
        records

[<Property>]
let ``record field with option string field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<string> |} |}>
        records

[<Property>]
let ``record field with option guid field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<Guid> |} |}>
        records

[<Property>]
let ``record field with option array field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<array<int>> |} |}>
        records

[<Property>]
let ``record field with option generic list field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<ResizeArray<int>> |} |}>
        records

[<Property>]
let ``record field with option fsharp list field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<list<int>> |} |}>
        records

[<Property>]
let ``record field with option record field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<{|
                Field3: int |}> |} |}>
        records

[<Property>]
let ``record field with option nullable field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<Nullable<int>> |} |}>
        records

[<Property>]
let ``record field with option option field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<option<int>> |} |}>
        records

[<Property>]
let ``nullable bool field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<bool> |}>
        records

[<Property>]
let ``nullable int8 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<int8> |}>
        records

[<Property>]
let ``nullable int16 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<int16> |}>
        records

[<Property>]
let ``nullable int32 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<int> |}>
        records

[<Property>]
let ``nullable int64 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<int64> |}>
        records

[<Property>]
let ``nullable uint8 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<uint8> |}>
        records

[<Property>]
let ``nullable uint16 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<uint16> |}>
        records

[<Property>]
let ``nullable uint32 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<uint> |}>
        records

[<Property>]
let ``nullable uint64 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<uint64> |}>
        records

[<Property>]
let ``nullable float32 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<float32> |}>
        records

[<Property>]
let ``nullable float64 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<float> |}>
        records

[<Property>]
let ``nullable decimal field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<decimal> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``nullable date time field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<DateTime> |}>
        records

[<Property>]
let ``nullable date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<DateTimeOffset> |}>
        records

[<Property>]
let ``nullable guid field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<Guid> |}>
        records

[<Property>]
let ``nullable record field with bool field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: bool |}> |}>
        records

[<Property>]
let ``nullable record field with int8 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: int8 |}> |}>
        records

[<Property>]
let ``nullable record field with int16 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: int16 |}> |}>
        records

[<Property>]
let ``nullable record field with int32 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: int |}> |}>
        records

[<Property>]
let ``nullable record field with int64 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: int64 |}> |}>
        records

[<Property>]
let ``nullable record field with uint8 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: uint8 |}> |}>
        records

[<Property>]
let ``nullable record field with uint16 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: uint16 |}> |}>
        records

[<Property>]
let ``nullable record field with uint32 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: uint |}> |}>
        records

[<Property>]
let ``nullable record field with uint64 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: uint64 |}> |}>
        records

[<Property>]
let ``nullable record field with float32 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: float32 |}> |}>
        records

[<Property>]
let ``nullable record field with float64 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: float |}> |}>
        records

[<Property>]
let ``nullable record field with decimal field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: decimal |}> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``nullable record field with date time field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: DateTime |}> |}>
        records

[<Property>]
let ``nullable record field with date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: DateTimeOffset |}> |}>
        records

[<Property>]
let ``nullable record field with string field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: string |}> |}>
        records

[<Property>]
let ``nullable record field with guid field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Guid |}> |}>
        records

[<Property>]
let ``nullable record field with array field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: array<int> |}> |}>
        records

[<Property>]
let ``nullable record field with generic list field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: ResizeArray<int> |}> |}>
        records

[<Property>]
let ``nullable record field with record field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: struct {|
                Field3: int |} |}> |}>
        records

[<Property>]
let ``nullable record field with nullable bool field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<bool> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable int8 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<int8> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable int16 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<int16> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable int32 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<int> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable int64 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<int64> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable uint8 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<uint8> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable uint16 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<uint16> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable uint32 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<uint> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable uint64 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<uint64> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable float32 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<float32> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable float64 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<float> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable decimal field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<decimal> |}> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``nullable record field with nullable date time field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<DateTime> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<DateTimeOffset> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable guid field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<Guid> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable record field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<struct {|
                Field3: int |}> |}> |}>
        records

[<Property>]
let ``option bool field`` records =
    testRecordRoundtrip<{|
        Field1: option<bool> |}>
        records

[<Property>]
let ``option int8 field`` records =
    testRecordRoundtrip<{|
        Field1: option<int8> |}>
        records

[<Property>]
let ``option int16 field`` records =
    testRecordRoundtrip<{|
        Field1: option<int16> |}>
        records

[<Property>]
let ``option int32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<int> |}>
        records

[<Property>]
let ``option int64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<int64> |}>
        records

[<Property>]
let ``option uint8 field`` records =
    testRecordRoundtrip<{|
        Field1: option<uint8> |}>
        records

[<Property>]
let ``option uint16 field`` records =
    testRecordRoundtrip<{|
        Field1: option<uint16> |}>
        records

[<Property>]
let ``option uint32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<uint> |}>
        records

[<Property>]
let ``option uint64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<uint64> |}>
        records

[<Property>]
let ``option float32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<float32> |}>
        records

[<Property>]
let ``option float64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<float> |}>
        records

[<Property>]
let ``option decimal field`` records =
    testRecordRoundtrip<{|
        Field1: option<decimal> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option date time field`` records =
    testRecordRoundtrip<{|
        Field1: option<DateTime> |}>
        records

[<Property>]
let ``option date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: option<DateTimeOffset> |}>
        records

[<Property>]
let ``option string field`` records =
    testRecordRoundtrip<{|
        Field1: option<string> |}>
        records

[<Property>]
let ``option guid field`` records =
    testRecordRoundtrip<{|
        Field1: option<Guid> |}>
        records

[<Property>]
let ``option array field with bool elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<bool>> |}>
        records

[<Property>]
let ``option array field with int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<int8>> |}>
        records

[<Property>]
let ``option array field with int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<int16>> |}>
        records

[<Property>]
let ``option array field with int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<int>> |}>
        records

[<Property>]
let ``option array field with int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<int64>> |}>
        records

[<Property>]
let ``option array field with uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<uint8>> |}>
        records

[<Property>]
let ``option array field with uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<uint16>> |}>
        records

[<Property>]
let ``option array field with uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<uint>> |}>
        records

[<Property>]
let ``option array field with uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<uint64>> |}>
        records

[<Property>]
let ``option array field with float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<float32>> |}>
        records

[<Property>]
let ``option array field with float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<float>> |}>
        records

[<Property>]
let ``option array field with decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option array field with date time elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<DateTime>> |}>
        records

[<Property>]
let ``option array field with date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<DateTimeOffset>> |}>
        records

[<Property>]
let ``option array field with string elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<string>> |}>
        records

[<Property>]
let ``option array field with guid elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Guid>> |}>
        records

[<Property>]
let ``option array field with array elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<array<int>>> |}>
        records

[<Property>]
let ``option array field with generic list elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<ResizeArray<int>>> |}>
        records

[<Property>]
let ``option array field with fsharp list elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<list<int>>> |}>
        records

[<Property>]
let ``option array field with record elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<{|
            Field2: int |}>> |}>
        records

[<Property>]
let ``option array field with nullable bool elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<bool>>> |}>
        records

[<Property>]
let ``option array field with nullable int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<int8>>> |}>
        records

[<Property>]
let ``option array field with nullable int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<int16>>> |}>
        records

[<Property>]
let ``option array field with nullable int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<int>>> |}>
        records

[<Property>]
let ``option array field with nullable int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<int64>>> |}>
        records

[<Property>]
let ``option array field with nullable uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<uint8>>> |}>
        records

[<Property>]
let ``option array field with nullable uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<uint16>>> |}>
        records

[<Property>]
let ``option array field with nullable uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<uint>>> |}>
        records

[<Property>]
let ``option array field with nullable uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<uint64>>> |}>
        records

[<Property>]
let ``option array field with nullable float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<float32>>> |}>
        records

[<Property>]
let ``option array field with nullable float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<float>>> |}>
        records

[<Property>]
let ``option array field with nullable decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<decimal>>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option array field with nullable date time elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<DateTime>>> |}>
        records

[<Property>]
let ``option array field with nullable date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<DateTimeOffset>>> |}>
        records

[<Property>]
let ``option array field with nullable guid elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<Guid>>> |}>
        records

[<Property>]
let ``option array field with nullable record elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<Nullable<struct {|
            Field2: int |}>>> |}>
        records

[<Property>]
let ``option array field with option bool elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<bool>>> |}>
        records

[<Property>]
let ``option array field with option int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<int8>>> |}>
        records

[<Property>]
let ``option array field with option int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<int16>>> |}>
        records

[<Property>]
let ``option array field with option int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<int>>> |}>
        records

[<Property>]
let ``option array field with option int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<int64>>> |}>
        records

[<Property>]
let ``option array field with option uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<uint8>>> |}>
        records

[<Property>]
let ``option array field with option uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<uint16>>> |}>
        records

[<Property>]
let ``option array field with option uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<uint>>> |}>
        records

[<Property>]
let ``option array field with option uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<uint64>>> |}>
        records

[<Property>]
let ``option array field with option float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<float32>>> |}>
        records

[<Property>]
let ``option array field with option float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<float>>> |}>
        records

[<Property>]
let ``option array field with option decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<decimal>>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option array field with option date time elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<DateTime>>> |}>
        records

[<Property>]
let ``option array field with option date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<DateTimeOffset>>> |}>
        records

[<Property>]
let ``option array field with option string elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<string>>> |}>
        records

[<Property>]
let ``option array field with option guid elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<Guid>>> |}>
        records

[<Property>]
let ``option array field with option array elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<array<int>>>> |}>
        records

[<Property>]
let ``option array field with option generic list elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<ResizeArray<int>>>> |}>
        records

[<Property>]
let ``option array field with option fsharp list elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<list<int>>>> |}>
        records

[<Property>]
let ``option array field with option record elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<{|
            Field2: int |}>>> |}>
        records

[<Property>]
let ``option array field with option nullable elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<Nullable<int>>>> |}>
        records

[<Property>]
let ``option array field with option option elements`` records =
    testRecordRoundtrip<{|
        Field1: option<array<option<option<int>>>> |}>
        records

[<Property>]
let ``option generic list field with bool elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<bool>> |}>
        records

[<Property>]
let ``option generic list field with int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<int8>> |}>
        records

[<Property>]
let ``option generic list field with int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<int16>> |}>
        records

[<Property>]
let ``option generic list field with int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<int>> |}>
        records

[<Property>]
let ``option generic list field with int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<int64>> |}>
        records

[<Property>]
let ``option generic list field with uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<uint8>> |}>
        records

[<Property>]
let ``option generic list field with uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<uint16>> |}>
        records

[<Property>]
let ``option generic list field with uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<uint>> |}>
        records

[<Property>]
let ``option generic list field with uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<uint64>> |}>
        records

[<Property>]
let ``option generic list field with float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<float32>> |}>
        records

[<Property>]
let ``option generic list field with float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<float>> |}>
        records

[<Property>]
let ``option generic list field with decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option generic list field with date time elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<DateTime>> |}>
        records

[<Property>]
let ``option generic list field with date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<DateTimeOffset>> |}>
        records

[<Property>]
let ``option generic list field with string elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<string>> |}>
        records

[<Property>]
let ``option generic list field with guid elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Guid>> |}>
        records

[<Property>]
let ``option generic list field with array elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<array<int>>> |}>
        records

[<Property>]
let ``option generic list field with generic list elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<ResizeArray<int>>> |}>
        records

[<Property>]
let ``option generic list field with fsharp list elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<list<int>>> |}>
        records

[<Property>]
let ``option generic list field with record elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<{|
            Field2: int |}>> |}>
        records

[<Property>]
let ``option generic list field with nullable bool elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<bool>>> |}>
        records

[<Property>]
let ``option generic list field with nullable int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<int8>>> |}>
        records

[<Property>]
let ``option generic list field with nullable int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<int16>>> |}>
        records

[<Property>]
let ``option generic list field with nullable int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<int>>> |}>
        records

[<Property>]
let ``option generic list field with nullable int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<int64>>> |}>
        records

[<Property>]
let ``option generic list field with nullable uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<uint8>>> |}>
        records

[<Property>]
let ``option generic list field with nullable uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<uint16>>> |}>
        records

[<Property>]
let ``option generic list field with nullable uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<uint>>> |}>
        records

[<Property>]
let ``option generic list field with nullable uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<uint64>>> |}>
        records

[<Property>]
let ``option generic list field with nullable float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<float32>>> |}>
        records

[<Property>]
let ``option generic list field with nullable float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<float>>> |}>
        records

[<Property>]
let ``option generic list field with nullable decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<decimal>>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option generic list field with nullable date time elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<DateTime>>> |}>
        records

[<Property>]
let ``option generic list field with nullable date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<DateTimeOffset>>> |}>
        records

[<Property>]
let ``option generic list field with nullable guid elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<Guid>>> |}>
        records

[<Property>]
let ``option generic list field with nullable record elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<Nullable<struct {|
            Field2: int |}>>> |}>
        records

[<Property>]
let ``option generic list field with option bool elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<bool>>> |}>
        records

[<Property>]
let ``option generic list field with option int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<int8>>> |}>
        records

[<Property>]
let ``option generic list field with option int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<int16>>> |}>
        records

[<Property>]
let ``option generic list field with option int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<int>>> |}>
        records

[<Property>]
let ``option generic list field with option int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<int64>>> |}>
        records

[<Property>]
let ``option generic list field with option uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<uint8>>> |}>
        records

[<Property>]
let ``option generic list field with option uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<uint16>>> |}>
        records

[<Property>]
let ``option generic list field with option uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<uint>>> |}>
        records

[<Property>]
let ``option generic list field with option uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<uint64>>> |}>
        records

[<Property>]
let ``option generic list field with option float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<float32>>> |}>
        records

[<Property>]
let ``option generic list field with option float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<float>>> |}>
        records

[<Property>]
let ``option generic list field with option decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<decimal>>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option generic list field with option date time elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<DateTime>>> |}>
        records

[<Property>]
let ``option generic list field with option date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<DateTimeOffset>>> |}>
        records

[<Property>]
let ``option generic list field with option string elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<string>>> |}>
        records

[<Property>]
let ``option generic list field with option guid elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<Guid>>> |}>
        records

[<Property>]
let ``option generic list field with option array elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<array<int>>>> |}>
        records

[<Property>]
let ``option generic list field with option generic list elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<ResizeArray<int>>>> |}>
        records

[<Property>]
let ``option generic list field with option fsharp list elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<list<int>>>> |}>
        records

[<Property>]
let ``option generic list field with option record elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<{|
            Field2: int |}>>> |}>
        records

[<Property>]
let ``option generic list field with option nullable elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<Nullable<int>>>> |}>
        records

[<Property>]
let ``option generic list field with option option elements`` records =
    testRecordRoundtrip<{|
        Field1: option<ResizeArray<option<option<int>>>> |}>
        records

[<Property>]
let ``option fsharp list field with bool elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<bool>> |}>
        records

[<Property>]
let ``option fsharp list field with int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<int8>> |}>
        records

[<Property>]
let ``option fsharp list field with int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<int16>> |}>
        records

[<Property>]
let ``option fsharp list field with int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<int>> |}>
        records

[<Property>]
let ``option fsharp list field with int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<int64>> |}>
        records

[<Property>]
let ``option fsharp list field with uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<uint8>> |}>
        records

[<Property>]
let ``option fsharp list field with uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<uint16>> |}>
        records

[<Property>]
let ``option fsharp list field with uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<uint>> |}>
        records

[<Property>]
let ``option fsharp list field with uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<uint64>> |}>
        records

[<Property>]
let ``option fsharp list field with float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<float32>> |}>
        records

[<Property>]
let ``option fsharp list field with float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<float>> |}>
        records

[<Property>]
let ``option fsharp list field with decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<decimal>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option fsharp list field with date time elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<DateTime>> |}>
        records

[<Property>]
let ``option fsharp list field with date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<DateTimeOffset>> |}>
        records

[<Property>]
let ``option fsharp list field with string elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<string>> |}>
        records

[<Property>]
let ``option fsharp list field with guid elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Guid>> |}>
        records

[<Property>]
let ``option fsharp list field with array elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<array<int>>> |}>
        records

[<Property>]
let ``option fsharp list field with generic list elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<ResizeArray<int>>> |}>
        records

[<Property>]
let ``option fsharp list field with fsharp list elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<list<int>>> |}>
        records

[<Property>]
let ``option fsharp list field with record elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<{|
            Field2: int |}>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable bool elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<bool>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<int8>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<int16>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<int>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<int64>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<uint8>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<uint16>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<uint>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<uint64>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<float32>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<float>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<decimal>>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option fsharp list field with nullable date time elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<DateTime>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<DateTimeOffset>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable guid elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<Guid>>> |}>
        records

[<Property>]
let ``option fsharp list field with nullable record elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<Nullable<struct {|
            Field2: int |}>>> |}>
        records

[<Property>]
let ``option fsharp list field with option bool elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<bool>>> |}>
        records

[<Property>]
let ``option fsharp list field with option int8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<int8>>> |}>
        records

[<Property>]
let ``option fsharp list field with option int16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<int16>>> |}>
        records

[<Property>]
let ``option fsharp list field with option int32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<int>>> |}>
        records

[<Property>]
let ``option fsharp list field with option int64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<int64>>> |}>
        records

[<Property>]
let ``option fsharp list field with option uint8 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<uint8>>> |}>
        records

[<Property>]
let ``option fsharp list field with option uint16 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<uint16>>> |}>
        records

[<Property>]
let ``option fsharp list field with option uint32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<uint>>> |}>
        records

[<Property>]
let ``option fsharp list field with option uint64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<uint64>>> |}>
        records

[<Property>]
let ``option fsharp list field with option float32 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<float32>>> |}>
        records

[<Property>]
let ``option fsharp list field with option float64 elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<float>>> |}>
        records

[<Property>]
let ``option fsharp list field with option decimal elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<decimal>>> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option fsharp list field with option date time elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<DateTime>>> |}>
        records

[<Property>]
let ``option fsharp list field with option date time offset elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<DateTimeOffset>>> |}>
        records

[<Property>]
let ``option fsharp list field with option string elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<string>>> |}>
        records

[<Property>]
let ``option fsharp list field with option guid elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<Guid>>> |}>
        records

[<Property>]
let ``option fsharp list field with option array elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<array<int>>>> |}>
        records

[<Property>]
let ``option fsharp list field with option generic list elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<ResizeArray<int>>>> |}>
        records

[<Property>]
let ``option fsharp list field with option fsharp list elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<list<int>>>> |}>
        records

[<Property>]
let ``option fsharp list field with option record elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<{|
            Field2: int |}>>> |}>
        records

[<Property>]
let ``option fsharp list field with option nullable elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<Nullable<int>>>> |}>
        records

[<Property>]
let ``option fsharp list field with option option elements`` records =
    testRecordRoundtrip<{|
        Field1: option<list<option<option<int>>>> |}>
        records

[<Property>]
let ``option record field with bool field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: bool |}> |}>
        records

[<Property>]
let ``option record field with int8 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: int8 |}> |}>
        records

[<Property>]
let ``option record field with int16 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: int16 |}> |}>
        records

[<Property>]
let ``option record field with int32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: int |}> |}>
        records

[<Property>]
let ``option record field with int64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: int64 |}> |}>
        records

[<Property>]
let ``option record field with uint8 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: uint8 |}> |}>
        records

[<Property>]
let ``option record field with uint16 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: uint16 |}> |}>
        records

[<Property>]
let ``option record field with uint32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: uint |}> |}>
        records

[<Property>]
let ``option record field with uint64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: uint64 |}> |}>
        records

[<Property>]
let ``option record field with float32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: float32 |}> |}>
        records

[<Property>]
let ``option record field with float64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: float |}> |}>
        records

[<Property>]
let ``option record field with decimal field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: decimal |}> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option record field with date time field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: DateTime |}> |}>
        records

[<Property>]
let ``option record field with date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: DateTimeOffset |}> |}>
        records

[<Property>]
let ``option record field with string field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: string |}> |}>
        records

[<Property>]
let ``option record field with guid field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Guid |}> |}>
        records

[<Property>]
let ``option record field with array field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: array<int> |}> |}>
        records

[<Property>]
let ``option record field with generic list field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: ResizeArray<int> |}> |}>
        records

[<Property>]
let ``option record field with fsharp list field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: list<int> |}> |}>
        records

[<Property>]
let ``option record field with record field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: {|
                Field3: int |} |}> |}>
        records

[<Property>]
let ``option record field with nullable bool field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<bool> |}> |}>
        records

[<Property>]
let ``option record field with nullable int8 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<int8> |}> |}>
        records

[<Property>]
let ``option record field with nullable int16 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<int16> |}> |}>
        records

[<Property>]
let ``option record field with nullable int32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<int> |}> |}>
        records

[<Property>]
let ``option record field with nullable int64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<int64> |}> |}>
        records

[<Property>]
let ``option record field with nullable uint8 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<uint8> |}> |}>
        records

[<Property>]
let ``option record field with nullable uint16 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<uint16> |}> |}>
        records

[<Property>]
let ``option record field with nullable uint32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<uint> |}> |}>
        records

[<Property>]
let ``option record field with nullable uint64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<uint64> |}> |}>
        records

[<Property>]
let ``option record field with nullable float32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<float32> |}> |}>
        records

[<Property>]
let ``option record field with nullable float64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<float> |}> |}>
        records

[<Property>]
let ``option record field with nullable decimal field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<decimal> |}> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option record field with nullable date time field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<DateTime> |}> |}>
        records

[<Property>]
let ``option record field with nullable date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<DateTimeOffset> |}> |}>
        records

[<Property>]
let ``option record field with nullable guid field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<Guid> |}> |}>
        records

[<Property>]
let ``option record field with nullable record field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<struct {|
                Field3: int |}> |}> |}>
        records

[<Property>]
let ``option record field with option bool field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<bool> |}> |}>
        records

[<Property>]
let ``option record field with option int8 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<int8> |}> |}>
        records

[<Property>]
let ``option record field with option int16 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<int16> |}> |}>
        records

[<Property>]
let ``option record field with option int32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<int> |}> |}>
        records

[<Property>]
let ``option record field with option int64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<int64> |}> |}>
        records

[<Property>]
let ``option record field with option uint8 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<uint8> |}> |}>
        records

[<Property>]
let ``option record field with option uint16 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<uint16> |}> |}>
        records

[<Property>]
let ``option record field with option uint32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<uint> |}> |}>
        records

[<Property>]
let ``option record field with option uint64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<uint64> |}> |}>
        records

[<Property>]
let ``option record field with option float32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<float32> |}> |}>
        records

[<Property>]
let ``option record field with option float64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<float> |}> |}>
        records

[<Property>]
let ``option record field with option decimal field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<decimal> |}> |}>
        records

[<Property(Arbitrary = [| typeof<UtcDateTime> |])>]
let ``option record field with option date time field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<DateTime> |}> |}>
        records

[<Property>]
let ``option record field with option date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<DateTimeOffset> |}> |}>
        records

[<Property>]
let ``option record field with option string field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<string> |}> |}>
        records

[<Property>]
let ``option record field with option guid field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<Guid> |}> |}>
        records

[<Property>]
let ``option record field with option array field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<array<int>> |}> |}>
        records

[<Property>]
let ``option record field with option generic list field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<ResizeArray<int>> |}> |}>
        records

[<Property>]
let ``option record field with option fsharp list field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<list<int>> |}> |}>
        records

[<Property>]
let ``option record field with option record field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<{|
                Field3: int |}> |}> |}>
        records

[<Property>]
let ``option record field with option nullable field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<Nullable<int>> |}> |}>
        records

[<Property>]
let ``option record field with option option field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<option<int>> |}> |}>
        records

[<Property>]
let ``option nullable field`` records =
    testRecordRoundtrip<{|
        Field1: option<Nullable<int>> |}>
        records

[<Property>]
let ``option option field`` records =
    testRecordRoundtrip<{|
        Field1: option<option<int>> |}>
        records
