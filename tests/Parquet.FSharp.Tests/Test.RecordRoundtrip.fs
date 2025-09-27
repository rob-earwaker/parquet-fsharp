module Parquet.FSharp.Tests.RecordRoundtrip

open FluentAssertions
open FsCheck.Xunit
open Parquet.FSharp
open Swensen.Unquote
open System
open System.IO

let testRecordRoundtrip<'Record when 'Record : equality> (records: 'Record[]) =
    use stream = new MemoryStream()
    let parquetWriter = ParquetStreamWriter<'Record>(stream)
    parquetWriter.WriteHeader()
    parquetWriter.WriteRowGroup(records)
    parquetWriter.WriteFooter()
    stream.Seek(0, SeekOrigin.Begin) |> ignore
    let parquetReader = ParquetStreamReader<'Record>(stream)
    parquetReader.ReadMetaData()
    let roundtrippedRecords = parquetReader.ReadRowGroup(0)
    let configureEquivalencyOptions (options: Equivalency.EquivalencyOptions<'Record>) =
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
let ``int32 field`` records =
    testRecordRoundtrip<{|
        Field1: int |}>
        records

[<Property>]
let ``float64 field`` records =
    testRecordRoundtrip<{|
        Field1: float |}>
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
let ``record field with bool field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: bool |} |}>
        records

[<Property>]
let ``record field with int32 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: int |} |}>
        records

[<Property>]
let ``record field with float64 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: float |} |}>
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
let ``record field with nullable int32 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<int> |} |}>
        records

[<Property>]
let ``record field with nullable float64 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<float> |} |}>
        records

[<Property>]
let ``record field with nullable date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: Nullable<DateTimeOffset> |} |}>
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
let ``record field with option int32 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<int> |} |}>
        records

[<Property>]
let ``record field with option float64 field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<float> |} |}>
        records

[<Property>]
let ``record field with option date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<DateTimeOffset> |} |}>
        records

[<Property>]
let ``record field with option record field`` records =
    testRecordRoundtrip<{|
        Field1: {|
            Field2: option<{|
                Field3: int |}> |} |}>
        records

[<Property>]
let ``nullable bool field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<bool> |}>
        records

[<Property>]
let ``nullable int32 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<int> |}>
        records

[<Property>]
let ``nullable float64 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<float> |}>
        records

[<Property>]
let ``nullable date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<DateTimeOffset> |}>
        records

[<Property>]
let ``nullable record field with bool field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: bool |}> |}>
        records

[<Property>]
let ``nullable record field with int32 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: int |}> |}>
        records

[<Property>]
let ``nullable record field with float64 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: float |}> |}>
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
let ``nullable record field with nullable int32 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<int> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable float64 field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<float> |}> |}>
        records

[<Property>]
let ``nullable record field with nullable date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: Nullable<struct {|
            Field2: Nullable<DateTimeOffset> |}> |}>
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
let ``option int32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<int> |}>
        records

[<Property>]
let ``option float64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<float> |}>
        records

[<Property>]
let ``option date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: option<DateTimeOffset> |}>
        records

[<Property>]
let ``option record field with bool field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: bool |}> |}>
        records

[<Property>]
let ``option record field with int32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: int |}> |}>
        records

[<Property>]
let ``option record field with float64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: float |}> |}>
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
let ``option record field with nullable int32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<int> |}> |}>
        records

[<Property>]
let ``option record field with nullable float64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<float> |}> |}>
        records

[<Property>]
let ``option record field with nullable date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: Nullable<DateTimeOffset> |}> |}>
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
let ``option record field with option int32 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<int> |}> |}>
        records

[<Property>]
let ``option record field with option float64 field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<float> |}> |}>
        records

[<Property>]
let ``option record field with option date time offset field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<DateTimeOffset> |}> |}>
        records

[<Property>]
let ``option record field with option record field`` records =
    testRecordRoundtrip<{|
        Field1: option<{|
            Field2: option<{|
                Field3: int |}> |}> |}>
        records
