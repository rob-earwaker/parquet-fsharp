namespace Parquet.FSharp.Tests.DateTime

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize date time`` =
    type Input = { Field1: DateTime }
    type Output = { Field1: DateTime }

    [<Theory>]
    //[<InlineData((* ticks *)                   0L, (* kind *) DateTimeKind.Unspecified)>] // Min value
    //[<InlineData((* ticks *)  621355968000000000L, (* kind *) DateTimeKind.Unspecified)>] // Unix epoch
    //[<InlineData((* ticks *)  638752524170000000L, (* kind *) DateTimeKind.Unspecified)>] // 15/02/2025 21:40:17
    //[<InlineData((* ticks *) 3155378975999999999L, (* kind *) DateTimeKind.Unspecified)>] // Max value
    [<InlineData((* ticks *)                   0L, (* kind *) DateTimeKind.Utc)>] // Min value
    [<InlineData((* ticks *)  621355968000000000L, (* kind *) DateTimeKind.Utc)>] // Unix epoch
    [<InlineData((* ticks *)  638752524170000000L, (* kind *) DateTimeKind.Utc)>] // 15/02/2025 21:40:17
    [<InlineData((* ticks *) 3155378975999999999L, (* kind *) DateTimeKind.Utc)>] // Max value
    //[<InlineData((* ticks *)                   0L, (* kind *) DateTimeKind.Local)>] // Min value
    //[<InlineData((* ticks *)  621355968000000000L, (* kind *) DateTimeKind.Local)>] // Unix epoch
    //[<InlineData((* ticks *)  638752524170000000L, (* kind *) DateTimeKind.Local)>] // 15/02/2025 21:40:17
    //[<InlineData((* ticks *) 3155378975999999999L, (* kind *) DateTimeKind.Local)>] // Max value
    let ``value`` (ticks: int64) kind =
        let value = DateTime(ticks, kind)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt96
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = value @>
        test <@ outputRecord.Field1.Kind = kind @>

module ``deserialize date time from required int96`` =
    type Input = { Field1: DateTime }
    type Output = { Field1: DateTime }

    [<Theory>]
    //[<InlineData((* ticks *)                   0L, (* kind *) DateTimeKind.Unspecified)>] // Min value
    //[<InlineData((* ticks *)  621355968000000000L, (* kind *) DateTimeKind.Unspecified)>] // Unix epoch
    //[<InlineData((* ticks *)  638752524170000000L, (* kind *) DateTimeKind.Unspecified)>] // 15/02/2025 21:40:17
    //[<InlineData((* ticks *) 3155378975999999999L, (* kind *) DateTimeKind.Unspecified)>] // Max value
    [<InlineData((* ticks *)                   0L, (* kind *) DateTimeKind.Utc)>] // Min value
    [<InlineData((* ticks *)  621355968000000000L, (* kind *) DateTimeKind.Utc)>] // Unix epoch
    [<InlineData((* ticks *)  638752524170000000L, (* kind *) DateTimeKind.Utc)>] // 15/02/2025 21:40:17
    [<InlineData((* ticks *) 3155378975999999999L, (* kind *) DateTimeKind.Utc)>] // Max value
    //[<InlineData((* ticks *)                   0L, (* kind *) DateTimeKind.Local)>] // Min value
    //[<InlineData((* ticks *)  621355968000000000L, (* kind *) DateTimeKind.Local)>] // Unix epoch
    //[<InlineData((* ticks *)  638752524170000000L, (* kind *) DateTimeKind.Local)>] // 15/02/2025 21:40:17
    //[<InlineData((* ticks *) 3155378975999999999L, (* kind *) DateTimeKind.Local)>] // Max value
    let ``value`` (ticks: int64) kind =
        let value = DateTime(ticks, kind)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = value @>
        test <@ outputRecord.Field1.Kind = kind @>

module ``deserialize date time from optional int96`` =
    type Input = { Field1: DateTime option }
    type Output = { Field1: DateTime }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<DateTime>.FullName}'" @>)

    [<Theory>]
    //[<InlineData((* ticks *)                   0L, (* kind *) DateTimeKind.Unspecified)>] // Min value
    //[<InlineData((* ticks *)  621355968000000000L, (* kind *) DateTimeKind.Unspecified)>] // Unix epoch
    //[<InlineData((* ticks *)  638752524170000000L, (* kind *) DateTimeKind.Unspecified)>] // 15/02/2025 21:40:17
    //[<InlineData((* ticks *) 3155378975999999999L, (* kind *) DateTimeKind.Unspecified)>] // Max value
    [<InlineData((* ticks *)                   0L, (* kind *) DateTimeKind.Utc)>] // Min value
    [<InlineData((* ticks *)  621355968000000000L, (* kind *) DateTimeKind.Utc)>] // Unix epoch
    [<InlineData((* ticks *)  638752524170000000L, (* kind *) DateTimeKind.Utc)>] // 15/02/2025 21:40:17
    [<InlineData((* ticks *) 3155378975999999999L, (* kind *) DateTimeKind.Utc)>] // Max value
    //[<InlineData((* ticks *)                   0L, (* kind *) DateTimeKind.Local)>] // Min value
    //[<InlineData((* ticks *)  621355968000000000L, (* kind *) DateTimeKind.Local)>] // Unix epoch
    //[<InlineData((* ticks *)  638752524170000000L, (* kind *) DateTimeKind.Local)>] // 15/02/2025 21:40:17
    //[<InlineData((* ticks *) 3155378975999999999L, (* kind *) DateTimeKind.Local)>] // Max value
    let ``non-null value`` (ticks: int64) kind =
        let value = DateTime(ticks, kind)
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = value @>
        test <@ outputRecord.Field1.Kind = kind @>
