namespace Parquet.FSharp.Tests.DateTimeOffset

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize date time offset`` =
    type Input = { Field1: DateTimeOffset }
    type Output = { Field1: DateTimeOffset }

    [<Theory>]
    [<InlineData((* ticks *)                   0L, (* offsetMins *)    0L)>] // Min value
    [<InlineData((* ticks *)         36000000000L, (* offsetMins *)   60L)>] // Min value with offset
    [<InlineData((* ticks *)  621355968000000000L, (* offsetMins *)    0L)>] // Unix epoch
    [<InlineData((* ticks *)  638752524170000000L, (* offsetMins *) -840L)>] // 15/02/2025 21:40:17 -14:00 (min offset)
    [<InlineData((* ticks *)  638752524170000000L, (* offsetMins *)   -1L)>] // 15/02/2025 21:40:17 -00:01
    [<InlineData((* ticks *)  638752524170000000L, (* offsetMins *)    0L)>] // 15/02/2025 21:40:17 +00:00
    [<InlineData((* ticks *)  638752524170000000L, (* offsetMins *)    1L)>] // 15/02/2025 21:40:17 +00:01
    [<InlineData((* ticks *)  638752524170000000L, (* offsetMins *)  840L)>] // 15/02/2025 21:40:17 +14:00 (max offset)
    [<InlineData((* ticks *) 3155378975999999999L, (* offsetMins *)    0L)>] // Max value
    [<InlineData((* ticks *) 3155378939999999999L, (* offsetMins *)  -60L)>] // Max value with offset
    let ``value`` (ticks: int64) (offsetMins: int64) =
        let offset = TimeSpan.FromMinutes(offsetMins)
        let value = DateTimeOffset(ticks, offset)
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
        // We assert using the default {DateTimeOffset} equality comparison on
        // purpose, despite it only comparing the number of UTC ticks and not
        // the offset as well. It's not possible to store the offset as part of
        // a Parquet INT96 or TIMESTAMP type, so all {DateTimeOffset} values
        // have to be stored in UTC.
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize date time offset from required int96`` =
    type Input = { Field1: DateTime }
    type Output = { Field1: DateTimeOffset }

    [<Theory>]
    [<InlineData((* ticks *)                   0L)>] // Min value
    [<InlineData((* ticks *)  621355968000000000L)>] // Unix epoch
    [<InlineData((* ticks *)  638752524170000000L)>] // 15/02/2025 21:40:17
    [<InlineData((* ticks *) 3155378975999999999L)>] // Max value
    let ``value`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = DateTimeOffset(ticks, TimeSpan.Zero)
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``deserialize date time offset from optional int96`` =
    type Input = { Field1: DateTime option }
    type Output = { Field1: DateTimeOffset }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<DateTimeOffset>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* ticks *)                   0L)>] // Min value
    [<InlineData((* ticks *)  621355968000000000L)>] // Unix epoch
    [<InlineData((* ticks *)  638752524170000000L)>] // 15/02/2025 21:40:17
    [<InlineData((* ticks *) 3155378975999999999L)>] // Max value
    let ``non-null value`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = DateTimeOffset(ticks, TimeSpan.Zero)
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
