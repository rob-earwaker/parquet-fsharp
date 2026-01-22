namespace Parquet.FSharp.Tests.DefaultConverter.DateTimeOffset

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize date time offset`` =
    type Input = { Field1: DateTimeOffset }
    type Output = { Field1: DateTimeOffset }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isTimestamp true "microseconds"
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData((* ticks *)                   0L, (* offsetMins *)    0L)>] // Min value
    [<InlineData((* ticks *)         36000000000L, (* offsetMins *)   60L)>] // Min value with offset
    [<InlineData((* ticks *)               10000L, (* offsetMins *)    0L)>] // Min value + 1ms
    [<InlineData((* ticks *)         36000010000L, (* offsetMins *)   60L)>] // Min value + 1ms with offset
    [<InlineData((* ticks *)  621355967999990000L, (* offsetMins *)    0L)>] // Unix epoch - 1ms
    [<InlineData((* ticks *)  621355968000000000L, (* offsetMins *)    0L)>] // Unix epoch
    [<InlineData((* ticks *)  621355968000010000L, (* offsetMins *)    0L)>] // Unix epoch + 1ms
    [<InlineData((* ticks *)  638752524171230000L, (* offsetMins *) -840L)>] // 15/02/2025 21:40:17.123 -14:00 (min offset)
    [<InlineData((* ticks *)  638752524171230000L, (* offsetMins *)   -1L)>] // 15/02/2025 21:40:17.123 -00:01
    [<InlineData((* ticks *)  638752524171230000L, (* offsetMins *)    0L)>] // 15/02/2025 21:40:17.123 +00:00
    [<InlineData((* ticks *)  638752524171230000L, (* offsetMins *)    1L)>] // 15/02/2025 21:40:17.123 +00:01
    [<InlineData((* ticks *)  638752524171230000L, (* offsetMins *)  840L)>] // 15/02/2025 21:40:17.123 +14:00 (max offset)
    [<InlineData((* ticks *) 3155378975999990000L, (* offsetMins *)    0L)>] // Max value (truncated to millis)
    [<InlineData((* ticks *) 3155378939999990000L, (* offsetMins *)  -60L)>] // Max value (truncated to millis) with offset
    let ``millisecond precision`` (ticks: int64) (offsetMins: int64) =
        let offset = TimeSpan.FromMinutes(offsetMins)
        let value = DateTimeOffset(ticks, offset)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        // We assert using the default {DateTimeOffset} equality comparison on
        // purpose, despite it only comparing the number of UTC ticks and not
        // the offset as well. It's not possible to store the offset as part of
        // a Parquet timestamp type, so all {DateTimeOffset} values have to be
        // stored in UTC.
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

    [<Theory>]
    [<InlineData((* ticks *)                   0L, (* offsetMins *)    0L)>] // Min value
    [<InlineData((* ticks *)         36000000000L, (* offsetMins *)   60L)>] // Min value with offset
    [<InlineData((* ticks *)                  10L, (* offsetMins *)    0L)>] // Min value + 1us
    [<InlineData((* ticks *)         36000000010L, (* offsetMins *)   60L)>] // Min value + 1us with offset
    [<InlineData((* ticks *)  621355967999999990L, (* offsetMins *)    0L)>] // Unix epoch - 1us
    [<InlineData((* ticks *)  621355968000000000L, (* offsetMins *)    0L)>] // Unix epoch
    [<InlineData((* ticks *)  621355968000000010L, (* offsetMins *)    0L)>] // Unix epoch + 1us
    [<InlineData((* ticks *)  638752524171234560L, (* offsetMins *) -840L)>] // 15/02/2025 21:40:17.123456 -14:00 (min offset)
    [<InlineData((* ticks *)  638752524171234560L, (* offsetMins *)   -1L)>] // 15/02/2025 21:40:17.123456 -00:01
    [<InlineData((* ticks *)  638752524171234560L, (* offsetMins *)    0L)>] // 15/02/2025 21:40:17.123456 +00:00
    [<InlineData((* ticks *)  638752524171234560L, (* offsetMins *)    1L)>] // 15/02/2025 21:40:17.123456 +00:01
    [<InlineData((* ticks *)  638752524171234560L, (* offsetMins *)  840L)>] // 15/02/2025 21:40:17.123456 +14:00 (max offset)
    [<InlineData((* ticks *) 3155378975999999990L, (* offsetMins *)    0L)>] // Max value (truncated to micros)
    [<InlineData((* ticks *) 3155378939999999990L, (* offsetMins *)  -60L)>] // Max value (truncated to micros) with offset
    let ``microsecond precision`` (ticks: int64) (offsetMins: int64) =
        let offset = TimeSpan.FromMinutes(offsetMins)
        let value = DateTimeOffset(ticks, offset)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        // We assert using the default {DateTimeOffset} equality comparison on
        // purpose, despite it only comparing the number of UTC ticks and not
        // the offset as well. It's not possible to store the offset as part of
        // a Parquet timestamp type, so all {DateTimeOffset} values have to be
        // stored in UTC.
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

    [<Theory>]
    [<InlineData((* ticks *)                   0L, (* offsetMins *)    0L)>] // Min value
    [<InlineData((* ticks *)         36000000000L, (* offsetMins *)   60L)>] // Min value with offset
    [<InlineData((* ticks *)                   1L, (* offsetMins *)    0L)>] // Min value + 100ns
    [<InlineData((* ticks *)         36000000001L, (* offsetMins *)   60L)>] // Min value + 100ns with offset
    [<InlineData((* ticks *)  621355967999999999L, (* offsetMins *)    0L)>] // Unix epoch - 100ns
    [<InlineData((* ticks *)  621355968000000000L, (* offsetMins *)    0L)>] // Unix epoch
    [<InlineData((* ticks *)  621355968000000001L, (* offsetMins *)    0L)>] // Unix epoch + 100ns
    [<InlineData((* ticks *)  638752524171234567L, (* offsetMins *) -840L)>] // 15/02/2025 21:40:17.1234567 -14:00 (min offset)
    [<InlineData((* ticks *)  638752524171234567L, (* offsetMins *)   -1L)>] // 15/02/2025 21:40:17.1234567 -00:01
    [<InlineData((* ticks *)  638752524171234567L, (* offsetMins *)    0L)>] // 15/02/2025 21:40:17.1234567 +00:00
    [<InlineData((* ticks *)  638752524171234567L, (* offsetMins *)    1L)>] // 15/02/2025 21:40:17.1234567 +00:01
    [<InlineData((* ticks *)  638752524171234567L, (* offsetMins *)  840L)>] // 15/02/2025 21:40:17.1234567 +14:00 (max offset)
    [<InlineData((* ticks *) 3155378975999999999L, (* offsetMins *)    0L)>] // Max value
    [<InlineData((* ticks *) 3155378939999999999L, (* offsetMins *)  -60L)>] // Max value with offset
    let ``nanosecond precision`` (ticks: int64) (offsetMins: int64) =
        let offset = TimeSpan.FromMinutes(offsetMins)
        let value = DateTimeOffset(ticks, offset)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to be truncated to microsecond precision.
        let expectedValue = value.AddTicks(-(value.Ticks % 10L))
        // We assert using the default {DateTimeOffset} equality comparison on
        // purpose, despite it only comparing the number of UTC ticks and not
        // the offset as well. It's not possible to store the offset as part of
        // a Parquet timestamp type, so all {DateTimeOffset} values have to be
        // stored in UTC.
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``deserialize date time offset from required utc micros logical timestamp`` =
    type Input = { Field1: DateTime }
    type Output = { Field1: DateTimeOffset }

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                 10L)>] // Min value + 1us
    [<InlineData( 621355967999999990L)>] // Unix epoch - 1us
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000010L)>] // Unix epoch + 1us
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
    let ``value`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = DateTimeOffset(ticks, TimeSpan.Zero)
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``deserialize date time offset from optional utc micros logical timestamp`` =
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
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                 10L)>] // Min value + 1us
    [<InlineData( 621355967999999990L)>] // Unix epoch - 1us
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000010L)>] // Unix epoch + 1us
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
    let ``non-null value`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = DateTimeOffset(ticks, TimeSpan.Zero)
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
