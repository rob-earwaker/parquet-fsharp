namespace Parquet.FSharp.Tests.DateTimeOffset

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

// TODO: Add min/max tests for nanoseconds - bug in Parquet.Net? Document in README!

module ``{ default } serialize`` =
    type Input = { Field1: DateTimeOffset }
    type Output = { Field1: DateTime }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isTimestamp "utc" "microseconds"
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
        let expectedValue = value.UtcDateTime
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
        let expectedValue = value.UtcDateTime
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
        let expectedValue = value.UtcDateTime.AddTicks(-(value.Ticks % TimeSpan.TicksPerMicrosecond))
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ optional=true } serialize`` =
    type Input = { [<ParquetDateTimeOffset(Optional = true)>] Field1: DateTimeOffset }
    type Output = { Field1: DateTime option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isTimestamp "utc" "microseconds"
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
        let expectedValue = value.UtcDateTime
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

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
        let expectedValue = value.UtcDateTime
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

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
        let expectedValue = value.UtcDateTime.AddTicks(-(value.Ticks % TimeSpan.TicksPerMicrosecond))
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

module ``{ unit=milliseconds } serialize`` =
    type Input = { [<ParquetDateTimeOffset(Unit = TimeUnit.Milliseconds)>] Field1: DateTimeOffset }
    type Output = { [<ParquetDateTime(Unit = TimeUnit.Milliseconds)>] Field1: DateTime }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isTimestamp "utc" "milliseconds"
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
        let expectedValue = value.UtcDateTime
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
        // Expect the value to be truncated to millisecond precision.
        let expectedValue = value.UtcDateTime.AddTicks(-(value.Ticks % TimeSpan.TicksPerMillisecond))
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
        // Expect the value to be truncated to millisecond precision.
        let expectedValue = value.UtcDateTime.AddTicks(-(value.Ticks % TimeSpan.TicksPerMillisecond))
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ unit=milliseconds; optional=true } serialize`` =
    type Input = {
        [<ParquetDateTimeOffset(Unit = TimeUnit.Milliseconds, Optional = true)>]
        Field1: DateTimeOffset }

    type Output = {
        [<ParquetDateTime(NestingLevel = 1, Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isTimestamp "utc" "milliseconds"
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
        let expectedValue = value.UtcDateTime
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

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
        // Expect the value to be truncated to millisecond precision.
        let expectedValue = value.UtcDateTime.AddTicks(-(value.Ticks % TimeSpan.TicksPerMillisecond))
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

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
        // Expect the value to be truncated to millisecond precision.
        let expectedValue = value.UtcDateTime.AddTicks(-(value.Ticks % TimeSpan.TicksPerMillisecond))
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

module ``{ unit=nanoseconds } serialize`` =
    type Input = { [<ParquetDateTimeOffset(Unit = TimeUnit.Nanoseconds)>] Field1: DateTimeOffset }
    type Output = { [<ParquetDateTime(Unit = TimeUnit.Nanoseconds)>] Field1: DateTime }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isTimestamp "utc" "nanoseconds"
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    //[<InlineData((* ticks *)                   0L, (* offsetMins *)    0L)>] // Min value
    //[<InlineData((* ticks *)         36000000000L, (* offsetMins *)   60L)>] // Min value with offset
    //[<InlineData((* ticks *)               10000L, (* offsetMins *)    0L)>] // Min value + 1ms
    //[<InlineData((* ticks *)         36000010000L, (* offsetMins *)   60L)>] // Min value + 1ms with offset
    [<InlineData((* ticks *)  621355967999990000L, (* offsetMins *)    0L)>] // Unix epoch - 1ms
    [<InlineData((* ticks *)  621355968000000000L, (* offsetMins *)    0L)>] // Unix epoch
    [<InlineData((* ticks *)  621355968000010000L, (* offsetMins *)    0L)>] // Unix epoch + 1ms
    [<InlineData((* ticks *)  638752524171230000L, (* offsetMins *) -840L)>] // 15/02/2025 21:40:17.123 -14:00 (min offset)
    [<InlineData((* ticks *)  638752524171230000L, (* offsetMins *)   -1L)>] // 15/02/2025 21:40:17.123 -00:01
    [<InlineData((* ticks *)  638752524171230000L, (* offsetMins *)    0L)>] // 15/02/2025 21:40:17.123 +00:00
    [<InlineData((* ticks *)  638752524171230000L, (* offsetMins *)    1L)>] // 15/02/2025 21:40:17.123 +00:01
    [<InlineData((* ticks *)  638752524171230000L, (* offsetMins *)  840L)>] // 15/02/2025 21:40:17.123 +14:00 (max offset)
    //[<InlineData((* ticks *) 3155378975999990000L, (* offsetMins *)    0L)>] // Max value (truncated to millis)
    //[<InlineData((* ticks *) 3155378939999990000L, (* offsetMins *)  -60L)>] // Max value (truncated to millis) with offset
    let ``millisecond precision`` (ticks: int64) (offsetMins: int64) =
        let offset = TimeSpan.FromMinutes(offsetMins)
        let value = DateTimeOffset(ticks, offset)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value.UtcDateTime
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

    [<Theory>]
    //[<InlineData((* ticks *)                   0L, (* offsetMins *)    0L)>] // Min value
    //[<InlineData((* ticks *)         36000000000L, (* offsetMins *)   60L)>] // Min value with offset
    //[<InlineData((* ticks *)                  10L, (* offsetMins *)    0L)>] // Min value + 1us
    //[<InlineData((* ticks *)         36000000010L, (* offsetMins *)   60L)>] // Min value + 1us with offset
    [<InlineData((* ticks *)  621355967999999990L, (* offsetMins *)    0L)>] // Unix epoch - 1us
    [<InlineData((* ticks *)  621355968000000000L, (* offsetMins *)    0L)>] // Unix epoch
    [<InlineData((* ticks *)  621355968000000010L, (* offsetMins *)    0L)>] // Unix epoch + 1us
    [<InlineData((* ticks *)  638752524171234560L, (* offsetMins *) -840L)>] // 15/02/2025 21:40:17.123456 -14:00 (min offset)
    [<InlineData((* ticks *)  638752524171234560L, (* offsetMins *)   -1L)>] // 15/02/2025 21:40:17.123456 -00:01
    [<InlineData((* ticks *)  638752524171234560L, (* offsetMins *)    0L)>] // 15/02/2025 21:40:17.123456 +00:00
    [<InlineData((* ticks *)  638752524171234560L, (* offsetMins *)    1L)>] // 15/02/2025 21:40:17.123456 +00:01
    [<InlineData((* ticks *)  638752524171234560L, (* offsetMins *)  840L)>] // 15/02/2025 21:40:17.123456 +14:00 (max offset)
    //[<InlineData((* ticks *) 3155378975999999990L, (* offsetMins *)    0L)>] // Max value (truncated to micros)
    //[<InlineData((* ticks *) 3155378939999999990L, (* offsetMins *)  -60L)>] // Max value (truncated to micros) with offset
    let ``microsecond precision`` (ticks: int64) (offsetMins: int64) =
        let offset = TimeSpan.FromMinutes(offsetMins)
        let value = DateTimeOffset(ticks, offset)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value.UtcDateTime
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

    [<Theory>]
    //[<InlineData((* ticks *)                   0L, (* offsetMins *)    0L)>] // Min value
    //[<InlineData((* ticks *)         36000000000L, (* offsetMins *)   60L)>] // Min value with offset
    //[<InlineData((* ticks *)                   1L, (* offsetMins *)    0L)>] // Min value + 100ns
    //[<InlineData((* ticks *)         36000000001L, (* offsetMins *)   60L)>] // Min value + 100ns with offset
    [<InlineData((* ticks *)  621355967999999999L, (* offsetMins *)    0L)>] // Unix epoch - 100ns
    [<InlineData((* ticks *)  621355968000000000L, (* offsetMins *)    0L)>] // Unix epoch
    [<InlineData((* ticks *)  621355968000000001L, (* offsetMins *)    0L)>] // Unix epoch + 100ns
    [<InlineData((* ticks *)  638752524171234567L, (* offsetMins *) -840L)>] // 15/02/2025 21:40:17.1234567 -14:00 (min offset)
    [<InlineData((* ticks *)  638752524171234567L, (* offsetMins *)   -1L)>] // 15/02/2025 21:40:17.1234567 -00:01
    [<InlineData((* ticks *)  638752524171234567L, (* offsetMins *)    0L)>] // 15/02/2025 21:40:17.1234567 +00:00
    [<InlineData((* ticks *)  638752524171234567L, (* offsetMins *)    1L)>] // 15/02/2025 21:40:17.1234567 +00:01
    [<InlineData((* ticks *)  638752524171234567L, (* offsetMins *)  840L)>] // 15/02/2025 21:40:17.1234567 +14:00 (max offset)
    //[<InlineData((* ticks *) 3155378975999999999L, (* offsetMins *)    0L)>] // Max value
    //[<InlineData((* ticks *) 3155378939999999999L, (* offsetMins *)  -60L)>] // Max value with offset
    let ``nanosecond precision`` (ticks: int64) (offsetMins: int64) =
        let offset = TimeSpan.FromMinutes(offsetMins)
        let value = DateTimeOffset(ticks, offset)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value.UtcDateTime
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ unit=nanoseconds; optional=true } serialize`` =
    type Input = {
        [<ParquetDateTimeOffset(Unit = TimeUnit.Nanoseconds, Optional = true)>]
        Field1: DateTimeOffset }

    type Output = {
        [<ParquetDateTime(NestingLevel = 1, Unit = TimeUnit.Nanoseconds)>]
        Field1: DateTime option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isTimestamp "utc" "nanoseconds"
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    //[<InlineData((* ticks *)                   0L, (* offsetMins *)    0L)>] // Min value
    //[<InlineData((* ticks *)         36000000000L, (* offsetMins *)   60L)>] // Min value with offset
    //[<InlineData((* ticks *)               10000L, (* offsetMins *)    0L)>] // Min value + 1ms
    //[<InlineData((* ticks *)         36000010000L, (* offsetMins *)   60L)>] // Min value + 1ms with offset
    [<InlineData((* ticks *)  621355967999990000L, (* offsetMins *)    0L)>] // Unix epoch - 1ms
    [<InlineData((* ticks *)  621355968000000000L, (* offsetMins *)    0L)>] // Unix epoch
    [<InlineData((* ticks *)  621355968000010000L, (* offsetMins *)    0L)>] // Unix epoch + 1ms
    [<InlineData((* ticks *)  638752524171230000L, (* offsetMins *) -840L)>] // 15/02/2025 21:40:17.123 -14:00 (min offset)
    [<InlineData((* ticks *)  638752524171230000L, (* offsetMins *)   -1L)>] // 15/02/2025 21:40:17.123 -00:01
    [<InlineData((* ticks *)  638752524171230000L, (* offsetMins *)    0L)>] // 15/02/2025 21:40:17.123 +00:00
    [<InlineData((* ticks *)  638752524171230000L, (* offsetMins *)    1L)>] // 15/02/2025 21:40:17.123 +00:01
    [<InlineData((* ticks *)  638752524171230000L, (* offsetMins *)  840L)>] // 15/02/2025 21:40:17.123 +14:00 (max offset)
    //[<InlineData((* ticks *) 3155378975999990000L, (* offsetMins *)    0L)>] // Max value (truncated to millis)
    //[<InlineData((* ticks *) 3155378939999990000L, (* offsetMins *)  -60L)>] // Max value (truncated to millis) with offset
    let ``millisecond precision`` (ticks: int64) (offsetMins: int64) =
        let offset = TimeSpan.FromMinutes(offsetMins)
        let value = DateTimeOffset(ticks, offset)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value.UtcDateTime
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

    [<Theory>]
    //[<InlineData((* ticks *)                   0L, (* offsetMins *)    0L)>] // Min value
    //[<InlineData((* ticks *)         36000000000L, (* offsetMins *)   60L)>] // Min value with offset
    //[<InlineData((* ticks *)                  10L, (* offsetMins *)    0L)>] // Min value + 1us
    //[<InlineData((* ticks *)         36000000010L, (* offsetMins *)   60L)>] // Min value + 1us with offset
    [<InlineData((* ticks *)  621355967999999990L, (* offsetMins *)    0L)>] // Unix epoch - 1us
    [<InlineData((* ticks *)  621355968000000000L, (* offsetMins *)    0L)>] // Unix epoch
    [<InlineData((* ticks *)  621355968000000010L, (* offsetMins *)    0L)>] // Unix epoch + 1us
    [<InlineData((* ticks *)  638752524171234560L, (* offsetMins *) -840L)>] // 15/02/2025 21:40:17.123456 -14:00 (min offset)
    [<InlineData((* ticks *)  638752524171234560L, (* offsetMins *)   -1L)>] // 15/02/2025 21:40:17.123456 -00:01
    [<InlineData((* ticks *)  638752524171234560L, (* offsetMins *)    0L)>] // 15/02/2025 21:40:17.123456 +00:00
    [<InlineData((* ticks *)  638752524171234560L, (* offsetMins *)    1L)>] // 15/02/2025 21:40:17.123456 +00:01
    [<InlineData((* ticks *)  638752524171234560L, (* offsetMins *)  840L)>] // 15/02/2025 21:40:17.123456 +14:00 (max offset)
    //[<InlineData((* ticks *) 3155378975999999990L, (* offsetMins *)    0L)>] // Max value (truncated to micros)
    //[<InlineData((* ticks *) 3155378939999999990L, (* offsetMins *)  -60L)>] // Max value (truncated to micros) with offset
    let ``microsecond precision`` (ticks: int64) (offsetMins: int64) =
        let offset = TimeSpan.FromMinutes(offsetMins)
        let value = DateTimeOffset(ticks, offset)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value.UtcDateTime
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

    [<Theory>]
    //[<InlineData((* ticks *)                   0L, (* offsetMins *)    0L)>] // Min value
    //[<InlineData((* ticks *)         36000000000L, (* offsetMins *)   60L)>] // Min value with offset
    //[<InlineData((* ticks *)                   1L, (* offsetMins *)    0L)>] // Min value + 100ns
    //[<InlineData((* ticks *)         36000000001L, (* offsetMins *)   60L)>] // Min value + 100ns with offset
    [<InlineData((* ticks *)  621355967999999999L, (* offsetMins *)    0L)>] // Unix epoch - 100ns
    [<InlineData((* ticks *)  621355968000000000L, (* offsetMins *)    0L)>] // Unix epoch
    [<InlineData((* ticks *)  621355968000000001L, (* offsetMins *)    0L)>] // Unix epoch + 100ns
    [<InlineData((* ticks *)  638752524171234567L, (* offsetMins *) -840L)>] // 15/02/2025 21:40:17.1234567 -14:00 (min offset)
    [<InlineData((* ticks *)  638752524171234567L, (* offsetMins *)   -1L)>] // 15/02/2025 21:40:17.1234567 -00:01
    [<InlineData((* ticks *)  638752524171234567L, (* offsetMins *)    0L)>] // 15/02/2025 21:40:17.1234567 +00:00
    [<InlineData((* ticks *)  638752524171234567L, (* offsetMins *)    1L)>] // 15/02/2025 21:40:17.1234567 +00:01
    [<InlineData((* ticks *)  638752524171234567L, (* offsetMins *)  840L)>] // 15/02/2025 21:40:17.1234567 +14:00 (max offset)
    //[<InlineData((* ticks *) 3155378975999999999L, (* offsetMins *)    0L)>] // Max value
    //[<InlineData((* ticks *) 3155378939999999999L, (* offsetMins *)  -60L)>] // Max value with offset
    let ``nanosecond precision`` (ticks: int64) (offsetMins: int64) =
        let offset = TimeSpan.FromMinutes(offsetMins)
        let value = DateTimeOffset(ticks, offset)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value.UtcDateTime
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

module ``{ default } deserialize`` =
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

module ``{ optional=true } deserialize`` =
    type Input = { Field1: DateTime option }
    type Output = { [<ParquetDateTimeOffset(Optional = true)>] Field1: DateTimeOffset }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<DateTimeOffset>}'" @>)

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                 10L)>] // Min value + 1us
    [<InlineData( 621355967999999990L)>] // Unix epoch - 1us
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000010L)>] // Unix epoch + 1us
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
    let ``non-null`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = DateTimeOffset(ticks, TimeSpan.Zero)
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ unit=milliseconds } deserialize`` =
    type Input = { [<ParquetDateTime(Unit = TimeUnit.Milliseconds)>] Field1: DateTime }
    type Output = { [<ParquetDateTimeOffset(Unit = TimeUnit.Milliseconds)>] Field1: DateTimeOffset }

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(              10000L)>] // Min value + 1ms
    [<InlineData( 621355967999990000L)>] // Unix epoch - 1ms
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000010000L)>] // Unix epoch + 1ms
    [<InlineData( 638752524171230000L)>] // 15/02/2025 21:40:17.123
    [<InlineData(3155378975999990000L)>] // Max value (truncated to millis)
    let ``value`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = DateTimeOffset(ticks, TimeSpan.Zero)
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ unit=milliseconds; optional=true } deserialize`` =
    type Input = {
        [<ParquetDateTime(NestingLevel = 1, Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime option }

    type Output = {
        [<ParquetDateTimeOffset(Unit = TimeUnit.Milliseconds, Optional = true)>]
        Field1: DateTimeOffset }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<DateTimeOffset>}'" @>)

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(              10000L)>] // Min value + 1ms
    [<InlineData( 621355967999990000L)>] // Unix epoch - 1ms
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000010000L)>] // Unix epoch + 1ms
    [<InlineData( 638752524171230000L)>] // 15/02/2025 21:40:17.123
    [<InlineData(3155378975999990000L)>] // Max value (truncated to millis)
    let ``non-null`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = DateTimeOffset(ticks, TimeSpan.Zero)
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ unit=nanoseconds } deserialize`` =
    type Input = { [<ParquetDateTime(Unit = TimeUnit.Nanoseconds)>] Field1: DateTime }
    type Output = { [<ParquetDateTimeOffset(Unit = TimeUnit.Nanoseconds)>] Field1: DateTimeOffset }

    [<Theory>]
    //[<InlineData(                  0L)>] // Min value
    //[<InlineData(                  1L)>] // Min value + 100ns
    [<InlineData( 621355967999999999L)>] // Unix epoch - 100ns
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000001L)>] // Unix epoch + 100ns
    [<InlineData( 638752524171234567L)>] // 15/02/2025 21:40:17.1234567
    //[<InlineData(3155378975999999999L)>] // Max value
    let ``value`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = DateTimeOffset(ticks, TimeSpan.Zero)
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ unit=nanoseconds; optional=true } deserialize`` =
    type Input = {
        [<ParquetDateTime(NestingLevel = 1, Unit = TimeUnit.Nanoseconds)>]
        Field1: DateTime option }

    type Output = {
        [<ParquetDateTimeOffset(Unit = TimeUnit.Nanoseconds, Optional = true)>]
        Field1: DateTimeOffset }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<DateTimeOffset>}'" @>)

    [<Theory>]
    //[<InlineData(                  0L)>] // Min value
    //[<InlineData(                  1L)>] // Min value + 100ns
    [<InlineData( 621355967999999999L)>] // Unix epoch - 100ns
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000001L)>] // Unix epoch + 100ns
    [<InlineData( 638752524171234567L)>] // 15/02/2025 21:40:17.1234567
    //[<InlineData(3155378975999999999L)>] // Max value
    let ``non-null`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = DateTimeOffset(ticks, TimeSpan.Zero)
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
