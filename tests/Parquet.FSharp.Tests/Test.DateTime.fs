namespace Parquet.FSharp.Tests.DateTime

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

// TODO: Add min/max tests for nanoseconds - bug in Parquet.Net? Document in README!

module ``{ default } serialize`` =
    type Input = { Field1: DateTime }
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
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``unspecified kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Unspecified)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Unspecified'"
                    + " during serialization of timestamp with instant"
                    + " semantics which only allows 'DateTimeKind.Utc' by"
                    + " default" @>)

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``utc kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``local kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Local)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Local' during"
                    + " serialization of timestamp with instant semantics which"
                    + " only allows 'DateTimeKind.Utc' by default" @>)

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(              10000L)>] // Min value + 1ms
    [<InlineData( 621355967999990000L)>] // Unix epoch - 1ms
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000010000L)>] // Unix epoch + 1ms
    [<InlineData( 638752524171230000L)>] // 15/02/2025 21:40:17.123
    [<InlineData(3155378975999990000L)>] // Max value (truncated to millis)
    let ``millisecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                 10L)>] // Min value + 1us
    [<InlineData( 621355967999999990L)>] // Unix epoch - 1us
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000010L)>] // Unix epoch + 1us
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
    let ``microsecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                  1L)>] // Min value + 100ns
    [<InlineData( 621355967999999999L)>] // Unix epoch - 100ns
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000001L)>] // Unix epoch + 100ns
    [<InlineData( 638752524171234567L)>] // 15/02/2025 21:40:17.1234567
    [<InlineData(3155378975999999999L)>] // Max value
    let ``nanosecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to be truncated to microsecond precision.
        let expectedValue = value.AddTicks(-(value.Ticks % TimeSpan.TicksPerMicrosecond))
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

module ``{ unit=milliseconds } serialize`` =
    type Input = { [<ParquetDateTime(Unit = TimeUnit.Milliseconds)>] Field1: DateTime }
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
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``unspecified kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Unspecified)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Unspecified'"
                    + " during serialization of timestamp with instant"
                    + " semantics which only allows 'DateTimeKind.Utc' by"
                    + " default" @>)

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``utc kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``local kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Local)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Local' during"
                    + " serialization of timestamp with instant semantics which"
                    + " only allows 'DateTimeKind.Utc' by default" @>)

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(              10000L)>] // Min value + 1ms
    [<InlineData( 621355967999990000L)>] // Unix epoch - 1ms
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000010000L)>] // Unix epoch + 1ms
    [<InlineData( 638752524171230000L)>] // 15/02/2025 21:40:17.123
    [<InlineData(3155378975999990000L)>] // Max value (truncated to millis)
    let ``millisecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                 10L)>] // Min value + 1us
    [<InlineData( 621355967999999990L)>] // Unix epoch - 1us
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000010L)>] // Unix epoch + 1us
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
    let ``microsecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to be truncated to millisecond precision.
        let expectedValue = value.AddTicks(-(value.Ticks % TimeSpan.TicksPerMillisecond))
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                  1L)>] // Min value + 100ns
    [<InlineData( 621355967999999999L)>] // Unix epoch - 100ns
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000001L)>] // Unix epoch + 100ns
    [<InlineData( 638752524171234567L)>] // 15/02/2025 21:40:17.1234567
    [<InlineData(3155378975999999999L)>] // Max value
    let ``nanosecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to be truncated to millisecond precision.
        let expectedValue = value.AddTicks(-(value.Ticks % TimeSpan.TicksPerMillisecond))
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

module ``{ unit=microseconds } serialize`` =
    type Input = { [<ParquetDateTime(Unit = TimeUnit.Microseconds)>] Field1: DateTime }
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
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``unspecified kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Unspecified)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Unspecified'"
                    + " during serialization of timestamp with instant"
                    + " semantics which only allows 'DateTimeKind.Utc' by"
                    + " default" @>)

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``utc kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``local kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Local)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Local' during"
                    + " serialization of timestamp with instant semantics which"
                    + " only allows 'DateTimeKind.Utc' by default" @>)

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(              10000L)>] // Min value + 1ms
    [<InlineData( 621355967999990000L)>] // Unix epoch - 1ms
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000010000L)>] // Unix epoch + 1ms
    [<InlineData( 638752524171230000L)>] // 15/02/2025 21:40:17.123
    [<InlineData(3155378975999990000L)>] // Max value (truncated to millis)
    let ``millisecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                 10L)>] // Min value + 1us
    [<InlineData( 621355967999999990L)>] // Unix epoch - 1us
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000010L)>] // Unix epoch + 1us
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
    let ``microsecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                  1L)>] // Min value + 100ns
    [<InlineData( 621355967999999999L)>] // Unix epoch - 100ns
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000001L)>] // Unix epoch + 100ns
    [<InlineData( 638752524171234567L)>] // 15/02/2025 21:40:17.1234567
    [<InlineData(3155378975999999999L)>] // Max value
    let ``nanosecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to be truncated to microsecond precision.
        let expectedValue = value.AddTicks(-(value.Ticks % TimeSpan.TicksPerMicrosecond))
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

module ``{ unit=nanoseconds } serialize`` =
    type Input = { [<ParquetDateTime(Unit = TimeUnit.Nanoseconds)>] Field1: DateTime }
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
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``unspecified kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Unspecified)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Unspecified'"
                    + " during serialization of timestamp with instant"
                    + " semantics which only allows 'DateTimeKind.Utc' by"
                    + " default" @>)

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``utc kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``local kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Local)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Local' during"
                    + " serialization of timestamp with instant semantics which"
                    + " only allows 'DateTimeKind.Utc' by default" @>)

    [<Theory>]
    //[<InlineData(                  0L)>] // Min value
    //[<InlineData(              10000L)>] // Min value + 1ms
    [<InlineData( 621355967999990000L)>] // Unix epoch - 1ms
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000010000L)>] // Unix epoch + 1ms
    [<InlineData( 638752524171230000L)>] // 15/02/2025 21:40:17.123
    //[<InlineData(3155378975999990000L)>] // Max value (truncated to millis)
    let ``millisecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    //[<InlineData(                  0L)>] // Min value
    //[<InlineData(                 10L)>] // Min value + 1us
    [<InlineData( 621355967999999990L)>] // Unix epoch - 1us
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000010L)>] // Unix epoch + 1us
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    //[<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
    let ``microsecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    //[<InlineData(                  0L)>] // Min value
    //[<InlineData(                  1L)>] // Min value + 100ns
    [<InlineData( 621355967999999999L)>] // Unix epoch - 100ns
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000001L)>] // Unix epoch + 100ns
    [<InlineData( 638752524171234567L)>] // 15/02/2025 21:40:17.1234567
    //[<InlineData(3155378975999999999L)>] // Max value
    let ``nanosecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

module ``{ local=false } serialize`` =
    type Input = { [<ParquetDateTime(Local = false)>] Field1: DateTime }
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
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``unspecified kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Unspecified)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Unspecified'"
                    + " during serialization of timestamp with instant"
                    + " semantics which only allows 'DateTimeKind.Utc' by"
                    + " default" @>)

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``utc kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``local kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Local)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Local' during"
                    + " serialization of timestamp with instant semantics which"
                    + " only allows 'DateTimeKind.Utc' by default" @>)

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(              10000L)>] // Min value + 1ms
    [<InlineData( 621355967999990000L)>] // Unix epoch - 1ms
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000010000L)>] // Unix epoch + 1ms
    [<InlineData( 638752524171230000L)>] // 15/02/2025 21:40:17.123
    [<InlineData(3155378975999990000L)>] // Max value (truncated to millis)
    let ``millisecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                 10L)>] // Min value + 1us
    [<InlineData( 621355967999999990L)>] // Unix epoch - 1us
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000010L)>] // Unix epoch + 1us
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
    let ``microsecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                  1L)>] // Min value + 100ns
    [<InlineData( 621355967999999999L)>] // Unix epoch - 100ns
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000001L)>] // Unix epoch + 100ns
    [<InlineData( 638752524171234567L)>] // 15/02/2025 21:40:17.1234567
    [<InlineData(3155378975999999999L)>] // Max value
    let ``nanosecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to be truncated to microsecond precision.
        let expectedValue = value.AddTicks(-(value.Ticks % TimeSpan.TicksPerMicrosecond))
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

module ``{ local=true } serialize`` =
    type Input = { [<ParquetDateTime(Local = true)>] Field1: DateTime }
    type Output = { [<ParquetDateTime(Local = true)>] Field1: DateTime }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isTimestamp "local" "microseconds"
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``unspecified kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Unspecified)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Unspecified'"
                    + " during serialization of timestamp with local"
                    + " semantics which only allows 'DateTimeKind.Local' by"
                    + " default" @>)

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``utc kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Utc'"
                    + " during serialization of timestamp with local"
                    + " semantics which only allows 'DateTimeKind.Local' by"
                    + " default" @>)

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``local kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Local)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Local @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(              10000L)>] // Min value + 1ms
    [<InlineData( 621355967999990000L)>] // Unix epoch - 1ms
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000010000L)>] // Unix epoch + 1ms
    [<InlineData( 638752524171230000L)>] // 15/02/2025 21:40:17.123
    [<InlineData(3155378975999990000L)>] // Max value (truncated to millis)
    let ``millisecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Local)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Local @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                 10L)>] // Min value + 1us
    [<InlineData( 621355967999999990L)>] // Unix epoch - 1us
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000010L)>] // Unix epoch + 1us
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
    let ``microsecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Local)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Local @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                  1L)>] // Min value + 100ns
    [<InlineData( 621355967999999999L)>] // Unix epoch - 100ns
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000001L)>] // Unix epoch + 100ns
    [<InlineData( 638752524171234567L)>] // 15/02/2025 21:40:17.1234567
    [<InlineData(3155378975999999999L)>] // Max value
    let ``nanosecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Local)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to be truncated to microsecond precision.
        let expectedValue = value.AddTicks(-(value.Ticks % TimeSpan.TicksPerMicrosecond))
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Local @>

module ``{ optional=false } serialize`` =
    type Input = { [<ParquetDateTime(Optional = false)>] Field1: DateTime }
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
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``unspecified kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Unspecified)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Unspecified'"
                    + " during serialization of timestamp with instant"
                    + " semantics which only allows 'DateTimeKind.Utc' by"
                    + " default" @>)

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``utc kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``local kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Local)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Local' during"
                    + " serialization of timestamp with instant semantics which"
                    + " only allows 'DateTimeKind.Utc' by default" @>)

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(              10000L)>] // Min value + 1ms
    [<InlineData( 621355967999990000L)>] // Unix epoch - 1ms
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000010000L)>] // Unix epoch + 1ms
    [<InlineData( 638752524171230000L)>] // 15/02/2025 21:40:17.123
    [<InlineData(3155378975999990000L)>] // Max value (truncated to millis)
    let ``millisecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                 10L)>] // Min value + 1us
    [<InlineData( 621355967999999990L)>] // Unix epoch - 1us
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000010L)>] // Unix epoch + 1us
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
    let ``microsecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                  1L)>] // Min value + 100ns
    [<InlineData( 621355967999999999L)>] // Unix epoch - 100ns
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000001L)>] // Unix epoch + 100ns
    [<InlineData( 638752524171234567L)>] // 15/02/2025 21:40:17.1234567
    [<InlineData(3155378975999999999L)>] // Max value
    let ``nanosecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to be truncated to microsecond precision.
        let expectedValue = value.AddTicks(-(value.Ticks % TimeSpan.TicksPerMicrosecond))
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

module ``{ optional=true } serialize`` =
    type Input = { [<ParquetDateTime(Optional = true)>] Field1: DateTime }
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
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``unspecified kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Unspecified)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Unspecified'"
                    + " during serialization of timestamp with instant"
                    + " semantics which only allows 'DateTimeKind.Utc' by"
                    + " default" @>)

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``utc kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Value.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(621355968000000000L)>] // Unix epoch
    [<InlineData(638752524170000000L)>] // 15/02/2025 21:40:17
    let ``local kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Local)
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "encountered 'DateTime' with 'DateTimeKind.Local' during"
                    + " serialization of timestamp with instant semantics which"
                    + " only allows 'DateTimeKind.Utc' by default" @>)

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(              10000L)>] // Min value + 1ms
    [<InlineData( 621355967999990000L)>] // Unix epoch - 1ms
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000010000L)>] // Unix epoch + 1ms
    [<InlineData( 638752524171230000L)>] // 15/02/2025 21:40:17.123
    [<InlineData(3155378975999990000L)>] // Max value (truncated to millis)
    let ``millisecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Value.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                 10L)>] // Min value + 1us
    [<InlineData( 621355967999999990L)>] // Unix epoch - 1us
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000010L)>] // Unix epoch + 1us
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
    let ``microsecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip, i.e. no truncation.
        let expectedValue = value
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Value.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                  1L)>] // Min value + 100ns
    [<InlineData( 621355967999999999L)>] // Unix epoch - 100ns
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000001L)>] // Unix epoch + 100ns
    [<InlineData( 638752524171234567L)>] // 15/02/2025 21:40:17.1234567
    [<InlineData(3155378975999999999L)>] // Max value
    let ``nanosecond precision`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to be truncated to microsecond precision.
        let expectedValue = value.AddTicks(-(value.Ticks % TimeSpan.TicksPerMicrosecond))
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Value.Kind = DateTimeKind.Utc @>

module ``{ default } deserialize`` =
    type Input = { Field1: DateTime }
    type Output = { Field1: DateTime }

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
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

module ``{ unit=milliseconds } deserialize`` =
    type Input = { [<ParquetDateTime(Unit = TimeUnit.Milliseconds)>] Field1: DateTime }
    type Output = { [<ParquetDateTime(Unit = TimeUnit.Milliseconds)>] Field1: DateTime }

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
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

module ``{ unit=microseconds } deserialize`` =
    type Input = { Field1: DateTime }
    type Output = { [<ParquetDateTime(Unit = TimeUnit.Microseconds)>] Field1: DateTime }

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
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

module ``{ unit=nanoseconds } deserialize`` =
    type Input = { [<ParquetDateTime(Unit = TimeUnit.Nanoseconds)>] Field1: DateTime }
    type Output = { [<ParquetDateTime(Unit = TimeUnit.Nanoseconds)>] Field1: DateTime }

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
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

module ``{ local=false } deserialize`` =
    type Input = { Field1: DateTime }
    type Output = { [<ParquetDateTime(Local = false)>] Field1: DateTime }

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
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

module ``{ local=true } deserialize`` =
    type Input = { [<ParquetDateTime(Local = true)>] Field1: DateTime }
    type Output = { [<ParquetDateTime(Local = true)>] Field1: DateTime }

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData(                 10L)>] // Min value + 1us
    [<InlineData( 621355967999999990L)>] // Unix epoch - 1us
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 621355968000000010L)>] // Unix epoch + 1us
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
    let ``value`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Local)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Local @>

module ``{ optional=false } deserialize`` =
    type Input = { Field1: DateTime }
    type Output = { [<ParquetDateTime(Optional = false)>] Field1: DateTime }

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
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>

module ``{ optional=true } deserialize`` =
    type Input = { Field1: DateTime option }
    type Output = { [<ParquetDateTime(Optional = true)>] Field1: DateTime }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<DateTime>}'" @>)

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
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords[0].Field1.Kind = DateTimeKind.Utc @>
