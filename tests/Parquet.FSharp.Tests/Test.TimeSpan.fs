namespace Parquet.FSharp.Tests.TimeSpan

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize time span`` =
    type Input = { Field1: TimeSpan }
    type Output = { Field1: int64 }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isInteger 64 true
                Assert.Field.ConvertedType.isInt64
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData(-922337203685477L)>] // Min value (truncated to millis)
    [<InlineData(       -86400001L)>] // -1.00:00:00.001
    [<InlineData(       -86400000L)>] // -1.00:00:00.000
    [<InlineData(       -86399999L)>] //   -23:59:59.999
    [<InlineData(       -78017123L)>] //   -21:40:17.123
    [<InlineData(              -1L)>] //   -00:00:00.001
    [<InlineData(               0L)>] //    00:00:00.000
    [<InlineData(               1L)>] //    00:00:00.001
    [<InlineData(        78017123L)>] //    21:40:17.123
    [<InlineData(        86399999L)>] //    23:59:59.999
    [<InlineData(        86400000L)>] //  1.00:00:00.000
    [<InlineData(        86400001L)>] //  1.00:00:00.001
    [<InlineData( 922337203685477L)>] // Max value (truncated to millis)
    let ``millisecond precision`` milliseconds =
        let value = TimeSpan(milliseconds * TimeSpan.TicksPerMillisecond)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip in microseconds without truncation.
        let expectedValue = milliseconds * 1000L
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

    [<Theory>]
    [<InlineData(-922337203685477580L)>] // Min value (truncated to micros)
    [<InlineData(       -86400000001L)>] // -1.00:00:00.000001
    [<InlineData(       -86400000000L)>] // -1.00:00:00.000000
    [<InlineData(       -86399999999L)>] //   -23:59:59.999999
    [<InlineData(       -78017123456L)>] //   -21:40:17.123456
    [<InlineData(                 -1L)>] //   -00:00:00.000001
    [<InlineData(                  0L)>] //    00:00:00.000000
    [<InlineData(                  1L)>] //    00:00:00.000001
    [<InlineData(        78017123456L)>] //    21:40:17.123456
    [<InlineData(        86399999999L)>] //    23:59:59.999999
    [<InlineData(        86400000000L)>] //  1.00:00:00.000000
    [<InlineData(        86400000001L)>] //  1.00:00:00.000001
    [<InlineData( 922337203685477580L)>] // Max value (truncated to micros)
    let ``microsecond precision`` microseconds =
        let value = TimeSpan(microseconds * TimeSpan.TicksPerMicrosecond)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to roundtrip without truncation.
        let expectedValue = microseconds
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

    [<Theory>]
    [<InlineData(-9223372036854775808L)>] // Min value
    [<InlineData(-9223372036854775807L)>] // Min value + 100ns
    [<InlineData(       -864000000001L)>] // -1.00:00:00.0000001
    [<InlineData(       -864000000000L)>] // -1.00:00:00.0000000
    [<InlineData(       -863999999999L)>] //   -23:59:59.9999999
    [<InlineData(       -780171234567L)>] //   -21:40:17.1234567
    [<InlineData(                  -1L)>] //   -00:00:00.0000001
    [<InlineData(                   0L)>] //    00:00:00.0000000
    [<InlineData(                   1L)>] //    00:00:00.0000001
    [<InlineData(        780171234567L)>] //    21:40:17.1234567
    [<InlineData(        863999999999L)>] //    23:59:59.9999999
    [<InlineData(        864000000000L)>] //  1.00:00:00.0000000
    [<InlineData(        864000000001L)>] //  1.00:00:00.0000001
    [<InlineData( 9223372036854775806L)>] // Max value - 100ns
    [<InlineData( 9223372036854775807L)>] // Max value
    let ``nanosecond precision`` ticks =
        let value = TimeSpan(ticks)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Expect the value to be truncated to microseconds.
        let expectedValue = ticks / TimeSpan.TicksPerMicrosecond
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``deserialize time span from required int64`` =
    type Input = { Field1: int64 }
    type Output = { Field1: TimeSpan }

    [<Theory>]
    [<InlineData(-922337203685477580L)>] // Min value (truncated to micros)
    [<InlineData(       -86400000001L)>] // -1.00:00:00.000001
    [<InlineData(       -86400000000L)>] // -1.00:00:00.000000
    [<InlineData(       -86399999999L)>] //   -23:59:59.999999
    [<InlineData(       -78017123456L)>] //   -21:40:17.123456
    [<InlineData(                 -1L)>] //   -00:00:00.000001
    [<InlineData(                  0L)>] //    00:00:00.000000
    [<InlineData(                  1L)>] //    00:00:00.000001
    [<InlineData(        78017123456L)>] //    21:40:17.123456
    [<InlineData(        86399999999L)>] //    23:59:59.999999
    [<InlineData(        86400000000L)>] //  1.00:00:00.000000
    [<InlineData(        86400000001L)>] //  1.00:00:00.000001
    [<InlineData( 922337203685477580L)>] // Max value (truncated to micros)
    let ``value`` microseconds =
        let inputRecords = [| { Input.Field1 = microseconds } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = TimeSpan(microseconds * TimeSpan.TicksPerMicrosecond)
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``deserialize time span from optional int64`` =
    type Input = { Field1: int64 option }
    type Output = { Field1: TimeSpan }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<TimeSpan>}'" @>)

    [<Theory>]
    [<InlineData(-922337203685477580L)>] // Min value (truncated to micros)
    [<InlineData(       -86400000001L)>] // -1.00:00:00.000001
    [<InlineData(       -86400000000L)>] // -1.00:00:00.000000
    [<InlineData(       -86399999999L)>] //   -23:59:59.999999
    [<InlineData(       -78017123456L)>] //   -21:40:17.123456
    [<InlineData(                 -1L)>] //   -00:00:00.000001
    [<InlineData(                  0L)>] //    00:00:00.000000
    [<InlineData(                  1L)>] //    00:00:00.000001
    [<InlineData(        78017123456L)>] //    21:40:17.123456
    [<InlineData(        86399999999L)>] //    23:59:59.999999
    [<InlineData(        86400000000L)>] //  1.00:00:00.000000
    [<InlineData(        86400000001L)>] //  1.00:00:00.000001
    [<InlineData( 922337203685477580L)>] // Max value (truncated to micros)
    let ``non-null`` microseconds =
        let inputRecords = [| { Input.Field1 = Option.Some microseconds } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = TimeSpan(microseconds * TimeSpan.TicksPerMicrosecond)
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
