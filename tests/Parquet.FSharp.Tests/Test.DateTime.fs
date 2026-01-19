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
    [<InlineData(                  0L)>] // Min value
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
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
    [<InlineData(                  0L)>] // Min value
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
    let ``utc kind`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isTimestamp true "microseconds"
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = value @>
        test <@ outputRecord.Field1.Kind = DateTimeKind.Utc @>

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
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

module ``deserialize date time from required int96`` =
    type Input = { Field1: DateTime }
    type Output = { Field1: DateTime }

    [<Theory>]
    [<InlineData(                  0L)>] // Min value
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
    let ``value`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = value @>
        test <@ outputRecord.Field1.Kind = DateTimeKind.Utc @>

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
    [<InlineData(                  0L)>] // Min value
    [<InlineData( 621355968000000000L)>] // Unix epoch
    [<InlineData( 638752524171234560L)>] // 15/02/2025 21:40:17.123456
    [<InlineData(3155378975999999990L)>] // Max value (truncated to micros)
    let ``non-null value`` (ticks: int64) =
        let value = DateTime(ticks, DateTimeKind.Utc)
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = value @>
        test <@ outputRecord.Field1.Kind = DateTimeKind.Utc @>
