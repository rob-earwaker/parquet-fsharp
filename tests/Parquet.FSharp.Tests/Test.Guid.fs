namespace Parquet.FSharp.Tests.Guid

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``{ default } serialize`` =
    type Input = { Field1: Guid }
    type Output = { Field1: Guid }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isFixedLengthByteArray 16
                Assert.Field.LogicalType.isUuid
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData("00000000-0000-0000-0000-000000000000")>]
    [<InlineData("fe611b97-e924-47d0-8f2a-270c564b5df6")>]
    let ``value`` (value: string) =
        let value = Guid.Parse(value)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=false } serialize`` =
    type Input = { [<ParquetGuid(Optional = false)>] Field1: Guid }
    type Output = { Field1: Guid }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isFixedLengthByteArray 16
                Assert.Field.LogicalType.isUuid
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData("00000000-0000-0000-0000-000000000000")>]
    [<InlineData("fe611b97-e924-47d0-8f2a-270c564b5df6")>]
    let ``value`` (value: string) =
        let value = Guid.Parse(value)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } serialize`` =
    type Input = { [<ParquetGuid(Optional = true)>] Field1: Guid }
    type Output = { Field1: Guid option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isFixedLengthByteArray 16
                Assert.Field.LogicalType.isUuid
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData("00000000-0000-0000-0000-000000000000")>]
    [<InlineData("fe611b97-e924-47d0-8f2a-270c564b5df6")>]
    let ``value`` (value: string) =
        let value = Guid.Parse(value)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ default } deserialize`` =
    type Input = { Field1: Guid }
    type Output = { Field1: Guid }

    [<Theory>]
    [<InlineData("00000000-0000-0000-0000-000000000000")>]
    [<InlineData("fe611b97-e924-47d0-8f2a-270c564b5df6")>]
    let ``value`` (value: string) =
        let value = Guid.Parse(value)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=false } deserialize`` =
    type Input = { Field1: Guid }
    type Output = { [<ParquetGuid(Optional = false)>] Field1: Guid }

    [<Theory>]
    [<InlineData("00000000-0000-0000-0000-000000000000")>]
    [<InlineData("fe611b97-e924-47d0-8f2a-270c564b5df6")>]
    let ``value`` (value: string) =
        let value = Guid.Parse(value)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } deserialize`` =
    type Input = { Field1: Guid option }
    type Output = { [<ParquetGuid(Optional = true)>] Field1: Guid }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<Guid>}'" @>)

    [<Theory>]
    [<InlineData("00000000-0000-0000-0000-000000000000")>]
    [<InlineData("fe611b97-e924-47d0-8f2a-270c564b5df6")>]
    let ``non-null`` (value: string) =
        let value = Guid.Parse(value)
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
