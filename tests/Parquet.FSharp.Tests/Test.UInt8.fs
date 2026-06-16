namespace Parquet.FSharp.Tests.UInt8

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``{ default } serialize`` =
    type Input = { Field1: uint8 }
    type Output = { Field1: uint8 }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 8 false
                Assert.Field.ConvertedType.isUInt8
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData(Byte.MinValue)>]
    [<InlineData(          1uy)>]
    [<InlineData(Byte.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=false } serialize`` =
    type Input = { [<ParquetUInt8(Optional = false)>] Field1: uint8 }
    type Output = { Field1: uint8 }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 8 false
                Assert.Field.ConvertedType.isUInt8
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData(Byte.MinValue)>]
    [<InlineData(          1uy)>]
    [<InlineData(Byte.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } serialize`` =
    type Input = { [<ParquetUInt8(Optional = true)>] Field1: uint8 }
    type Output = { Field1: uint8 option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 8 false
                Assert.Field.ConvertedType.isUInt8
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData(Byte.MinValue)>]
    [<InlineData(          1uy)>]
    [<InlineData(Byte.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ default } deserialize`` =
    type Input = { Field1: uint8 }
    type Output = { Field1: uint8 }

    [<Theory>]
    [<InlineData(Byte.MinValue)>]
    [<InlineData(          1uy)>]
    [<InlineData(Byte.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=false } deserialize`` =
    type Input = { Field1: uint8 }
    type Output = { [<ParquetUInt8(Optional = false)>] Field1: uint8 }

    [<Theory>]
    [<InlineData(Byte.MinValue)>]
    [<InlineData(          1uy)>]
    [<InlineData(Byte.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } deserialize`` =
    type Input = { Field1: uint8 option }
    type Output = { [<ParquetUInt8(Optional = true)>] Field1: uint8 }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<uint8>}'" @>)

    [<Theory>]
    [<InlineData(Byte.MinValue)>]
    [<InlineData(          1uy)>]
    [<InlineData(Byte.MaxValue)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
