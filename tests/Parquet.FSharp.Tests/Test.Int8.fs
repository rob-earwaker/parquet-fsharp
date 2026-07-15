namespace Parquet.FSharp.Tests.Int8

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``{ default } serialize`` =
    type Input = { Field1: int8 }
    type Output = { Field1: int8 }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 8 true
                Assert.Field.ConvertedType.isInt8
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData(SByte.MinValue)>]
    [<InlineData(           -1y)>]
    [<InlineData(            0y)>]
    [<InlineData(            1y)>]
    [<InlineData(SByte.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=false } serialize`` =
    type Input = { [<ParquetInt8(Optional = false)>] Field1: int8 }
    type Output = { Field1: int8 }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 8 true
                Assert.Field.ConvertedType.isInt8
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData(SByte.MinValue)>]
    [<InlineData(           -1y)>]
    [<InlineData(            0y)>]
    [<InlineData(            1y)>]
    [<InlineData(SByte.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } serialize`` =
    type Input = { [<ParquetInt8(Optional = true)>] Field1: int8 }
    type Output = { Field1: int8 option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 8 true
                Assert.Field.ConvertedType.isInt8
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData(SByte.MinValue)>]
    [<InlineData(           -1y)>]
    [<InlineData(            0y)>]
    [<InlineData(            1y)>]
    [<InlineData(SByte.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ default } deserialize`` =
    type Input = { Field1: int8 }
    type Output = { Field1: int8 }

    [<Theory>]
    [<InlineData(SByte.MinValue)>]
    [<InlineData(           -1y)>]
    [<InlineData(            0y)>]
    [<InlineData(            1y)>]
    [<InlineData(SByte.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=false } deserialize`` =
    type Input = { Field1: int8 }
    type Output = { [<ParquetInt8(Optional = false)>] Field1: int8 }

    [<Theory>]
    [<InlineData(SByte.MinValue)>]
    [<InlineData(           -1y)>]
    [<InlineData(            0y)>]
    [<InlineData(            1y)>]
    [<InlineData(SByte.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } deserialize`` =
    type Input = { Field1: int8 option }
    type Output = { [<ParquetInt8(Optional = true)>] Field1: int8 }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<int8>}'" @>)

    [<Theory>]
    [<InlineData(SByte.MinValue)>]
    [<InlineData(           -1y)>]
    [<InlineData(            0y)>]
    [<InlineData(            1y)>]
    [<InlineData(SByte.MaxValue)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
