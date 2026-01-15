namespace Parquet.FSharp.Tests.Int8

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize int8`` =
    type Input = { Field1: int8 }
    type Output = { Field1: int8 }

    [<Theory>]
    [<InlineData(0y)>]
    [<InlineData(SByte.MinValue)>]
    [<InlineData(SByte.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInt 8 true
                Assert.Field.ConvertedType.isInt8
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize int8 option`` =
    type Input = { Field1: int8 option }
    type Output = { Field1: int8 option }

    [<Fact>]
    let ``none value`` () =
        let value = Option<int8>.None
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInt 8 true
                Assert.Field.ConvertedType.isInt8
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

    [<Theory>]
    [<InlineData(0y)>]
    [<InlineData(SByte.MinValue)>]
    [<InlineData(SByte.MaxValue)>]
    let ``some value`` value =
        let value = Option.Some value
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInt 8 true
                Assert.Field.ConvertedType.isInt8
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize nullable int8`` =
    type Input = { Field1: Nullable<int8> }
    type Output = { Field1: int8 option }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Nullable() } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInt 8 true
                Assert.Field.ConvertedType.isInt8
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Theory>]
    [<InlineData(0y)>]
    [<InlineData(SByte.MinValue)>]
    [<InlineData(SByte.MaxValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Nullable(value) } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInt 8 true
                Assert.Field.ConvertedType.isInt8
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``deserialize int8 from required int8`` =
    type Input = { Field1: int8 }
    type Output = { Field1: int8 }

    [<Theory>]
    [<InlineData(0y)>]
    [<InlineData(SByte.MinValue)>]
    [<InlineData(SByte.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize int8 from optional int8`` =
    type Input = { Field1: int8 option }
    type Output = { Field1: int8 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"null value encountered for non-nullable"
                    + $" type '{typeof<int8>.FullName}'" @>)

    [<Theory>]
    [<InlineData(0y)>]
    [<InlineData(SByte.MinValue)>]
    [<InlineData(SByte.MaxValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
