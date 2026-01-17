namespace Parquet.FSharp.Tests.UInt16

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize uint16`` =
    type Input = { Field1: uint16 }
    type Output = { Field1: uint16 }

    [<Theory>]
    [<InlineData(UInt16.MinValue)>]
    [<InlineData(            1us)>]
    [<InlineData(UInt16.MaxValue)>]
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
                Assert.Field.LogicalType.isInteger 16 false
                Assert.Field.ConvertedType.isUInt16
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize uint16 from required uint16`` =
    type Input = { Field1: uint16 }
    type Output = { Field1: uint16 }

    [<Theory>]
    [<InlineData(UInt16.MinValue)>]
    [<InlineData(            1us)>]
    [<InlineData(UInt16.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize uint16 from optional uint16`` =
    type Input = { Field1: uint16 option }
    type Output = { Field1: uint16 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<uint16>.FullName}'" @>)

    [<Theory>]
    [<InlineData(UInt16.MinValue)>]
    [<InlineData(            1us)>]
    [<InlineData(UInt16.MaxValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize uint16 from required uint8`` =
    type Input = { Field1: uint8 }
    type Output = { Field1: uint16 }

    [<Theory>]
    [<InlineData((* inputValue *) Byte.MinValue, (* outputValue *)   0us)>]
    [<InlineData((* inputValue *)           1uy, (* outputValue *)   1us)>]
    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255us)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize uint16 from optional uint8`` =
    type Input = { Field1: uint8 option }
    type Output = { Field1: uint16 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<uint16>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) Byte.MinValue, (* outputValue *)   0us)>]
    [<InlineData((* inputValue *)           1uy, (* outputValue *)   1us)>]
    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255us)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
