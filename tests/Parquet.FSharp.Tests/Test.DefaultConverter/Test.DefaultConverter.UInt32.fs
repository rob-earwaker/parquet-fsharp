namespace Parquet.FSharp.Tests.DefaultConverter.UInt32

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize uint32`` =
    type Input = { Field1: uint32 }
    type Output = { Field1: uint32 }

    [<Theory>]
    [<InlineData(UInt32.MinValue)>]
    [<InlineData(             1u)>]
    [<InlineData(UInt32.MaxValue)>]
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
                Assert.Field.LogicalType.isInteger 32 false
                Assert.Field.ConvertedType.isUInt32
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize uint32 from required uint32`` =
    type Input = { Field1: uint32 }
    type Output = { Field1: uint32 }

    [<Theory>]
    [<InlineData(UInt32.MinValue)>]
    [<InlineData(             1u)>]
    [<InlineData(UInt32.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize uint32 from optional uint32`` =
    type Input = { Field1: uint32 option }
    type Output = { Field1: uint32 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<uint32>.FullName}'" @>)

    [<Theory>]
    [<InlineData(UInt32.MinValue)>]
    [<InlineData(             1u)>]
    [<InlineData(UInt32.MaxValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize uint32 from required uint16`` =
    type Input = { Field1: uint16 }
    type Output = { Field1: uint32 }

    [<Theory>]
    [<InlineData((* inputValue *) UInt16.MinValue, (* outputValue *)     0u)>]
    [<InlineData((* inputValue *)             1us, (* outputValue *)     1u)>]
    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535u)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize uint32 from optional uint16`` =
    type Input = { Field1: uint16 option }
    type Output = { Field1: uint32 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<uint32>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) UInt16.MinValue, (* outputValue *)     0u)>]
    [<InlineData((* inputValue *)             1us, (* outputValue *)     1u)>]
    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535u)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize uint32 from required uint8`` =
    type Input = { Field1: uint8 }
    type Output = { Field1: uint32 }

    [<Theory>]
    [<InlineData((* inputValue *) Byte.MinValue, (* outputValue *)   0u)>]
    [<InlineData((* inputValue *)           1uy, (* outputValue *)   1u)>]
    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255u)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize uint32 from optional uint8`` =
    type Input = { Field1: uint8 option }
    type Output = { Field1: uint32 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<uint32>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) Byte.MinValue, (* outputValue *)   0u)>]
    [<InlineData((* inputValue *)           1uy, (* outputValue *)   1u)>]
    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255u)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
