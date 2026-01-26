namespace Parquet.FSharp.Tests.UInt64

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize uint64`` =
    type Input = { Field1: uint64 }
    type Output = { Field1: uint64 }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isInteger 64 false
                Assert.Field.ConvertedType.isUInt64
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData(UInt64.MinValue)>]
    [<InlineData(            1UL)>]
    [<InlineData(UInt64.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize uint64 from required uint64`` =
    type Input = { Field1: uint64 }
    type Output = { Field1: uint64 }

    [<Theory>]
    [<InlineData(UInt64.MinValue)>]
    [<InlineData(            1UL)>]
    [<InlineData(UInt64.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize uint64 from optional uint64`` =
    type Input = { Field1: uint64 option }
    type Output = { Field1: uint64 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<uint64>.FullName}'" @>)

    [<Theory>]
    [<InlineData(UInt64.MinValue)>]
    [<InlineData(            1UL)>]
    [<InlineData(UInt64.MaxValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize uint64 from required uint32`` =
    type Input = { Field1: uint32 }
    type Output = { Field1: uint64 }

    [<Theory>]
    [<InlineData((* inputValue *) UInt32.MinValue, (* outputValue *)          0UL)>]
    [<InlineData((* inputValue *)              1u, (* outputValue *)          1UL)>]
    [<InlineData((* inputValue *) UInt32.MaxValue, (* outputValue *) 4294967295UL)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize uint64 from optional uint32`` =
    type Input = { Field1: uint32 option }
    type Output = { Field1: uint64 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<uint64>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) UInt32.MinValue, (* outputValue *)          0UL)>]
    [<InlineData((* inputValue *)              1u, (* outputValue *)          1UL)>]
    [<InlineData((* inputValue *) UInt32.MaxValue, (* outputValue *) 4294967295UL)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize uint64 from required uint16`` =
    type Input = { Field1: uint16 }
    type Output = { Field1: uint64 }

    [<Theory>]
    [<InlineData((* inputValue *) UInt16.MinValue, (* outputValue *)     0UL)>]
    [<InlineData((* inputValue *)             1us, (* outputValue *)     1UL)>]
    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535UL)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize uint64 from optional uint16`` =
    type Input = { Field1: uint16 option }
    type Output = { Field1: uint64 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<uint64>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) UInt16.MinValue, (* outputValue *)     0UL)>]
    [<InlineData((* inputValue *)             1us, (* outputValue *)     1UL)>]
    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535UL)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize uint64 from required uint8`` =
    type Input = { Field1: uint8 }
    type Output = { Field1: uint64 }

    [<Theory>]
    [<InlineData((* inputValue *) Byte.MinValue, (* outputValue *)   0UL)>]
    [<InlineData((* inputValue *)           1uy, (* outputValue *)   1UL)>]
    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255UL)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize uint64 from optional uint8`` =
    type Input = { Field1: uint8 option }
    type Output = { Field1: uint64 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<uint64>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) Byte.MinValue, (* outputValue *)   0UL)>]
    [<InlineData((* inputValue *)           1uy, (* outputValue *)   1UL)>]
    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255UL)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
