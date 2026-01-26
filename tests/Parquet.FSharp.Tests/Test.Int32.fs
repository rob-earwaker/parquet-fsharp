namespace Parquet.FSharp.Tests.Int32

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize int32`` =
    type Input = { Field1: int32 }
    type Output = { Field1: int32 }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 32 true
                Assert.Field.ConvertedType.isInt32
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData(Int32.MinValue)>]
    [<InlineData(            -1)>]
    [<InlineData(             0)>]
    [<InlineData(             1)>]
    [<InlineData(Int32.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize int32 from required int32`` =
    type Input = { Field1: int32 }
    type Output = { Field1: int32 }

    [<Theory>]
    [<InlineData(Int32.MinValue)>]
    [<InlineData(            -1)>]
    [<InlineData(             0)>]
    [<InlineData(             1)>]
    [<InlineData(Int32.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize int32 from optional int32`` =
    type Input = { Field1: int32 option }
    type Output = { Field1: int32 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<int32>.FullName}'" @>)

    [<Theory>]
    [<InlineData(Int32.MinValue)>]
    [<InlineData(            -1)>]
    [<InlineData(             0)>]
    [<InlineData(             1)>]
    [<InlineData(Int32.MaxValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize int32 from required int16`` =
    type Input = { Field1: int16 }
    type Output = { Field1: int32 }

    [<Theory>]
    [<InlineData((* inputValue *) Int16.MinValue, (* outputValue *) -32768)>]
    [<InlineData((* inputValue *)            -1s, (* outputValue *)     -1)>]
    [<InlineData((* inputValue *)             0s, (* outputValue *)      0)>]
    [<InlineData((* inputValue *)             1s, (* outputValue *)      1)>]
    [<InlineData((* inputValue *) Int16.MaxValue, (* outputValue *)  32767)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int32 from optional int16`` =
    type Input = { Field1: int16 option }
    type Output = { Field1: int32 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<int32>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) Int16.MinValue, (* outputValue *) -32768)>]
    [<InlineData((* inputValue *)            -1s, (* outputValue *)     -1)>]
    [<InlineData((* inputValue *)             0s, (* outputValue *)      0)>]
    [<InlineData((* inputValue *)             1s, (* outputValue *)      1)>]
    [<InlineData((* inputValue *) Int16.MaxValue, (* outputValue *)  32767)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int32 from required int8`` =
    type Input = { Field1: int8 }
    type Output = { Field1: int32 }

    [<Theory>]
    [<InlineData((* inputValue *) SByte.MinValue, (* outputValue *) -128)>]
    [<InlineData((* inputValue *)            -1y, (* outputValue *)   -1)>]
    [<InlineData((* inputValue *)             0y, (* outputValue *)    0)>]
    [<InlineData((* inputValue *)             1y, (* outputValue *)    1)>]
    [<InlineData((* inputValue *) SByte.MaxValue, (* outputValue *)  127)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int32 from optional int8`` =
    type Input = { Field1: int8 option }
    type Output = { Field1: int32 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<int32>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) SByte.MinValue, (* outputValue *) -128)>]
    [<InlineData((* inputValue *)            -1y, (* outputValue *)   -1)>]
    [<InlineData((* inputValue *)             0y, (* outputValue *)    0)>]
    [<InlineData((* inputValue *)             1y, (* outputValue *)    1)>]
    [<InlineData((* inputValue *) SByte.MaxValue, (* outputValue *)  127)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int32 from required uint16`` =
    type Input = { Field1: uint16 }
    type Output = { Field1: int32 }

    [<Theory>]
    [<InlineData((* inputValue *) UInt16.MinValue, (* outputValue *)     0)>]
    [<InlineData((* inputValue *)             1us, (* outputValue *)     1)>]
    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int32 from optional uint16`` =
    type Input = { Field1: uint16 option }
    type Output = { Field1: int32 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<int32>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) UInt16.MinValue, (* outputValue *)     0)>]
    [<InlineData((* inputValue *)             1us, (* outputValue *)     1)>]
    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int32 from required uint8`` =
    type Input = { Field1: uint8 }
    type Output = { Field1: int32 }

    [<Theory>]
    [<InlineData((* inputValue *) Byte.MinValue, (* outputValue *)   0)>]
    [<InlineData((* inputValue *)           1uy, (* outputValue *)   1)>]
    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int32 from optional uint8`` =
    type Input = { Field1: uint8 option }
    type Output = { Field1: int32 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<int32>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) Byte.MinValue, (* outputValue *)   0)>]
    [<InlineData((* inputValue *)           1uy, (* outputValue *)   1)>]
    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
