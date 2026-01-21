namespace Parquet.FSharp.Tests.DefaultConverter.Int64

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize int64`` =
    type Input = { Field1: int64 }
    type Output = { Field1: int64 }

    [<Theory>]
    [<InlineData(Int64.MinValue)>]
    [<InlineData(           -1L)>]
    [<InlineData(            0L)>]
    [<InlineData(            1L)>]
    [<InlineData(Int64.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isInteger 64 true
                Assert.Field.ConvertedType.isInt64
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize int64 from required int64`` =
    type Input = { Field1: int64 }
    type Output = { Field1: int64 }

    [<Theory>]
    [<InlineData(Int64.MinValue)>]
    [<InlineData(           -1L)>]
    [<InlineData(            0L)>]
    [<InlineData(            1L)>]
    [<InlineData(Int64.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize int64 from optional int64`` =
    type Input = { Field1: int64 option }
    type Output = { Field1: int64 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<int64>.FullName}'" @>)

    [<Theory>]
    [<InlineData(Int64.MinValue)>]
    [<InlineData(           -1L)>]
    [<InlineData(            0L)>]
    [<InlineData(            1L)>]
    [<InlineData(Int64.MaxValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize int64 from required int32`` =
    type Input = { Field1: int32 }
    type Output = { Field1: int64 }

    [<Theory>]
    [<InlineData((* inputValue *) Int32.MinValue, (* outputValue *) -2147483648L)>]
    [<InlineData((* inputValue *)             -1, (* outputValue *)          -1L)>]
    [<InlineData((* inputValue *)              0, (* outputValue *)           0L)>]
    [<InlineData((* inputValue *)              1, (* outputValue *)           1L)>]
    [<InlineData((* inputValue *) Int32.MaxValue, (* outputValue *)  2147483647L)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int64 from optional int32`` =
    type Input = { Field1: int32 option }
    type Output = { Field1: int64 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<int64>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) Int32.MinValue, (* outputValue *) -2147483648L)>]
    [<InlineData((* inputValue *)             -1, (* outputValue *)          -1L)>]
    [<InlineData((* inputValue *)              0, (* outputValue *)           0L)>]
    [<InlineData((* inputValue *)              1, (* outputValue *)           1L)>]
    [<InlineData((* inputValue *) Int32.MaxValue, (* outputValue *)  2147483647L)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int64 from required int16`` =
    type Input = { Field1: int16 }
    type Output = { Field1: int64 }

    [<Theory>]
    [<InlineData((* inputValue *) Int16.MinValue, (* outputValue *) -32768L)>]
    [<InlineData((* inputValue *)            -1s, (* outputValue *)     -1L)>]
    [<InlineData((* inputValue *)             0s, (* outputValue *)      0L)>]
    [<InlineData((* inputValue *)             1s, (* outputValue *)      1L)>]
    [<InlineData((* inputValue *) Int16.MaxValue, (* outputValue *)  32767L)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int64 from optional int16`` =
    type Input = { Field1: int16 option }
    type Output = { Field1: int64 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<int64>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) Int16.MinValue, (* outputValue *) -32768L)>]
    [<InlineData((* inputValue *)            -1s, (* outputValue *)     -1L)>]
    [<InlineData((* inputValue *)             0s, (* outputValue *)      0L)>]
    [<InlineData((* inputValue *)             1s, (* outputValue *)      1L)>]
    [<InlineData((* inputValue *) Int16.MaxValue, (* outputValue *)  32767L)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int64 from required int8`` =
    type Input = { Field1: int8 }
    type Output = { Field1: int64 }

    [<Theory>]
    [<InlineData((* inputValue *) SByte.MinValue, (* outputValue *) -128L)>]
    [<InlineData((* inputValue *)            -1y, (* outputValue *)   -1L)>]
    [<InlineData((* inputValue *)             0y, (* outputValue *)    0L)>]
    [<InlineData((* inputValue *)             1y, (* outputValue *)    1L)>]
    [<InlineData((* inputValue *) SByte.MaxValue, (* outputValue *)  127L)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int64 from optional int8`` =
    type Input = { Field1: int8 option }
    type Output = { Field1: int64 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<int64>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) SByte.MinValue, (* outputValue *) -128L)>]
    [<InlineData((* inputValue *)            -1y, (* outputValue *)   -1L)>]
    [<InlineData((* inputValue *)             0y, (* outputValue *)    0L)>]
    [<InlineData((* inputValue *)             1y, (* outputValue *)    1L)>]
    [<InlineData((* inputValue *) SByte.MaxValue, (* outputValue *)  127L)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int64 from required uint32`` =
    type Input = { Field1: uint32 }
    type Output = { Field1: int64 }

    [<Theory>]
    [<InlineData((* inputValue *) UInt32.MinValue, (* outputValue *)          0L)>]
    [<InlineData((* inputValue *)              1u, (* outputValue *)          1L)>]
    [<InlineData((* inputValue *) UInt32.MaxValue, (* outputValue *) 4294967295L)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int64 from optional uint32`` =
    type Input = { Field1: uint32 option }
    type Output = { Field1: int64 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<int64>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) UInt32.MinValue, (* outputValue *)          0L)>]
    [<InlineData((* inputValue *)              1u, (* outputValue *)          1L)>]
    [<InlineData((* inputValue *) UInt32.MaxValue, (* outputValue *) 4294967295L)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int64 from required uint16`` =
    type Input = { Field1: uint16 }
    type Output = { Field1: int64 }

    [<Theory>]
    [<InlineData((* inputValue *) UInt16.MinValue, (* outputValue *)     0L)>]
    [<InlineData((* inputValue *)             1us, (* outputValue *)     1L)>]
    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535L)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int64 from optional uint16`` =
    type Input = { Field1: uint16 option }
    type Output = { Field1: int64 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<int64>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) UInt16.MinValue, (* outputValue *)     0L)>]
    [<InlineData((* inputValue *)             1us, (* outputValue *)     1L)>]
    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535L)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int64 from required uint8`` =
    type Input = { Field1: uint8 }
    type Output = { Field1: int64 }

    [<Theory>]
    [<InlineData((* inputValue *) Byte.MinValue, (* outputValue *)   0L)>]
    [<InlineData((* inputValue *)           1uy, (* outputValue *)   1L)>]
    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255L)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize int64 from optional uint8`` =
    type Input = { Field1: uint8 option }
    type Output = { Field1: int64 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<int64>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) Byte.MinValue, (* outputValue *)   0L)>]
    [<InlineData((* inputValue *)           1uy, (* outputValue *)   1L)>]
    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255L)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
