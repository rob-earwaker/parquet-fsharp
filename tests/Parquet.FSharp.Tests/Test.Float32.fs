namespace Parquet.FSharp.Tests.Float32

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize float32`` =
    type Input = { Field1: float32 }
    type Output = { Field1: float32 }

    [<Fact>]
    let ``nan value`` () =
        let inputRecords = [| { Input.Field1 = Single.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isFloat32
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Single.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Single.NegativeInfinity)>]
    [<InlineData(        Single.MinValue)>]
    [<InlineData(                  -1.0f)>]
    [<InlineData(        -Single.Epsilon)>]
    [<InlineData(    Single.NegativeZero)>]
    [<InlineData(                   0.0f)>]
    [<InlineData(         Single.Epsilon)>]
    [<InlineData(                   1.0f)>]
    [<InlineData(               Single.E)>]
    [<InlineData(              Single.Pi)>]
    [<InlineData(             Single.Tau)>]
    [<InlineData(        Single.MaxValue)>]
    [<InlineData(Single.PositiveInfinity)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isFloat32
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize float32 from required float32`` =
    type Input = { Field1: float32 }
    type Output = { Field1: float32 }

    [<Fact>]
    let ``nan value`` () =
        let inputRecords = [| { Input.Field1 = Single.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Single.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Single.NegativeInfinity)>]
    [<InlineData(        Single.MinValue)>]
    [<InlineData(                  -1.0f)>]
    [<InlineData(        -Single.Epsilon)>]
    [<InlineData(    Single.NegativeZero)>]
    [<InlineData(                   0.0f)>]
    [<InlineData(         Single.Epsilon)>]
    [<InlineData(                   1.0f)>]
    [<InlineData(               Single.E)>]
    [<InlineData(              Single.Pi)>]
    [<InlineData(             Single.Tau)>]
    [<InlineData(        Single.MaxValue)>]
    [<InlineData(Single.PositiveInfinity)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize float32 from optional float32`` =
    type Input = { Field1: float32 option }
    type Output = { Field1: float32 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<float32>.FullName}'" @>)

    [<Fact>]
    let ``nan value`` () =
        let inputRecords = [| { Input.Field1 = Option.Some Single.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Single.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Single.NegativeInfinity)>]
    [<InlineData(        Single.MinValue)>]
    [<InlineData(                  -1.0f)>]
    [<InlineData(        -Single.Epsilon)>]
    [<InlineData(    Single.NegativeZero)>]
    [<InlineData(                   0.0f)>]
    [<InlineData(         Single.Epsilon)>]
    [<InlineData(                   1.0f)>]
    [<InlineData(               Single.E)>]
    [<InlineData(              Single.Pi)>]
    [<InlineData(             Single.Tau)>]
    [<InlineData(        Single.MaxValue)>]
    [<InlineData(Single.PositiveInfinity)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize float32 from required int16`` =
    type Input = { Field1: int16 }
    type Output = { Field1: float32 }

    [<Theory>]
    [<InlineData((* inputValue *) Int16.MinValue, (* outputValue *) -32768.0f)>]
    [<InlineData((* inputValue *)            -1s, (* outputValue *)     -1.0f)>]
    [<InlineData((* inputValue *)             0s, (* outputValue *)      0.0f)>]
    [<InlineData((* inputValue *)             1s, (* outputValue *)      1.0f)>]
    [<InlineData((* inputValue *) Int16.MaxValue, (* outputValue *)  32767.0f)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float32 from optional int16`` =
    type Input = { Field1: int16 option }
    type Output = { Field1: float32 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<float32>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) Int16.MinValue, (* outputValue *) -32768.0f)>]
    [<InlineData((* inputValue *)            -1s, (* outputValue *)     -1.0f)>]
    [<InlineData((* inputValue *)             0s, (* outputValue *)      0.0f)>]
    [<InlineData((* inputValue *)             1s, (* outputValue *)      1.0f)>]
    [<InlineData((* inputValue *) Int16.MaxValue, (* outputValue *)  32767.0f)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float32 from required int8`` =
    type Input = { Field1: int8 }
    type Output = { Field1: float32 }

    [<Theory>]
    [<InlineData((* inputValue *) SByte.MinValue, (* outputValue *) -128.0f)>]
    [<InlineData((* inputValue *)            -1y, (* outputValue *)   -1.0f)>]
    [<InlineData((* inputValue *)             0y, (* outputValue *)    0.0f)>]
    [<InlineData((* inputValue *)             1y, (* outputValue *)    1.0f)>]
    [<InlineData((* inputValue *) SByte.MaxValue, (* outputValue *)  127.0f)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float32 from optional int8`` =
    type Input = { Field1: int8 option }
    type Output = { Field1: float32 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<float32>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) SByte.MinValue, (* outputValue *) -128.0f)>]
    [<InlineData((* inputValue *)            -1y, (* outputValue *)   -1.0f)>]
    [<InlineData((* inputValue *)             0y, (* outputValue *)    0.0f)>]
    [<InlineData((* inputValue *)             1y, (* outputValue *)    1.0f)>]
    [<InlineData((* inputValue *) SByte.MaxValue, (* outputValue *)  127.0f)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float32 from required uint16`` =
    type Input = { Field1: uint16 }
    type Output = { Field1: float32 }

    [<Theory>]
    [<InlineData((* inputValue *) UInt16.MinValue, (* outputValue *)     0.0f)>]
    [<InlineData((* inputValue *)             1us, (* outputValue *)     1.0f)>]
    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535.0f)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float32 from optional uint16`` =
    type Input = { Field1: uint16 option }
    type Output = { Field1: float32 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<float32>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) UInt16.MinValue, (* outputValue *)     0.0f)>]
    [<InlineData((* inputValue *)             1us, (* outputValue *)     1.0f)>]
    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535.0f)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float32 from required uint8`` =
    type Input = { Field1: uint8 }
    type Output = { Field1: float32 }

    [<Theory>]
    [<InlineData((* inputValue *) Byte.MinValue, (* outputValue *)   0.0f)>]
    [<InlineData((* inputValue *)           1uy, (* outputValue *)   1.0f)>]
    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255.0f)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float32 from optional uint8`` =
    type Input = { Field1: uint8 option }
    type Output = { Field1: float32 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<float32>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) Byte.MinValue, (* outputValue *)   0.0f)>]
    [<InlineData((* inputValue *)           1uy, (* outputValue *)   1.0f)>]
    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255.0f)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
