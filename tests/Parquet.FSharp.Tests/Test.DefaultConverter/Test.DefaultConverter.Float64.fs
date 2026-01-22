namespace Parquet.FSharp.Tests.DefaultConverter.Float64

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize float64`` =
    type Input = { Field1: float }
    type Output = { Field1: float }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isFloat64
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``nan value`` () =
        let inputRecords = [| { Input.Field1 = Double.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Double.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Double.NegativeInfinity)>]
    [<InlineData(        Double.MinValue)>]
    [<InlineData(                   -1.0)>]
    [<InlineData(        -Double.Epsilon)>]
    [<InlineData(    Double.NegativeZero)>]
    [<InlineData(                    0.0)>]
    [<InlineData(         Double.Epsilon)>]
    [<InlineData(                    1.0)>]
    [<InlineData(               Double.E)>]
    [<InlineData(              Double.Pi)>]
    [<InlineData(             Double.Tau)>]
    [<InlineData(        Double.MaxValue)>]
    [<InlineData(Double.PositiveInfinity)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize float64 from required float64`` =
    type Input = { Field1: float }
    type Output = { Field1: float }

    [<Fact>]
    let ``nan value`` () =
        let inputRecords = [| { Input.Field1 = Double.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Double.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Double.NegativeInfinity)>]
    [<InlineData(        Double.MinValue)>]
    [<InlineData(                   -1.0)>]
    [<InlineData(        -Double.Epsilon)>]
    [<InlineData(    Double.NegativeZero)>]
    [<InlineData(                    0.0)>]
    [<InlineData(         Double.Epsilon)>]
    [<InlineData(                    1.0)>]
    [<InlineData(               Double.E)>]
    [<InlineData(              Double.Pi)>]
    [<InlineData(             Double.Tau)>]
    [<InlineData(        Double.MaxValue)>]
    [<InlineData(Double.PositiveInfinity)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize float64 from optional float64`` =
    type Input = { Field1: float option }
    type Output = { Field1: float }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<float>.FullName}'" @>)

    [<Fact>]
    let ``nan value`` () =
        let inputRecords = [| { Input.Field1 = Option.Some Double.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Double.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Double.NegativeInfinity)>]
    [<InlineData(        Double.MinValue)>]
    [<InlineData(                   -1.0)>]
    [<InlineData(        -Double.Epsilon)>]
    [<InlineData(    Double.NegativeZero)>]
    [<InlineData(                    0.0)>]
    [<InlineData(         Double.Epsilon)>]
    [<InlineData(                    1.0)>]
    [<InlineData(               Double.E)>]
    [<InlineData(              Double.Pi)>]
    [<InlineData(             Double.Tau)>]
    [<InlineData(        Double.MaxValue)>]
    [<InlineData(Double.PositiveInfinity)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize float64 from required float32`` =
    type Input = { Field1: float32 }
    type Output = { Field1: float }

    [<Fact>]
    let ``nan value`` () =
        let inputRecords = [| { Input.Field1 = Single.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Double.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData((* inputValue *) Single.NegativeInfinity, (* outputValue *) Double.NegativeInfinity)>]
    [<InlineData((* inputValue *)         Single.MinValue, (* outputValue *)         -3.40282347e+38)>]
    [<InlineData((* inputValue *)                   -1.0f, (* outputValue *)                    -1.0)>]
    [<InlineData((* inputValue *)         -Single.Epsilon, (* outputValue *)         -1.40129846e-45)>]
    [<InlineData((* inputValue *)     Single.NegativeZero, (* outputValue *)     Double.NegativeZero)>]
    [<InlineData((* inputValue *)                    0.0f, (* outputValue *)                     0.0)>]
    [<InlineData((* inputValue *)          Single.Epsilon, (* outputValue *)          1.40129846e-45)>]
    [<InlineData((* inputValue *)                    1.0f, (* outputValue *)                     1.0)>]
    [<InlineData((* inputValue *)                Single.E, (* outputValue *)                Double.E)>]
    [<InlineData((* inputValue *)               Single.Pi, (* outputValue *)               Double.Pi)>]
    [<InlineData((* inputValue *)              Single.Tau, (* outputValue *)              Double.Tau)>]
    [<InlineData((* inputValue *)         Single.MaxValue, (* outputValue *)          3.40282347e+38)>]
    [<InlineData((* inputValue *) Single.PositiveInfinity, (* outputValue *) Double.PositiveInfinity)>]
    let ``value`` inputValue (outputValue: float) =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Test for equality based on relative error to float32 precision.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        Assert.float64RelativeEqual outputValue outputRecord.Field1 1E-7

module ``deserialize float64 from optional float32`` =
    type Input = { Field1: float32 option }
    type Output = { Field1: float }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<float>.FullName}'" @>)

    [<Fact>]
    let ``nan value`` () =
        let inputRecords = [| { Input.Field1 = Option.Some Single.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Double.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData((* inputValue *) Single.NegativeInfinity, (* outputValue *) Double.NegativeInfinity)>]
    [<InlineData((* inputValue *)         Single.MinValue, (* outputValue *)         -3.40282347e+38)>]
    [<InlineData((* inputValue *)                   -1.0f, (* outputValue *)                    -1.0)>]
    [<InlineData((* inputValue *)         -Single.Epsilon, (* outputValue *)         -1.40129846e-45)>]
    [<InlineData((* inputValue *)     Single.NegativeZero, (* outputValue *)     Double.NegativeZero)>]
    [<InlineData((* inputValue *)                    0.0f, (* outputValue *)                     0.0)>]
    [<InlineData((* inputValue *)          Single.Epsilon, (* outputValue *)          1.40129846e-45)>]
    [<InlineData((* inputValue *)                    1.0f, (* outputValue *)                     1.0)>]
    [<InlineData((* inputValue *)                Single.E, (* outputValue *)                Double.E)>]
    [<InlineData((* inputValue *)               Single.Pi, (* outputValue *)               Double.Pi)>]
    [<InlineData((* inputValue *)              Single.Tau, (* outputValue *)              Double.Tau)>]
    [<InlineData((* inputValue *)         Single.MaxValue, (* outputValue *)          3.40282347e+38)>]
    [<InlineData((* inputValue *) Single.PositiveInfinity, (* outputValue *) Double.PositiveInfinity)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Test for equality based on relative error to float32 precision.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        Assert.float64RelativeEqual outputValue outputRecord.Field1 1E-7

module ``deserialize float64 from required int32`` =
    type Input = { Field1: int32 }
    type Output = { Field1: float }

    [<Theory>]
    [<InlineData((* inputValue *) Int32.MinValue, (* outputValue *) -2147483648.0)>]
    [<InlineData((* inputValue *)             -1, (* outputValue *)          -1.0)>]
    [<InlineData((* inputValue *)              0, (* outputValue *)           0.0)>]
    [<InlineData((* inputValue *)              1, (* outputValue *)           1.0)>]
    [<InlineData((* inputValue *) Int32.MaxValue, (* outputValue *)  2147483647.0)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float64 from optional int32`` =
    type Input = { Field1: int32 option }
    type Output = { Field1: float }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<float>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) Int32.MinValue, (* outputValue *) -2147483648.0)>]
    [<InlineData((* inputValue *)             -1, (* outputValue *)          -1.0)>]
    [<InlineData((* inputValue *)              0, (* outputValue *)           0.0)>]
    [<InlineData((* inputValue *)              1, (* outputValue *)           1.0)>]
    [<InlineData((* inputValue *) Int32.MaxValue, (* outputValue *)  2147483647.0)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float64 from required int16`` =
    type Input = { Field1: int16 }
    type Output = { Field1: float }

    [<Theory>]
    [<InlineData((* inputValue *) Int16.MinValue, (* outputValue *) -32768.0)>]
    [<InlineData((* inputValue *)            -1s, (* outputValue *)     -1.0)>]
    [<InlineData((* inputValue *)             0s, (* outputValue *)      0.0)>]
    [<InlineData((* inputValue *)             1s, (* outputValue *)      1.0)>]
    [<InlineData((* inputValue *) Int16.MaxValue, (* outputValue *)  32767.0)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float64 from optional int16`` =
    type Input = { Field1: int16 option }
    type Output = { Field1: float }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<float>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) Int16.MinValue, (* outputValue *) -32768.0)>]
    [<InlineData((* inputValue *)            -1s, (* outputValue *)     -1.0)>]
    [<InlineData((* inputValue *)             0s, (* outputValue *)      0.0)>]
    [<InlineData((* inputValue *)             1s, (* outputValue *)      1.0)>]
    [<InlineData((* inputValue *) Int16.MaxValue, (* outputValue *)  32767.0)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float64 from required int8`` =
    type Input = { Field1: int8 }
    type Output = { Field1: float }

    [<Theory>]
    [<InlineData((* inputValue *) SByte.MinValue, (* outputValue *) -128.0)>]
    [<InlineData((* inputValue *)            -1y, (* outputValue *)   -1.0)>]
    [<InlineData((* inputValue *)             0y, (* outputValue *)    0.0)>]
    [<InlineData((* inputValue *)             1y, (* outputValue *)    1.0)>]
    [<InlineData((* inputValue *) SByte.MaxValue, (* outputValue *)  127.0)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float64 from optional int8`` =
    type Input = { Field1: int8 option }
    type Output = { Field1: float }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<float>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) SByte.MinValue, (* outputValue *) -128.0)>]
    [<InlineData((* inputValue *)            -1y, (* outputValue *)   -1.0)>]
    [<InlineData((* inputValue *)             0y, (* outputValue *)    0.0)>]
    [<InlineData((* inputValue *)             1y, (* outputValue *)    1.0)>]
    [<InlineData((* inputValue *) SByte.MaxValue, (* outputValue *)  127.0)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float64 from required uint32`` =
    type Input = { Field1: uint32 }
    type Output = { Field1: float }

    [<Theory>]
    [<InlineData((* inputValue *) UInt32.MinValue, (* outputValue *)          0.0)>]
    [<InlineData((* inputValue *)              1u, (* outputValue *)          1.0)>]
    [<InlineData((* inputValue *) UInt32.MaxValue, (* outputValue *) 4294967295.0)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float64 from optional uint32`` =
    type Input = { Field1: uint32 option }
    type Output = { Field1: float }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<float>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) UInt32.MinValue, (* outputValue *)          0.0)>]
    [<InlineData((* inputValue *)              1u, (* outputValue *)          1.0)>]
    [<InlineData((* inputValue *) UInt32.MaxValue, (* outputValue *) 4294967295.0)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float64 from required uint16`` =
    type Input = { Field1: uint16 }
    type Output = { Field1: float }

    [<Theory>]
    [<InlineData((* inputValue *) UInt16.MinValue, (* outputValue *)     0.0)>]
    [<InlineData((* inputValue *)             1us, (* outputValue *)     1.0)>]
    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535.0)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float64 from optional uint16`` =
    type Input = { Field1: uint16 option }
    type Output = { Field1: float }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<float>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) UInt16.MinValue, (* outputValue *)     0.0)>]
    [<InlineData((* inputValue *)             1us, (* outputValue *)     1.0)>]
    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535.0)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float64 from required uint8`` =
    type Input = { Field1: uint8 }
    type Output = { Field1: float }

    [<Theory>]
    [<InlineData((* inputValue *) Byte.MinValue, (* outputValue *)   0.0)>]
    [<InlineData((* inputValue *)           1uy, (* outputValue *)   1.0)>]
    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255.0)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize float64 from optional uint8`` =
    type Input = { Field1: uint8 option }
    type Output = { Field1: float }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<float>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) Byte.MinValue, (* outputValue *)   0.0)>]
    [<InlineData((* inputValue *)           1uy, (* outputValue *)   1.0)>]
    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255.0)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
