namespace Parquet.FSharp.Tests.Enum

open FSharp.Core.LanguagePrimitives
open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize int64 enum`` =
    type Enum = Value1 = 0L | Value2 = 1L | Value3 = 2L
    type Input = { Field1: Enum }
    type Output = { Field1: int64 }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isInteger 64 true
                Assert.Field.ConvertedType.isInt64
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData((* inputValue *) Enum.Value1, (* outputValue *) 0L)>]
    [<InlineData((* inputValue *) Enum.Value2, (* outputValue *) 1L)>]
    [<InlineData((* inputValue *) Enum.Value3, (* outputValue *) 2L)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(Int64.MinValue)>]
    [<InlineData(           -1L)>]
    [<InlineData(            3L)>]
    [<InlineData(Int64.MaxValue)>]
    let ``undefined value`` value =
        let inputRecords = [| { Input.Field1 = EnumOfValue value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize int64 enum from required int64`` =
    type Enum = Value1 = 0L | Value2 = 1L | Value3 = 2L
    type Input = { Field1: int64 }
    type Output = { Field1: Enum }

    [<Theory>]
    [<InlineData((* inputValue *) 0L, (* outputValue *) Enum.Value1)>]
    [<InlineData((* inputValue *) 1L, (* outputValue *) Enum.Value2)>]
    [<InlineData((* inputValue *) 2L, (* outputValue *) Enum.Value3)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(Int64.MinValue)>]
    [<InlineData(           -1L)>]
    [<InlineData(            3L)>]
    [<InlineData(Int64.MaxValue)>]
    let ``undefined value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = EnumOfValue value } |] @>

module ``deserialize int64 enum from optional int64`` =
    type Enum = Value1 = 0L | Value2 = 1L | Value3 = 2L
    type Input = { Field1: int64 option }
    type Output = { Field1: Enum }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<Enum>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) 0L, (* outputValue *) Enum.Value1)>]
    [<InlineData((* inputValue *) 1L, (* outputValue *) Enum.Value2)>]
    [<InlineData((* inputValue *) 2L, (* outputValue *) Enum.Value3)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(Int64.MinValue)>]
    [<InlineData(           -1L)>]
    [<InlineData(            3L)>]
    [<InlineData(Int64.MaxValue)>]
    let ``undefined value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = EnumOfValue value } |] @>

//module ``deserialize int64 enum from required int32`` =
//    type Enum = Value1 = 0L | Value2 = 1L | Value3 = 2L
//    type Input = { Field1: int32 }
//    type Output = { Field1: Enum }

//    [<Theory>]
//    [<InlineData((* inputValue *) 0, (* outputValue *) Enum.Value1)>]
//    [<InlineData((* inputValue *) 1, (* outputValue *) Enum.Value2)>]
//    [<InlineData((* inputValue *) 2, (* outputValue *) Enum.Value3)>]
//    let ``value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

//    [<Theory>]
//    [<InlineData((* inputValue *) Int32.MinValue, (* outputValue *) -2147483648L)>]
//    [<InlineData((* inputValue *)             -1, (* outputValue *)          -1L)>]
//    [<InlineData((* inputValue *)              3, (* outputValue *)           3L)>]
//    [<InlineData((* inputValue *) Int32.MaxValue, (* outputValue *)  2147483647L)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int64 enum from optional int32`` =
//    type Enum = Value1 = 0L | Value2 = 1L | Value3 = 2L
//    type Input = { Field1: int32 option }
//    type Output = { Field1: Enum }

//    [<Fact>]
//    let ``null value`` () =
//        let inputRecords = [| { Input.Field1 = Option.None } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        raisesWith<SerializationException>
//            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
//            (fun exn ->
//                <@ exn.Message =
//                    "null value encountered during deserialization for"
//                    + $" non-nullable type '{typeof<Enum>.FullName}'" @>)

//    [<Theory>]
//    [<InlineData((* inputValue *) 0, (* outputValue *) Enum.Value1)>]
//    [<InlineData((* inputValue *) 1, (* outputValue *) Enum.Value2)>]
//    [<InlineData((* inputValue *) 2, (* outputValue *) Enum.Value3)>]
//    let ``value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

//    [<Theory>]
//    [<InlineData((* inputValue *) Int32.MinValue, (* outputValue *) -2147483648L)>]
//    [<InlineData((* inputValue *)             -1, (* outputValue *)          -1L)>]
//    [<InlineData((* inputValue *)              3, (* outputValue *)           3L)>]
//    [<InlineData((* inputValue *) Int32.MaxValue, (* outputValue *)  2147483647L)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int64 enum from required int16`` =
//    type Enum = Value1 = 0L | Value2 = 1L | Value3 = 2L
//    type Input = { Field1: int16 }
//    type Output = { Field1: Enum }

//    [<Theory>]
//    [<InlineData((* inputValue *) 0s, (* outputValue *) Enum.Value1)>]
//    [<InlineData((* inputValue *) 1s, (* outputValue *) Enum.Value2)>]
//    [<InlineData((* inputValue *) 2s, (* outputValue *) Enum.Value3)>]
//    let ``value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

//    [<Theory>]
//    [<InlineData((* inputValue *) Int16.MinValue, (* outputValue *) -32768L)>]
//    [<InlineData((* inputValue *)            -1s, (* outputValue *)     -1L)>]
//    [<InlineData((* inputValue *)             3s, (* outputValue *)      3L)>]
//    [<InlineData((* inputValue *) Int16.MaxValue, (* outputValue *)  32767L)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int64 enum from optional int16`` =
//    type Enum = Value1 = 0L | Value2 = 1L | Value3 = 2L
//    type Input = { Field1: int16 option }
//    type Output = { Field1: Enum }

//    [<Fact>]
//    let ``null value`` () =
//        let inputRecords = [| { Input.Field1 = Option.None } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        raisesWith<SerializationException>
//            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
//            (fun exn ->
//                <@ exn.Message =
//                    "null value encountered during deserialization for"
//                    + $" non-nullable type '{typeof<Enum>.FullName}'" @>)

//    [<Theory>]
//    [<InlineData((* inputValue *) 0s, (* outputValue *) Enum.Value1)>]
//    [<InlineData((* inputValue *) 1s, (* outputValue *) Enum.Value2)>]
//    [<InlineData((* inputValue *) 2s, (* outputValue *) Enum.Value3)>]
//    let ``value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

//    [<Theory>]
//    [<InlineData((* inputValue *) Int16.MinValue, (* outputValue *) -32768L)>]
//    [<InlineData((* inputValue *)            -1s, (* outputValue *)     -1L)>]
//    [<InlineData((* inputValue *)             3s, (* outputValue *)      3L)>]
//    [<InlineData((* inputValue *) Int16.MaxValue, (* outputValue *)  32767L)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int64 enum from required int8`` =
//    type Enum = Value1 = 0L | Value2 = 1L | Value3 = 2L
//    type Input = { Field1: int8 }
//    type Output = { Field1: Enum }

//    [<Theory>]
//    [<InlineData((* inputValue *) 0y, (* outputValue *) Enum.Value1)>]
//    [<InlineData((* inputValue *) 1y, (* outputValue *) Enum.Value2)>]
//    [<InlineData((* inputValue *) 2y, (* outputValue *) Enum.Value3)>]
//    let ``value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

//    [<Theory>]
//    [<InlineData((* inputValue *) SByte.MinValue, (* outputValue *) -128L)>]
//    [<InlineData((* inputValue *)            -1y, (* outputValue *)   -1L)>]
//    [<InlineData((* inputValue *)             3y, (* outputValue *)    3L)>]
//    [<InlineData((* inputValue *) SByte.MaxValue, (* outputValue *)  127L)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int64 enum from optional int8`` =
//    type Enum = Value1 = 0L | Value2 = 1L | Value3 = 2L
//    type Input = { Field1: int8 option }
//    type Output = { Field1: Enum }

//    [<Fact>]
//    let ``null value`` () =
//        let inputRecords = [| { Input.Field1 = Option.None } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        raisesWith<SerializationException>
//            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
//            (fun exn ->
//                <@ exn.Message =
//                    "null value encountered during deserialization for"
//                    + $" non-nullable type '{typeof<Enum>.FullName}'" @>)

//    [<Theory>]
//    [<InlineData((* inputValue *) 0y, (* outputValue *) Enum.Value1)>]
//    [<InlineData((* inputValue *) 1y, (* outputValue *) Enum.Value2)>]
//    [<InlineData((* inputValue *) 2y, (* outputValue *) Enum.Value3)>]
//    let ``value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

//    [<Theory>]
//    [<InlineData((* inputValue *) SByte.MinValue, (* outputValue *) -128L)>]
//    [<InlineData((* inputValue *)            -1y, (* outputValue *)   -1L)>]
//    [<InlineData((* inputValue *)             3y, (* outputValue *)    3L)>]
//    [<InlineData((* inputValue *) SByte.MaxValue, (* outputValue *)  127L)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int64 enum from required uint32`` =
//    type Enum = Value1 = 0L | Value2 = 1L | Value3 = 2L
//    type Input = { Field1: uint32 }
//    type Output = { Field1: Enum }

//    [<Theory>]
//    [<InlineData((* inputValue *) 0u, (* outputValue *) Enum.Value1)>]
//    [<InlineData((* inputValue *) 1u, (* outputValue *) Enum.Value2)>]
//    [<InlineData((* inputValue *) 2u, (* outputValue *) Enum.Value3)>]
//    let ``value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

//    [<Theory>]
//    [<InlineData((* inputValue *)              3u, (* outputValue *)          3L)>]
//    [<InlineData((* inputValue *) UInt32.MaxValue, (* outputValue *) 4294967295L)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int64 enum from optional uint32`` =
//    type Enum = Value1 = 0L | Value2 = 1L | Value3 = 2L
//    type Input = { Field1: uint32 option }
//    type Output = { Field1: Enum }

//    [<Fact>]
//    let ``null value`` () =
//        let inputRecords = [| { Input.Field1 = Option.None } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        raisesWith<SerializationException>
//            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
//            (fun exn ->
//                <@ exn.Message =
//                    "null value encountered during deserialization for"
//                    + $" non-nullable type '{typeof<Enum>.FullName}'" @>)

//    [<Theory>]
//    [<InlineData((* inputValue *) 0u, (* outputValue *) Enum.Value1)>]
//    [<InlineData((* inputValue *) 1u, (* outputValue *) Enum.Value2)>]
//    [<InlineData((* inputValue *) 2u, (* outputValue *) Enum.Value3)>]
//    let ``value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

//    [<Theory>]
//    [<InlineData((* inputValue *)              3u, (* outputValue *)          3L)>]
//    [<InlineData((* inputValue *) UInt32.MaxValue, (* outputValue *) 4294967295L)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int64 enum from required uint16`` =
//    type Enum = Value1 = 0L | Value2 = 1L | Value3 = 2L
//    type Input = { Field1: uint16 }
//    type Output = { Field1: Enum }

//    [<Theory>]
//    [<InlineData((* inputValue *) 0us, (* outputValue *) Enum.Value1)>]
//    [<InlineData((* inputValue *) 1us, (* outputValue *) Enum.Value2)>]
//    [<InlineData((* inputValue *) 2us, (* outputValue *) Enum.Value3)>]
//    let ``value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

//    [<Theory>]
//    [<InlineData((* inputValue *)             3us, (* outputValue *)     3L)>]
//    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535L)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int64 enum from optional uint16`` =
//    type Enum = Value1 = 0L | Value2 = 1L | Value3 = 2L
//    type Input = { Field1: uint16 option }
//    type Output = { Field1: Enum }

//    [<Fact>]
//    let ``null value`` () =
//        let inputRecords = [| { Input.Field1 = Option.None } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        raisesWith<SerializationException>
//            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
//            (fun exn ->
//                <@ exn.Message =
//                    "null value encountered during deserialization for"
//                    + $" non-nullable type '{typeof<Enum>.FullName}'" @>)

//    [<Theory>]
//    [<InlineData((* inputValue *) 0us, (* outputValue *) Enum.Value1)>]
//    [<InlineData((* inputValue *) 1us, (* outputValue *) Enum.Value2)>]
//    [<InlineData((* inputValue *) 2us, (* outputValue *) Enum.Value3)>]
//    let ``value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

//    [<Theory>]
//    [<InlineData((* inputValue *)             3us, (* outputValue *)     3L)>]
//    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535L)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int64 enum from required uint8`` =
//    type Enum = Value1 = 0L | Value2 = 1L | Value3 = 2L
//    type Input = { Field1: uint8 }
//    type Output = { Field1: Enum }

//    [<Theory>]
//    [<InlineData((* inputValue *) 0uy, (* outputValue *) Enum.Value1)>]
//    [<InlineData((* inputValue *) 1uy, (* outputValue *) Enum.Value2)>]
//    [<InlineData((* inputValue *) 2uy, (* outputValue *) Enum.Value3)>]
//    let ``value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

//    [<Theory>]
//    [<InlineData((* inputValue *)           3uy, (* outputValue *)   3L)>]
//    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255L)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int64 enum from optional uint8`` =
//    type Enum = Value1 = 0L | Value2 = 1L | Value3 = 2L
//    type Input = { Field1: uint8 option }
//    type Output = { Field1: Enum }

//    [<Fact>]
//    let ``null value`` () =
//        let inputRecords = [| { Input.Field1 = Option.None } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        raisesWith<SerializationException>
//            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
//            (fun exn ->
//                <@ exn.Message =
//                    "null value encountered during deserialization for"
//                    + $" non-nullable type '{typeof<Enum>.FullName}'" @>)

//    [<Theory>]
//    [<InlineData((* inputValue *) 0uy, (* outputValue *) Enum.Value1)>]
//    [<InlineData((* inputValue *) 1uy, (* outputValue *) Enum.Value2)>]
//    [<InlineData((* inputValue *) 2uy, (* outputValue *) Enum.Value3)>]
//    let ``value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

//    [<Theory>]
//    [<InlineData((* inputValue *)           3uy, (* outputValue *)   3L)>]
//    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255L)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>
