namespace Parquet.FSharp.Tests.Enum

open FSharp.Core.LanguagePrimitives
open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize int32 enum`` =
    type Enum = Value1 = 0 | Value2 = 1 | Value3 = 2
    type Input = { Field1: Enum }
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
    [<InlineData((* inputValue *) Enum.Value1, (* outputValue *) 0)>]
    [<InlineData((* inputValue *) Enum.Value2, (* outputValue *) 1)>]
    [<InlineData((* inputValue *) Enum.Value3, (* outputValue *) 2)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(Int32.MinValue)>]
    [<InlineData(            -1)>]
    [<InlineData(             3)>]
    [<InlineData(Int32.MaxValue)>]
    let ``undefined value`` value =
        let inputRecords = [| { Input.Field1 = EnumOfValue value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize int32 enum from required int32`` =
    type Enum = Value1 = 0 | Value2 = 1 | Value3 = 2
    type Input = { Field1: int32 }
    type Output = { Field1: Enum }

    [<Theory>]
    [<InlineData((* inputValue *) 0, (* outputValue *) Enum.Value1)>]
    [<InlineData((* inputValue *) 1, (* outputValue *) Enum.Value2)>]
    [<InlineData((* inputValue *) 2, (* outputValue *) Enum.Value3)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(Int32.MinValue)>]
    [<InlineData(            -1)>]
    [<InlineData(             3)>]
    [<InlineData(Int32.MaxValue)>]
    let ``undefined value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = EnumOfValue value } |] @>

module ``deserialize int32 enum from optional int32`` =
    type Enum = Value1 = 0 | Value2 = 1 | Value3 = 2
    type Input = { Field1: int32 option }
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
    [<InlineData((* inputValue *) 0, (* outputValue *) Enum.Value1)>]
    [<InlineData((* inputValue *) 1, (* outputValue *) Enum.Value2)>]
    [<InlineData((* inputValue *) 2, (* outputValue *) Enum.Value3)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(Int32.MinValue)>]
    [<InlineData(            -1)>]
    [<InlineData(             3)>]
    [<InlineData(Int32.MaxValue)>]
    let ``undefined value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = EnumOfValue value } |] @>

//module ``deserialize int32 enum from required int16`` =
//    type Enum = Value1 = 0 | Value2 = 1 | Value3 = 2
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
//    [<InlineData((* inputValue *) Int16.MinValue, (* outputValue *) -32768)>]
//    [<InlineData((* inputValue *)            -1s, (* outputValue *)     -1)>]
//    [<InlineData((* inputValue *)             3s, (* outputValue *)      3)>]
//    [<InlineData((* inputValue *) Int16.MaxValue, (* outputValue *)  32767)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int32 enum from optional int16`` =
//    type Enum = Value1 = 0 | Value2 = 1 | Value3 = 2
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
//    [<InlineData((* inputValue *) Int16.MinValue, (* outputValue *) -32768)>]
//    [<InlineData((* inputValue *)            -1s, (* outputValue *)     -1)>]
//    [<InlineData((* inputValue *)             3s, (* outputValue *)      3)>]
//    [<InlineData((* inputValue *) Int16.MaxValue, (* outputValue *)  32767)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int32 enum from required int8`` =
//    type Enum = Value1 = 0 | Value2 = 1 | Value3 = 2
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
//    [<InlineData((* inputValue *) SByte.MinValue, (* outputValue *) -128)>]
//    [<InlineData((* inputValue *)            -1y, (* outputValue *)   -1)>]
//    [<InlineData((* inputValue *)             3y, (* outputValue *)    3)>]
//    [<InlineData((* inputValue *) SByte.MaxValue, (* outputValue *)  127)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int32 enum from optional int8`` =
//    type Enum = Value1 = 0 | Value2 = 1 | Value3 = 2
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
//    [<InlineData((* inputValue *) SByte.MinValue, (* outputValue *) -128)>]
//    [<InlineData((* inputValue *)            -1y, (* outputValue *)   -1)>]
//    [<InlineData((* inputValue *)             3y, (* outputValue *)    3)>]
//    [<InlineData((* inputValue *) SByte.MaxValue, (* outputValue *)  127)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int32 enum from required uint16`` =
//    type Enum = Value1 = 0 | Value2 = 1 | Value3 = 2
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
//    [<InlineData((* inputValue *)             3us, (* outputValue *)     3)>]
//    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int32 enum from optional uint16`` =
//    type Enum = Value1 = 0 | Value2 = 1 | Value3 = 2
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
//    [<InlineData((* inputValue *)             3us, (* outputValue *)     3)>]
//    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int32 enum from required uint8`` =
//    type Enum = Value1 = 0 | Value2 = 1 | Value3 = 2
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
//    [<InlineData((* inputValue *)           3uy, (* outputValue *)   3)>]
//    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int32 enum from optional uint8`` =
//    type Enum = Value1 = 0 | Value2 = 1 | Value3 = 2
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
//    [<InlineData((* inputValue *)           3uy, (* outputValue *)   3)>]
//    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>
