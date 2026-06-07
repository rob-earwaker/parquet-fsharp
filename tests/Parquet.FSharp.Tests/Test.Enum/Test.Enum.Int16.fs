namespace Parquet.FSharp.Tests.Enum

open FSharp.Core.LanguagePrimitives
open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize int16 enum`` =
    type Enum = Value1 = 0s | Value2 = 1s | Value3 = 2s
    type Input = { Field1: Enum }
    type Output = { Field1: int16 }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 16 true
                Assert.Field.ConvertedType.isInt16
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData((* inputValue *) Enum.Value1, (* outputValue *) 0s)>]
    [<InlineData((* inputValue *) Enum.Value2, (* outputValue *) 1s)>]
    [<InlineData((* inputValue *) Enum.Value3, (* outputValue *) 2s)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(Int16.MinValue)>]
    [<InlineData(           -1s)>]
    [<InlineData(            3s)>]
    [<InlineData(Int16.MaxValue)>]
    let ``undefined value`` value =
        let inputRecords = [| { Input.Field1 = EnumOfValue value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize int16 enum from required int16`` =
    type Enum = Value1 = 0s | Value2 = 1s | Value3 = 2s
    type Input = { Field1: int16 }
    type Output = { Field1: Enum }

    [<Theory>]
    [<InlineData((* inputValue *) 0s, (* outputValue *) Enum.Value1)>]
    [<InlineData((* inputValue *) 1s, (* outputValue *) Enum.Value2)>]
    [<InlineData((* inputValue *) 2s, (* outputValue *) Enum.Value3)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(Int16.MinValue)>]
    [<InlineData(           -1s)>]
    [<InlineData(            3s)>]
    [<InlineData(Int16.MaxValue)>]
    let ``undefined value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = EnumOfValue value } |] @>

module ``deserialize int16 enum from optional int16`` =
    type Enum = Value1 = 0s | Value2 = 1s | Value3 = 2s
    type Input = { Field1: int16 option }
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
                    + $" non-nullable type '{typeof<Enum>}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) 0s, (* outputValue *) Enum.Value1)>]
    [<InlineData((* inputValue *) 1s, (* outputValue *) Enum.Value2)>]
    [<InlineData((* inputValue *) 2s, (* outputValue *) Enum.Value3)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(Int16.MinValue)>]
    [<InlineData(           -1s)>]
    [<InlineData(            3s)>]
    [<InlineData(Int16.MaxValue)>]
    let ``undefined value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = EnumOfValue value } |] @>

//module ``deserialize int16 enum from required int8`` =
//    type Enum = Value1 = 0s | Value2 = 1s | Value3 = 2s
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
//    [<InlineData((* inputValue *) SByte.MinValue, (* outputValue *) -128s)>]
//    [<InlineData((* inputValue *)            -1y, (* outputValue *)   -1s)>]
//    [<InlineData((* inputValue *)             3y, (* outputValue *)    3s)>]
//    [<InlineData((* inputValue *) SByte.MaxValue, (* outputValue *)  127s)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int16 enum from optional int8`` =
//    type Enum = Value1 = 0s | Value2 = 1s | Value3 = 2s
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
//                    + $" non-nullable type '{typeof<Enum>}'" @>)

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
//    [<InlineData((* inputValue *) SByte.MinValue, (* outputValue *) -128s)>]
//    [<InlineData((* inputValue *)            -1y, (* outputValue *)   -1s)>]
//    [<InlineData((* inputValue *)             3y, (* outputValue *)    3s)>]
//    [<InlineData((* inputValue *) SByte.MaxValue, (* outputValue *)  127s)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int16 enum from required uint8`` =
//    type Enum = Value1 = 0s | Value2 = 1s | Value3 = 2s
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
//    [<InlineData((* inputValue *)           3uy, (* outputValue *)   3s)>]
//    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255s)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize int16 enum from optional uint8`` =
//    type Enum = Value1 = 0s | Value2 = 1s | Value3 = 2s
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
//                    + $" non-nullable type '{typeof<Enum>}'" @>)

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
//    [<InlineData((* inputValue *)           3uy, (* outputValue *)   3s)>]
//    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255s)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>
