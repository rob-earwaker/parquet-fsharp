namespace Parquet.FSharp.Tests.Enum

open FSharp.Core.LanguagePrimitives
open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize uint64 enum`` =
    type Enum = Value1 = 0UL | Value2 = 1UL | Value3 = 2UL
    type Input = { Field1: Enum }
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
    [<InlineData((* inputValue *) Enum.Value1, (* outputValue *) 0UL)>]
    [<InlineData((* inputValue *) Enum.Value2, (* outputValue *) 1UL)>]
    [<InlineData((* inputValue *) Enum.Value3, (* outputValue *) 2UL)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(            3UL)>]
    [<InlineData(UInt64.MaxValue)>]
    let ``undefined value`` value =
        let inputRecords = [| { Input.Field1 = EnumOfValue value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize uint64 enum from required uint64`` =
    type Enum = Value1 = 0UL | Value2 = 1UL | Value3 = 2UL
    type Input = { Field1: uint64 }
    type Output = { Field1: Enum }

    [<Theory>]
    [<InlineData((* inputValue *) 0UL, (* outputValue *) Enum.Value1)>]
    [<InlineData((* inputValue *) 1UL, (* outputValue *) Enum.Value2)>]
    [<InlineData((* inputValue *) 2UL, (* outputValue *) Enum.Value3)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(            3UL)>]
    [<InlineData(UInt64.MaxValue)>]
    let ``undefined value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = EnumOfValue value } |] @>

module ``deserialize uint64 enum from optional uint64`` =
    type Enum = Value1 = 0UL | Value2 = 1UL | Value3 = 2UL
    type Input = { Field1: uint64 option }
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
    [<InlineData((* inputValue *) 0UL, (* outputValue *) Enum.Value1)>]
    [<InlineData((* inputValue *) 1UL, (* outputValue *) Enum.Value2)>]
    [<InlineData((* inputValue *) 2UL, (* outputValue *) Enum.Value3)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(            3UL)>]
    [<InlineData(UInt64.MaxValue)>]
    let ``undefined value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = EnumOfValue value } |] @>

//module ``deserialize uint64 enum from required uint32`` =
//    type Enum = Value1 = 0UL | Value2 = 1UL | Value3 = 2UL
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
//    [<InlineData((* inputValue *)              3u, (* outputValue *)          3UL)>]
//    [<InlineData((* inputValue *) UInt32.MaxValue, (* outputValue *) 4294967295UL)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize uint64 enum from optional uint32`` =
//    type Enum = Value1 = 0UL | Value2 = 1UL | Value3 = 2UL
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
//    [<InlineData((* inputValue *)              3u, (* outputValue *)          3UL)>]
//    [<InlineData((* inputValue *) UInt32.MaxValue, (* outputValue *) 4294967295UL)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize uint64 enum from required uint16`` =
//    type Enum = Value1 = 0UL | Value2 = 1UL | Value3 = 2UL
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
//    [<InlineData((* inputValue *)             3us, (* outputValue *)     3UL)>]
//    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535UL)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize uint64 enum from optional uint16`` =
//    type Enum = Value1 = 0UL | Value2 = 1UL | Value3 = 2UL
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
//    [<InlineData((* inputValue *)             3us, (* outputValue *)     3UL)>]
//    [<InlineData((* inputValue *) UInt16.MaxValue, (* outputValue *) 65535UL)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize uint64 enum from required uint8`` =
//    type Enum = Value1 = 0UL | Value2 = 1UL | Value3 = 2UL
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
//    [<InlineData((* inputValue *)           3uy, (* outputValue *)   3UL)>]
//    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255UL)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>

//module ``deserialize uint64 enum from optional uint8`` =
//    type Enum = Value1 = 0UL | Value2 = 1UL | Value3 = 2UL
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
//    [<InlineData((* inputValue *)           3uy, (* outputValue *)   3UL)>]
//    [<InlineData((* inputValue *) Byte.MaxValue, (* outputValue *) 255UL)>]
//    let ``undefined value`` inputValue outputValue =
//        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
//        let bytes = ParquetSerializer.Serialize(inputRecords)
//        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
//        test <@ outputRecords = [| { Output.Field1 = EnumOfValue outputValue } |] @>
