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
                    + $" non-nullable type '{typeof<Enum>}'" @>)

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
