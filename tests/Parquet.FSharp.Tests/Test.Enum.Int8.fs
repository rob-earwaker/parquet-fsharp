namespace Parquet.FSharp.Tests.Enum

open FSharp.Core.LanguagePrimitives
open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize int8 enum`` =
    type Enum = Value1 = 0y | Value2 = 1y | Value3 = 2y
    type Input = { Field1: Enum }
    type Output = { Field1: int8 }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 8 true
                Assert.Field.ConvertedType.isInt8
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData((* inputValue *) Enum.Value1, (* outputValue *) 0y)>]
    [<InlineData((* inputValue *) Enum.Value2, (* outputValue *) 1y)>]
    [<InlineData((* inputValue *) Enum.Value3, (* outputValue *) 2y)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(SByte.MinValue)>]
    [<InlineData(           -1y)>]
    [<InlineData(            3y)>]
    [<InlineData(SByte.MaxValue)>]
    let ``undefined value`` value =
        let inputRecords = [| { Input.Field1 = EnumOfValue value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize int8 enum from required int8`` =
    type Enum = Value1 = 0y | Value2 = 1y | Value3 = 2y
    type Input = { Field1: int8 }
    type Output = { Field1: Enum }

    [<Theory>]
    [<InlineData((* inputValue *) 0y, (* outputValue *) Enum.Value1)>]
    [<InlineData((* inputValue *) 1y, (* outputValue *) Enum.Value2)>]
    [<InlineData((* inputValue *) 2y, (* outputValue *) Enum.Value3)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(SByte.MinValue)>]
    [<InlineData(           -1y)>]
    [<InlineData(            3y)>]
    [<InlineData(SByte.MaxValue)>]
    let ``undefined value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = EnumOfValue value } |] @>

module ``deserialize int8 enum from optional int8`` =
    type Enum = Value1 = 0y | Value2 = 1y | Value3 = 2y
    type Input = { Field1: int8 option }
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
    [<InlineData((* inputValue *) 0y, (* outputValue *) Enum.Value1)>]
    [<InlineData((* inputValue *) 1y, (* outputValue *) Enum.Value2)>]
    [<InlineData((* inputValue *) 2y, (* outputValue *) Enum.Value3)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(SByte.MinValue)>]
    [<InlineData(           -1y)>]
    [<InlineData(            3y)>]
    [<InlineData(SByte.MaxValue)>]
    let ``undefined value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = EnumOfValue value } |] @>
