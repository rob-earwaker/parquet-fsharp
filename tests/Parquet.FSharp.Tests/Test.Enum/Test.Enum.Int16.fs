namespace Parquet.FSharp.Tests.Enum.Int16

open FSharp.Core.LanguagePrimitives
open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``{ default } serialize`` =
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
    let ``defined`` inputValue outputValue =
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
    let ``undefined`` value =
        let inputRecords = [| { Input.Field1 = EnumOfValue value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=false } serialize`` =
    type Enum = Value1 = 0s | Value2 = 1s | Value3 = 2s
    type Input = { [<ParquetEnum(Optional = false)>] Field1: Enum }
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
    let ``defined`` inputValue outputValue =
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
    let ``undefined`` value =
        let inputRecords = [| { Input.Field1 = EnumOfValue value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } serialize`` =
    type Enum = Value1 = 0s | Value2 = 1s | Value3 = 2s
    type Input = { [<ParquetEnum(Optional = true)>] Field1: Enum }
    type Output = { Field1: int16 option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 16 true
                Assert.Field.ConvertedType.isInt16
                Assert.Field.hasNoChildren ] ]

    [<Theory>]
    [<InlineData((* inputValue *) Enum.Value1, (* outputValue *) 0s)>]
    [<InlineData((* inputValue *) Enum.Value2, (* outputValue *) 1s)>]
    [<InlineData((* inputValue *) Enum.Value3, (* outputValue *) 2s)>]
    let ``defined`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some outputValue } |] @>

    [<Theory>]
    [<InlineData(Int16.MinValue)>]
    [<InlineData(           -1s)>]
    [<InlineData(            3s)>]
    [<InlineData(Int16.MaxValue)>]
    let ``undefined`` value =
        let inputRecords = [| { Input.Field1 = EnumOfValue value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ default } deserialize`` =
    type Enum = Value1 = 0s | Value2 = 1s | Value3 = 2s
    type Input = { Field1: int16 }
    type Output = { Field1: Enum }

    [<Theory>]
    [<InlineData((* inputValue *) 0s, (* outputValue *) Enum.Value1)>]
    [<InlineData((* inputValue *) 1s, (* outputValue *) Enum.Value2)>]
    [<InlineData((* inputValue *) 2s, (* outputValue *) Enum.Value3)>]
    let ``defined`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(Int16.MinValue)>]
    [<InlineData(           -1s)>]
    [<InlineData(            3s)>]
    [<InlineData(Int16.MaxValue)>]
    let ``undefined`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = EnumOfValue value } |] @>

module ``{ optional=false } deserialize`` =
    type Enum = Value1 = 0s | Value2 = 1s | Value3 = 2s
    type Input = { Field1: int16 }
    type Output = { [<ParquetEnum(Optional = false)>] Field1: Enum }

    [<Theory>]
    [<InlineData((* inputValue *) 0s, (* outputValue *) Enum.Value1)>]
    [<InlineData((* inputValue *) 1s, (* outputValue *) Enum.Value2)>]
    [<InlineData((* inputValue *) 2s, (* outputValue *) Enum.Value3)>]
    let ``defined`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(Int16.MinValue)>]
    [<InlineData(           -1s)>]
    [<InlineData(            3s)>]
    [<InlineData(Int16.MaxValue)>]
    let ``undefined`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = EnumOfValue value } |] @>

module ``{ optional=true } deserialize`` =
    type Enum = Value1 = 0s | Value2 = 1s | Value3 = 2s
    type Input = { Field1: int16 option }
    type Output = { [<ParquetEnum(Optional = true)>] Field1: Enum }

    [<Fact>]
    let ``null`` () =
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
    let ``defined`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData(Int16.MinValue)>]
    [<InlineData(           -1s)>]
    [<InlineData(            3s)>]
    [<InlineData(Int16.MaxValue)>]
    let ``undefined`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = EnumOfValue value } |] @>
