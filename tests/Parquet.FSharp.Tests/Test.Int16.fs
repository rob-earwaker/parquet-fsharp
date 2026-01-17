namespace Parquet.FSharp.Tests.Int16

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize int16`` =
    type Input = { Field1: int16 }
    type Output = { Field1: int16 }

    [<Theory>]
    [<InlineData(0s)>]
    [<InlineData(1s)>]
    [<InlineData(-1s)>]
    [<InlineData(Int16.MinValue)>]
    [<InlineData(Int16.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 16 true
                Assert.Field.ConvertedType.isInt16
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize int16 from required int16`` =
    type Input = { Field1: int16 }
    type Output = { Field1: int16 }

    [<Theory>]
    [<InlineData(0s)>]
    [<InlineData(1s)>]
    [<InlineData(-1s)>]
    [<InlineData(Int16.MinValue)>]
    [<InlineData(Int16.MaxValue)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize int16 from optional int16`` =
    type Input = { Field1: int16 option }
    type Output = { Field1: int16 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<int16>.FullName}'" @>)

    [<Theory>]
    [<InlineData(0s)>]
    [<InlineData(1s)>]
    [<InlineData(-1s)>]
    [<InlineData(Int16.MinValue)>]
    [<InlineData(Int16.MaxValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize int16 from required int8`` =
    type Input = { Field1: int8 }
    type Output = { Field1: int16 }

    [<Theory>]
    [<InlineData((* inputValue *) 0y,             (* expectedOutputValue *) 0s)>]
    [<InlineData((* inputValue *) 1y,             (* expectedOutputValue *) 1s)>]
    [<InlineData((* inputValue *) -1y,            (* expectedOutputValue *) -1s)>]
    [<InlineData((* inputValue *) SByte.MinValue, (* expectedOutputValue *) -128s)>]
    [<InlineData((* inputValue *) SByte.MaxValue, (* expectedOutputValue *) 127s)>]
    let ``value`` inputValue expectedOutputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = expectedOutputValue } |] @>

module ``deserialize int16 from optional int8`` =
    type Input = { Field1: int8 option }
    type Output = { Field1: int16 }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<int16>.FullName}'" @>)

    [<Theory>]
    [<InlineData((* inputValue *) 0y,             (* expectedOutputValue *) 0s)>]
    [<InlineData((* inputValue *) 1y,             (* expectedOutputValue *) 1s)>]
    [<InlineData((* inputValue *) -1y,            (* expectedOutputValue *) -1s)>]
    [<InlineData((* inputValue *) SByte.MinValue, (* expectedOutputValue *) -128s)>]
    [<InlineData((* inputValue *) SByte.MaxValue, (* expectedOutputValue *) 127s)>]
    let ``non-null value`` inputValue expectedOutputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = expectedOutputValue } |] @>
