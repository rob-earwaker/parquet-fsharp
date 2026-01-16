namespace Parquet.FSharp.Tests.String

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``serialize string`` =
    type Input = { Field1: string }
    type Output = { Field1: string }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<string>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    [<Theory>]
    [<InlineData("")>]
    [<InlineData("abcdefghijklmnopqrstuvwxyz")>]
    [<InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ")>]
    [<InlineData("0123456789")>]
    [<InlineData(" \t\n\r")>]
    [<InlineData("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isByteArray
                Assert.Field.LogicalType.isString
                Assert.Field.ConvertedType.isUtf8
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize string option`` =
    type Input = { Field1: string option }
    type Output = { Field1: string option }

    [<Fact>]
    let ``none value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isByteArray
                Assert.Field.LogicalType.isString
                Assert.Field.ConvertedType.isUtf8
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``some null value`` () =
        let inputRecords = [| { Input.Field1 = Option.Some null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<string>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    [<Theory>]
    [<InlineData("")>]
    [<InlineData("abcdefghijklmnopqrstuvwxyz")>]
    [<InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ")>]
    [<InlineData("0123456789")>]
    [<InlineData(" \t\n\r")>]
    [<InlineData("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")>]
    let ``some non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isByteArray
                Assert.Field.LogicalType.isString
                Assert.Field.ConvertedType.isUtf8
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``deserialize string from required string`` =
    type Input = { Field1: string }
    type Output = { Field1: string }

    [<Theory>]
    [<InlineData("")>]
    [<InlineData("abcdefghijklmnopqrstuvwxyz")>]
    [<InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ")>]
    [<InlineData("0123456789")>]
    [<InlineData(" \t\n\r")>]
    [<InlineData("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize string from optional string`` =
    type Input = { Field1: string option }
    type Output = { Field1: string }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<string>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    [<Theory>]
    [<InlineData("")>]
    [<InlineData("abcdefghijklmnopqrstuvwxyz")>]
    [<InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ")>]
    [<InlineData("0123456789")>]
    [<InlineData(" \t\n\r")>]
    [<InlineData("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize string option from required string`` =
    type Input = { Field1: string }
    type Output = { Field1: string option }

    [<Theory>]
    [<InlineData("")>]
    [<InlineData("abcdefghijklmnopqrstuvwxyz")>]
    [<InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ")>]
    [<InlineData("0123456789")>]
    [<InlineData(" \t\n\r")>]
    [<InlineData("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``deserialize string option from optional string`` =
    type Input = { Field1: string option }
    type Output = { Field1: string option }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Theory>]
    [<InlineData("")>]
    [<InlineData("abcdefghijklmnopqrstuvwxyz")>]
    [<InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ")>]
    [<InlineData("0123456789")>]
    [<InlineData(" \t\n\r")>]
    [<InlineData("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>
