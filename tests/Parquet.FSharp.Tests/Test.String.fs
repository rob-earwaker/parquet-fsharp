namespace Parquet.FSharp.Tests.String

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``{ default } serialize`` =
    type Input = { Field1: string }
    type Output = { Field1: string }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isByteArray
                Assert.Field.LogicalType.isString
                Assert.Field.ConvertedType.isUtf8
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<string>}' which is not optional by default" @>)

    [<Theory>]
    [<InlineData("")>]
    [<InlineData("abcdefghijklmnopqrstuvwxyz")>]
    [<InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ")>]
    [<InlineData("0123456789")>]
    [<InlineData(" \t\n\r")>]
    [<InlineData("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ allowNull=true } serialize`` =
    type Input = { [<ParquetString(AllowNull = true)>] Field1: string }
    type Output = { Field1: string }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isByteArray
                Assert.Field.LogicalType.isString
                Assert.Field.ConvertedType.isUtf8
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<string>}' which is not optional by default" @>)

    [<Theory>]
    [<InlineData("")>]
    [<InlineData("abcdefghijklmnopqrstuvwxyz")>]
    [<InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ")>]
    [<InlineData("0123456789")>]
    [<InlineData(" \t\n\r")>]
    [<InlineData("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } serialize`` =
    type Input = { [<ParquetString(Optional = true)>] Field1: string }
    type Output = { Field1: string option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isByteArray
                Assert.Field.LogicalType.isString
                Assert.Field.ConvertedType.isUtf8
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<string>}' for which nulls are not allowed by default" @>)

    [<Theory>]
    [<InlineData("")>]
    [<InlineData("abcdefghijklmnopqrstuvwxyz")>]
    [<InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ")>]
    [<InlineData("0123456789")>]
    [<InlineData(" \t\n\r")>]
    [<InlineData("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ optional=true; allowNull=true } serialize`` =
    type Input = { [<ParquetString(Optional = true, AllowNull = true)>] Field1: string }
    type Output = { Field1: string option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isByteArray
                Assert.Field.LogicalType.isString
                Assert.Field.ConvertedType.isUtf8
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = null } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Theory>]
    [<InlineData("")>]
    [<InlineData("abcdefghijklmnopqrstuvwxyz")>]
    [<InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ")>]
    [<InlineData("0123456789")>]
    [<InlineData(" \t\n\r")>]
    [<InlineData("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ default } deserialize`` =
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

module ``{ optional=true } deserialize`` =
    type Input = { Field1: string option }
    type Output = { [<ParquetString(Optional = true)>] Field1: string }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<string>}' for which nulls are not allowed by default" @>)

    [<Theory>]
    [<InlineData("")>]
    [<InlineData("abcdefghijklmnopqrstuvwxyz")>]
    [<InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ")>]
    [<InlineData("0123456789")>]
    [<InlineData(" \t\n\r")>]
    [<InlineData("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true; allowNull=true } deserialize`` =
    type Input = { Field1: string option }
    type Output = { [<ParquetString(Optional = true, AllowNull = true)>] Field1: string }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = null } |] @>

    [<Theory>]
    [<InlineData("")>]
    [<InlineData("abcdefghijklmnopqrstuvwxyz")>]
    [<InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ")>]
    [<InlineData("0123456789")>]
    [<InlineData(" \t\n\r")>]
    [<InlineData("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
