namespace Parquet.FSharp.Tests.Option

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``serialize atomic value-type option`` =
    type Input = { Field1: int option }
    type Output = { Field1: int option }

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
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 32 true
                Assert.Field.ConvertedType.isInt32
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``some value`` () =
        let inputRecords = [| { Input.Field1 = Option.Some 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 32 true
                Assert.Field.ConvertedType.isInt32
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = Option.Some 1 } |] @>

module ``serialize atomic reference-type option`` =
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

    [<Fact>]
    let ``some non-null value`` () =
        let inputRecords = [| { Input.Field1 = Option.Some "hello" } |]
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
        test <@ outputRecords = [| { Output.Field1 = Option.Some "hello" } |] @>

module ``deserialize atomic value-type option from required atomic`` =
    type Input = { Field1: int }
    type Output = { Field1: int option }

    [<Fact>]
    let ``value`` () =
        let inputRecords = [| { Input.Field1 = 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some 1 } |] @>

module ``deserialize atomic value-type option from optional atomic`` =
    type Input = { Field1: int option }
    type Output = { Field1: int option }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``non-null value`` () =
        let inputRecords = [| { Input.Field1 = Option.Some 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some 1 } |] @>

module ``deserialize atomic reference-type option from required atomic`` =
    type Input = { Field1: string }
    type Output = { Field1: string option }

    [<Fact>]
    let ``value`` () =
        let inputRecords = [| { Input.Field1 = "hello" } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some "hello" } |] @>

module ``deserialize atomic reference-type option from optional atomic`` =
    type Input = { Field1: string option }
    type Output = { Field1: string option }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``non-null value`` () =
        let inputRecords = [| { Input.Field1 = Option.Some "hello" } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some "hello" } |] @>
