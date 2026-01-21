namespace Parquet.FSharp.Tests.DefaultConverter.Array1d

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``serialize atomic value-type array`` =
    type Input = { Field1: int[] }
    type Output = { Field1: int[] }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.isList
                Assert.Field.ConvertedType.isList
                Assert.Field.child [
                    Assert.Field.nameEquals "list"
                    Assert.Field.isRepeated
                    Assert.Field.Type.hasNoValue
                    Assert.Field.LogicalType.hasNoValue
                    Assert.Field.ConvertedType.hasNoValue
                    Assert.Field.child [
                        Assert.Field.nameEquals "element"
                        Assert.Field.isRequired
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<int[]>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    [<Fact>]
    let ``empty`` () =
        let inputRecords = [| { Input.Field1 = [||] } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = [||] } |] @>

    [<Fact>]
    let ``single element`` () =
        let inputRecords = [| { Input.Field1 = [| 1 |] } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = [| 1 |] } |] @>

    [<Fact>]
    let ``multiple elements`` () =
        let inputRecords = [| { Input.Field1 = [| 1; 2; 3 |] } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = [| 1; 2; 3 |] } |] @>

module ``serialize atomic reference-type array`` =
    type Input = { Field1: string[] }
    type Output = { Field1: string[] }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.isList
                Assert.Field.ConvertedType.isList
                Assert.Field.child [
                    Assert.Field.nameEquals "list"
                    Assert.Field.isRepeated
                    Assert.Field.Type.hasNoValue
                    Assert.Field.LogicalType.hasNoValue
                    Assert.Field.ConvertedType.hasNoValue
                    Assert.Field.child [
                        Assert.Field.nameEquals "element"
                        Assert.Field.isRequired
                        Assert.Field.Type.isByteArray
                        Assert.Field.LogicalType.isString
                        Assert.Field.ConvertedType.isUtf8
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<string[]>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    [<Fact>]
    let ``empty`` () =
        let inputRecords = [| { Input.Field1 = [||] } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = [||] } |] @>

    [<Fact>]
    let ``single element with null value`` () =
        let inputRecords = [| { Input.Field1 = [| null |] } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<string>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    [<Fact>]
    let ``single element with non-null value`` () =
        let inputRecords = [| { Input.Field1 = [| "hello" |] } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = [| "hello" |] } |] @>

    [<Fact>]
    let ``multiple elements with all null values`` () =
        let inputRecords = [| { Input.Field1 = [| null; null; null |] } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<string>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    [<Fact>]
    let ``multiple elements with single null value`` () =
        let inputRecords = [| { Input.Field1 = [| "hello"; null; "!" |] } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<string>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    [<Fact>]
    let ``multiple elements with all non-null values`` () =
        let inputRecords = [| { Input.Field1 = [| "hello"; "world"; "!" |] } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = [| "hello"; "world"; "!" |] } |] @>
