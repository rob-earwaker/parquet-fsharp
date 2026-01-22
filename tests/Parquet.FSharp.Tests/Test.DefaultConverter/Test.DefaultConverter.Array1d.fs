namespace Parquet.FSharp.Tests.DefaultConverter.Array1d

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``serialize 1d array with atomic value-type elements`` =
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

    let NonNull = [|
        [| box<int[]> (**) [||] (**) |]
        [| box<int[]> (**) [| 1 |] (**) |]
        [| box<int[]> (**) [| 1; 2; 3 |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize 1d array with atomic reference-type elements`` =
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

    let NullElements = [|
        [| box<string[]> (**) [| null |] (**) |]
        [| box<string[]> (**) [| null; null; null |] (**) |]
        [| box<string[]> (**) [| "hello"; null; "!" |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NullElements)>]
    let ``null elements`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<string>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNullElements = [|
        [| box<string[]> (**) [||] (**) |]
        [| box<string[]> (**) [| "hello" |] (**) |]
        [| box<string[]> (**) [| "hello"; "world"; "!" |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullElements)>]
    let ``non-null elements`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize 1d array with list elements`` =
    type Input = { Field1: int[][] }
    type Output = { Field1: int[][] }

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
                                Assert.Field.hasNoChildren ] ] ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<int[][]>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NullElements = [|
        [| box<int[][]> (**) [| null |] (**) |]
        [| box<int[][]> (**) [| null; null; null |] (**) |]
        [| box<int[][]> (**) [| [| 1; 2 |]; null; [| 3 |] |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NullElements)>]
    let ``null elements`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<int[]>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNullElements = [|
        [| box<int[][]> (**) [||] (**) |]
        [| box<int[][]> (**) [| [||] |] (**) |]
        [| box<int[][]> (**) [| [| 1; 2; 3 |] |] (**) |]
        [| box<int[][]> (**) [| [||]; [||]; [||] |] (**) |]
        [| box<int[][]> (**) [| [| 1 |]; [||]; [| 2; 3; 4 |] |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullElements)>]
    let ``non-null elements`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

// TODO: Add deserialization tests!
