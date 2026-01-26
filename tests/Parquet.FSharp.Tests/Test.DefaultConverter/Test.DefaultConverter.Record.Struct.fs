namespace Parquet.FSharp.Tests.DefaultConverter.Record

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``serialize record struct with atomic field`` =
    type [<Struct>] Record = { Field2: int }
    type Input = { Field1: Record }
    type Output = { Field1: Record }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "Field2"
                    Assert.Field.isRequired
                    Assert.Field.Type.isInt32
                    Assert.Field.LogicalType.isInteger 32 true
                    Assert.Field.ConvertedType.isInt32
                    Assert.Field.hasNoChildren ] ] ]

    [<Fact>]
    let ``value`` () =
        let value = { Record.Field2 = 1 }
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize record struct with list field`` =
    type [<Struct>] Record = { Field2: int list }
    type Input = { Field1: Record }
    type Output = { Field1: Record }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "Field2"
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
                            Assert.Field.hasNoChildren ] ] ] ] ]

    let Value = [|
        [| box<Record> (**) { Field2 = [] } (**) |]
        [| box<Record> (**) { Field2 = [ 1 ] } (**) |]
        [| box<Record> (**) { Field2 = [ 1; 2; 3 ] } (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize record struct with record field`` =
    type Inner = { Field3: int }
    type [<Struct>] Record = { Field2: Inner }
    type Input = { Field1: Record }
    type Output = { Field1: Record }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "Field2"
                    Assert.Field.isRequired
                    Assert.Field.Type.hasNoValue
                    Assert.Field.LogicalType.hasNoValue
                    Assert.Field.ConvertedType.hasNoValue
                    Assert.Field.child [
                        Assert.Field.nameEquals "Field3"
                        Assert.Field.isRequired
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``value`` () =
        let value = { Record.Field2 = { Inner.Field3 = 1 } }
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize record struct with optional field`` =
    type [<Struct>] Record = { Field2: int option }
    type Input = { Field1: Record }
    type Output = { Field1: Record }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "Field2"
                    Assert.Field.isOptional
                    Assert.Field.Type.isInt32
                    Assert.Field.LogicalType.isInteger 32 true
                    Assert.Field.ConvertedType.isInt32
                    Assert.Field.hasNoChildren ] ] ]

    let Value = [|
        [| box<Record> (**) { Field2 = Option.None } (**) |]
        [| box<Record> (**) { Field2 = Option.Some 1 } (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize record struct with multiple fields`` =
    type [<Struct>] Record = { Field2: int; Field3: bool; Field4: float }
    type Input = { Field1: Record }
    type Output = { Field1: Record }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.children [
                    Assert.field [
                        Assert.Field.nameEquals "Field2"
                        Assert.Field.isRequired
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "Field3"
                        Assert.Field.isRequired
                        Assert.Field.Type.isBool
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "Field4"
                        Assert.Field.isRequired
                        Assert.Field.Type.isFloat64
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``value`` () =
        let value = { Record.Field2 = 1; Field3 = true; Field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize record struct with atomic field from required record`` =
    type [<Struct>] Record = { Field2: int }
    type Input = { Field1: Record }
    type Output = { Field1: Record }

    [<Fact>]
    let ``value`` () =
        let value = { Record.Field2 = 1 }
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize record struct with atomic field from optional record`` =
    type [<Struct>] Record = { Field2: int }
    type Input = { Field1: Record option }
    type Output = { Field1: Record }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<Record>.FullName}'" @>)

    [<Fact>]
    let ``non-null value`` () =
        let value = { Record.Field2 = 1 }
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize record struct with list field from required record`` =
    type [<Struct>] Record = { Field2: int list }
    type Input = { Field1: Record }
    type Output = { Field1: Record }

    let Value = [|
        [| box<Record> (**) { Field2 = [] } (**) |]
        [| box<Record> (**) { Field2 = [ 1 ] } (**) |]
        [| box<Record> (**) { Field2 = [ 1; 2; 3 ] } (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize record struct with list field from optional record`` =
    type [<Struct>] Record = { Field2: int list }
    type Input = { Field1: Record option }
    type Output = { Field1: Record }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<Record>.FullName}'" @>)

    let NonNullValue = [|
        [| box<Record> (**) { Field2 = [] } (**) |]
        [| box<Record> (**) { Field2 = [ 1 ] } (**) |]
        [| box<Record> (**) { Field2 = [ 1; 2; 3 ] } (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize record struct with record field from required record`` =
    type Inner = { Field3: int }
    type [<Struct>] Record = { Field2: Inner }
    type Input = { Field1: Record }
    type Output = { Field1: Record }

    [<Fact>]
    let ``value`` () =
        let value = { Record.Field2 = { Inner.Field3 = 1 } }
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize record struct with record field from optional record`` =
    type Inner = { Field3: int }
    type [<Struct>] Record = { Field2: Inner }
    type Input = { Field1: Record option }
    type Output = { Field1: Record }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<Record>.FullName}'" @>)

    [<Fact>]
    let ``non-null value`` () =
        let value = { Record.Field2 = { Inner.Field3 = 1 } }
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize record struct with optional field from required record`` =
    type [<Struct>] Record = { Field2: int option }
    type Input = { Field1: Record }
    type Output = { Field1: Record }

    let Value = [|
        [| box<Record> (**) { Field2 = Option.None } (**) |]
        [| box<Record> (**) { Field2 = Option.Some 1 } (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize record struct with optional field from optional record`` =
    type [<Struct>] Record = { Field2: int option }
    type Input = { Field1: Record option }
    type Output = { Field1: Record }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<Record>.FullName}'" @>)

    let NonNullValue = [|
        [| box<Record> (**) { Field2 = Option.None } (**) |]
        [| box<Record> (**) { Field2 = Option.Some 1 } (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize record struct with multiple fields from required record`` =
    type [<Struct>] Record = { Field2: int; Field3: bool; Field4: float }
    type Input = { Field1: Record }
    type Output = { Field1: Record }

    [<Fact>]
    let ``value`` () =
        let value = { Record.Field2 = 1; Field3 = true; Field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize record struct with multiple fields from optional record`` =
    type [<Struct>] Record = { Field2: int; Field3: bool; Field4: float }
    type Input = { Field1: Record option }
    type Output = { Field1: Record }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<Record>.FullName}'" @>)

    [<Fact>]
    let ``non-null value`` () =
        let value = { Record.Field2 = 1; Field3 = true; Field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``deserialize record struct with out-of-order fields from required record`` =
    type InputRecord = { Field2: int; Field3: bool; Field4: float }
    type [<Struct>] OutputRecord = { Field3: bool; Field4: float; Field2: int }
    type Input = { Field1: InputRecord }
    type Output = { Field1: OutputRecord }

    [<Fact>]
    let ``value`` () =
        let inputRecord = { InputRecord.Field2 = 1; Field3 = true; Field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = inputRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { OutputRecord.Field3 = true; Field4 = 2.34; Field2 = 1 }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``deserialize record struct with out-of-order fields from optional record`` =
    type InputRecord = { Field2: int; Field3: bool; Field4: float }
    type [<Struct>] OutputRecord = { Field3: bool; Field4: float; Field2: int }
    type Input = { Field1: InputRecord option }
    type Output = { Field1: OutputRecord }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<OutputRecord>.FullName}'" @>)

    [<Fact>]
    let ``non-null value`` () =
        let inputRecord = { InputRecord.Field2 = 1; Field3 = true; Field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = Option.Some inputRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { OutputRecord.Field3 = true; Field4 = 2.34; Field2 = 1 }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``deserialize record struct with subset of fields from required record`` =
    type InputRecord = { Field2: int; Field3: bool; Field4: float }
    type [<Struct>] OutputRecord = { Field2: int; Field4: float }
    type Input = { Field1: InputRecord }
    type Output = { Field1: OutputRecord }

    [<Fact>]
    let ``value`` () =
        let value = { InputRecord.Field2 = 1; Field3 = true; Field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { OutputRecord.Field2 = 1; Field4 = 2.34 }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``deserialize record struct with subset of fields from optional record`` =
    type InputRecord = { Field2: int; Field3: bool; Field4: float }
    type [<Struct>] OutputRecord = { Field2: int; Field4: float }
    type Input = { Field1: InputRecord option }
    type Output = { Field1: OutputRecord }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + " non-nullable type"
                    + $" '{typeof<OutputRecord>.FullName}'" @>)

    [<Fact>]
    let ``non-null value`` () =
        let value = { InputRecord.Field2 = 1; Field3 = true; Field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { OutputRecord.Field2 = 1; Field4 = 2.34 }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
