namespace Parquet.FSharp.Tests.Record.Struct

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``{ default } serialize with atomic field`` =
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

module ``{ default } serialize with list field`` =
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

module ``{ default } serialize with record field`` =
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

module ``{ default } serialize with optional field`` =
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

module ``{ default } serialize with multiple fields`` =
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

module ``{ default } serialize with mutable field`` =
    type [<Struct>] Record = { mutable Field2: int }
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

module ``{ optional=true } serialize with atomic field`` =
    type [<Struct>] Record = { Field2: int }
    type Input = { [<ParquetRecordStruct(Optional = true)>] Field1: Record }
    type Output = { Field1: Record option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ optional=true } serialize with list field`` =
    type [<Struct>] Record = { Field2: int list }
    type Input = { [<ParquetRecordStruct(Optional = true)>] Field1: Record }
    type Output = { Field1: Record option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ optional=true } serialize with record field`` =
    type Inner = { Field3: int }
    type [<Struct>] Record = { Field2: Inner }
    type Input = { [<ParquetRecordStruct(Optional = true)>] Field1: Record }
    type Output = { Field1: Record option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ optional=true } serialize with optional field`` =
    type [<Struct>] Record = { Field2: int option }
    type Input = { [<ParquetRecordStruct(Optional = true)>] Field1: Record }
    type Output = { Field1: Record option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ optional=true } serialize with multiple fields`` =
    type [<Struct>] Record = { Field2: int; Field3: bool; Field4: float }
    type Input = { [<ParquetRecordStruct(Optional = true)>] Field1: Record }
    type Output = { Field1: Record option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ optional=true } serialize with mutable field`` =
    type [<Struct>] Record = { mutable Field2: int }
    type Input = { [<ParquetRecordStruct(Optional = true)>] Field1: Record }
    type Output = { Field1: Record option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ default } deserialize with atomic field`` =
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

module ``{ default } deserialize with list field`` =
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

module ``{ default } deserialize with record field`` =
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

module ``{ default } deserialize with optional field`` =
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

module ``{ default } deserialize with multiple fields`` =
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

module ``{ default } deserialize with mutable field`` =
    type InputRecord = { Field2: int }
    type [<Struct>] OutputRecord = { mutable Field2: int }
    type Input = { Field1: InputRecord }
    type Output = { Field1: OutputRecord }

    [<Fact>]
    let ``value`` () =
        let value = { InputRecord.Field2 = 1 }
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = { OutputRecord.Field2 = 1 } } |] @>

module ``{ default } deserialize with out-of-order fields`` =
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

module ``{ default } deserialize with subset of fields`` =
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

module ``{ optional=true } deserialize with atomic field`` =
    type [<Struct>] Record = { Field2: int }
    type Input = { Field1: Record option }
    type Output = { [<ParquetRecordStruct(Optional = true)>] Field1: Record }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<Record>}'" @>)

    [<Fact>]
    let ``non-null`` () =
        let value = { Record.Field2 = 1 }
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } deserialize with list field`` =
    type [<Struct>] Record = { Field2: int list }
    type Input = { Field1: Record option }
    type Output = { [<ParquetRecordStruct(Optional = true)>] Field1: Record }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<Record>}'" @>)

    let NonNull = [|
        [| box<Record> (**) { Field2 = [] } (**) |]
        [| box<Record> (**) { Field2 = [ 1 ] } (**) |]
        [| box<Record> (**) { Field2 = [ 1; 2; 3 ] } (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } deserialize with record field`` =
    type Inner = { Field3: int }
    type [<Struct>] Record = { Field2: Inner }
    type Input = { Field1: Record option }
    type Output = { [<ParquetRecordStruct(Optional = true)>] Field1: Record }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<Record>}'" @>)

    [<Fact>]
    let ``non-null`` () =
        let value = { Record.Field2 = { Inner.Field3 = 1 } }
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } deserialize with optional field`` =
    type [<Struct>] Record = { Field2: int option }
    type Input = { Field1: Record option }
    type Output = { [<ParquetRecordStruct(Optional = true)>] Field1: Record }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<Record>}'" @>)

    let NonNull = [|
        [| box<Record> (**) { Field2 = Option.None } (**) |]
        [| box<Record> (**) { Field2 = Option.Some 1 } (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } deserialize with multiple fields`` =
    type [<Struct>] Record = { Field2: int; Field3: bool; Field4: float }
    type Input = { Field1: Record option }
    type Output = { [<ParquetRecordStruct(Optional = true)>] Field1: Record }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<Record>}'" @>)

    [<Fact>]
    let ``non-null`` () =
        let value = { Record.Field2 = 1; Field3 = true; Field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } deserialize with mutable field`` =
    type InputRecord = { Field2: int }
    type [<Struct>] OutputRecord = { mutable Field2: int }
    type Input = { Field1: InputRecord option }
    type Output = { [<ParquetRecordStruct(Optional = true)>] Field1: OutputRecord }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<OutputRecord>}'" @>)

    [<Fact>]
    let ``non-null`` () =
        let value = { InputRecord.Field2 = 1 }
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = { OutputRecord.Field2 = 1 } } |] @>

module ``{ optional=true } deserialize with out-of-order fields`` =
    type InputRecord = { Field2: int; Field3: bool; Field4: float }
    type [<Struct>] OutputRecord = { Field3: bool; Field4: float; Field2: int }
    type Input = { Field1: InputRecord option }
    type Output = { [<ParquetRecordStruct(Optional = true)>] Field1: OutputRecord }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<OutputRecord>}'" @>)

    [<Fact>]
    let ``non-null`` () =
        let inputRecord = { InputRecord.Field2 = 1; Field3 = true; Field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = Option.Some inputRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { OutputRecord.Field3 = true; Field4 = 2.34; Field2 = 1 }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ optional=true } deserialize with subset of fields`` =
    type InputRecord = { Field2: int; Field3: bool; Field4: float }
    type [<Struct>] OutputRecord = { Field2: int; Field4: float }
    type Input = { Field1: InputRecord option }
    type Output = { [<ParquetRecordStruct(Optional = true)>] Field1: OutputRecord }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<OutputRecord>}'" @>)

    [<Fact>]
    let ``non-null`` () =
        let value = { InputRecord.Field2 = 1; Field3 = true; Field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { OutputRecord.Field2 = 1; Field4 = 2.34 }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>
