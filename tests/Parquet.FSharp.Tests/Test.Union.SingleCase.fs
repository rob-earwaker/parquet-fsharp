namespace Parquet.FSharp.Tests.Union.SingleCase

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

// TODO: Add tests for struct unions

module ``{ default } serialize with atomic field`` =
    type Union = Case1 of field2:int
    type UnionRecord = { field2: int }
    type Input = { Field1: Union }
    type Output = { Field1: UnionRecord }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
                    Assert.Field.isRequired
                    Assert.Field.Type.isInt32
                    Assert.Field.LogicalType.isInteger 32 true
                    Assert.Field.ConvertedType.isInt32
                    Assert.Field.hasNoChildren ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Union>}' which is not optional by default" @>)

    [<Fact>]
    let ``non-null`` () =
        let inputRecords = [| { Input.Field1 = Union.Case1 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = 1 }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ default } serialize with list field`` =
    type Union = Case1 of field2:int list
    type UnionRecord = { field2: int list }
    type Input = { Field1: Union }
    type Output = { Field1: UnionRecord }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
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

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Union>}' which is not optional by default" @>)

    let NonNull = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Union.Case1 value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = value }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ default } serialize with record field`` =
    type Record = { Field3: int }
    type Union = Case1 of field2:Record
    type UnionRecord = { field2: Record }
    type Input = { Field1: Union }
    type Output = { Field1: UnionRecord }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
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
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Union>}' which is not optional by default" @>)

    [<Fact>]
    let ``non-null`` () =
        let value = { Record.Field3 = 1 }
        let inputRecords = [| { Input.Field1 = Union.Case1 value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = value }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ default } serialize with optional field`` =
    type Union = Case1 of field2:int option
    type UnionRecord = { field2: int option }
    type Input = { Field1: Union }
    type Output = { Field1: UnionRecord }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
                    Assert.Field.isOptional
                    Assert.Field.Type.isInt32
                    Assert.Field.LogicalType.isInteger 32 true
                    Assert.Field.ConvertedType.isInt32
                    Assert.Field.hasNoChildren ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Union>}' which is not optional by default" @>)

    let NonNull = [|
        [| box<int option> <| (**) Option.None (**) |]
        [| box<int option> <| (**) Option.Some 1 (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Union.Case1 value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = value }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ default } serialize with multiple fields`` =
    type Union = Case1 of field2:int * field3:bool * field4:float
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Input = { Field1: Union }
    type Output = { Field1: UnionRecord }

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
                        Assert.Field.nameEquals "field2"
                        Assert.Field.isRequired
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "field3"
                        Assert.Field.isRequired
                        Assert.Field.Type.isBool
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "field4"
                        Assert.Field.isRequired
                        Assert.Field.Type.isFloat64
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Union>}' which is not optional by default" @>)

    [<Fact>]
    let ``non-null`` () =
        let inputRecords = [| { Input.Field1 = Union.Case1 (1, true, 1.23) } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = 1; field3 = true; field4 = 1.23 }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ allowNull=true } serialize with atomic field`` =
    type Union = Case1 of field2:int
    type UnionRecord = { field2: int }
    type Input = { [<ParquetSingleCaseUnion(AllowNull = true)>] Field1: Union }
    type Output = { Field1: UnionRecord }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
                    Assert.Field.isRequired
                    Assert.Field.Type.isInt32
                    Assert.Field.LogicalType.isInteger 32 true
                    Assert.Field.ConvertedType.isInt32
                    Assert.Field.hasNoChildren ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Union>}' which is not optional by default" @>)

    [<Fact>]
    let ``non-null`` () =
        let inputRecords = [| { Input.Field1 = Union.Case1 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = 1 }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ allowNull=true } serialize with list field`` =
    type Union = Case1 of field2:int list
    type UnionRecord = { field2: int list }
    type Input = { [<ParquetSingleCaseUnion(AllowNull = true)>] Field1: Union }
    type Output = { Field1: UnionRecord }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
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

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Union>}' which is not optional by default" @>)

    let NonNull = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Union.Case1 value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = value }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ allowNull=true } serialize with record field`` =
    type Record = { Field3: int }
    type Union = Case1 of field2:Record
    type UnionRecord = { field2: Record }
    type Input = { [<ParquetSingleCaseUnion(AllowNull = true)>] Field1: Union }
    type Output = { Field1: UnionRecord }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
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
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Union>}' which is not optional by default" @>)

    [<Fact>]
    let ``non-null`` () =
        let value = { Record.Field3 = 1 }
        let inputRecords = [| { Input.Field1 = Union.Case1 value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = value }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ allowNull=true } serialize with optional field`` =
    type Union = Case1 of field2:int option
    type UnionRecord = { field2: int option }
    type Input = { [<ParquetSingleCaseUnion(AllowNull = true)>] Field1: Union }
    type Output = { Field1: UnionRecord }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
                    Assert.Field.isOptional
                    Assert.Field.Type.isInt32
                    Assert.Field.LogicalType.isInteger 32 true
                    Assert.Field.ConvertedType.isInt32
                    Assert.Field.hasNoChildren ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Union>}' which is not optional by default" @>)

    let NonNull = [|
        [| box<int option> <| (**) Option.None (**) |]
        [| box<int option> <| (**) Option.Some 1 (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Union.Case1 value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = value }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ allowNull=true } serialize with multiple fields`` =
    type Union = Case1 of field2:int * field3:bool * field4:float
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Input = { [<ParquetSingleCaseUnion(AllowNull = true)>] Field1: Union }
    type Output = { Field1: UnionRecord }

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
                        Assert.Field.nameEquals "field2"
                        Assert.Field.isRequired
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "field3"
                        Assert.Field.isRequired
                        Assert.Field.Type.isBool
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "field4"
                        Assert.Field.isRequired
                        Assert.Field.Type.isFloat64
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Union>}' which is not optional by default" @>)

    [<Fact>]
    let ``non-null`` () =
        let inputRecords = [| { Input.Field1 = Union.Case1 (1, true, 1.23) } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = 1; field3 = true; field4 = 1.23 }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``{ optional=true } serialize with atomic field`` =
    type Union = Case1 of field2:int
    type UnionRecord = { field2: int }
    type Input = { [<ParquetSingleCaseUnion(Optional = true)>] Field1: Union }
    type Output = { Field1: UnionRecord option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
                    Assert.Field.isRequired
                    Assert.Field.Type.isInt32
                    Assert.Field.LogicalType.isInteger 32 true
                    Assert.Field.ConvertedType.isInt32
                    Assert.Field.hasNoChildren ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Union>}' for which nulls are not allowed by default" @>)

    [<Fact>]
    let ``non-null`` () =
        let inputRecords = [| { Input.Field1 = Union.Case1 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = 1 }
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

module ``{ optional=true } serialize with list field`` =
    type Union = Case1 of field2:int list
    type UnionRecord = { field2: int list }
    type Input = { [<ParquetSingleCaseUnion(Optional = true)>] Field1: Union }
    type Output = { Field1: UnionRecord option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
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

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Union>}' for which nulls are not allowed by default" @>)

    let NonNull = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Union.Case1 value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = value }
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

module ``{ optional=true } serialize with record field`` =
    type Record = { Field3: int }
    type Union = Case1 of field2:Record
    type UnionRecord = { field2: Record }
    type Input = { [<ParquetSingleCaseUnion(Optional = true)>] Field1: Union }
    type Output = { Field1: UnionRecord option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
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
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Union>}' for which nulls are not allowed by default" @>)

    [<Fact>]
    let ``non-null`` () =
        let value = { Record.Field3 = 1 }
        let inputRecords = [| { Input.Field1 = Union.Case1 value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = value }
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

module ``{ optional=true } serialize with optional field`` =
    type Union = Case1 of field2:int option
    type UnionRecord = { field2: int option }
    type Input = { [<ParquetSingleCaseUnion(Optional = true)>] Field1: Union }
    type Output = { Field1: UnionRecord option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
                    Assert.Field.isOptional
                    Assert.Field.Type.isInt32
                    Assert.Field.LogicalType.isInteger 32 true
                    Assert.Field.ConvertedType.isInt32
                    Assert.Field.hasNoChildren ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Union>}' for which nulls are not allowed by default" @>)

    let NonNull = [|
        [| box<int option> <| (**) Option.None (**) |]
        [| box<int option> <| (**) Option.Some 1 (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Union.Case1 value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = value }
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

module ``{ optional=true } serialize with multiple fields`` =
    type Union = Case1 of field2:int * field3:bool * field4:float
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Input = { [<ParquetSingleCaseUnion(Optional = true)>] Field1: Union }
    type Output = { Field1: UnionRecord option }

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
                        Assert.Field.nameEquals "field2"
                        Assert.Field.isRequired
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "field3"
                        Assert.Field.isRequired
                        Assert.Field.Type.isBool
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "field4"
                        Assert.Field.isRequired
                        Assert.Field.Type.isFloat64
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Union>}' for which nulls are not allowed by default" @>)

    [<Fact>]
    let ``non-null`` () =
        let inputRecords = [| { Input.Field1 = Union.Case1 (1, true, 1.23) } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = 1; field3 = true; field4 = 1.23 }
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

module ``{ optional=true; allowNull=true } serialize with atomic field`` =
    type Union = Case1 of field2:int
    type UnionRecord = { field2: int }
    type Input = { [<ParquetSingleCaseUnion(Optional = true, AllowNull = true)>] Field1: Union }
    type Output = { Field1: UnionRecord option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
                    Assert.Field.isRequired
                    Assert.Field.Type.isInt32
                    Assert.Field.LogicalType.isInteger 32 true
                    Assert.Field.ConvertedType.isInt32
                    Assert.Field.hasNoChildren ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``non-null`` () =
        let inputRecords = [| { Input.Field1 = Union.Case1 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = 1 }
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

module ``{ optional=true; allowNull=true } serialize with list field`` =
    type Union = Case1 of field2:int list
    type UnionRecord = { field2: int list }
    type Input = { [<ParquetSingleCaseUnion(Optional = true, AllowNull = true)>] Field1: Union }
    type Output = { Field1: UnionRecord option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
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

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    let NonNull = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Union.Case1 value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = value }
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

module ``{ optional=true; allowNull=true } serialize with record field`` =
    type Record = { Field3: int }
    type Union = Case1 of field2:Record
    type UnionRecord = { field2: Record }
    type Input = { [<ParquetSingleCaseUnion(Optional = true, AllowNull = true)>] Field1: Union }
    type Output = { Field1: UnionRecord option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
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
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``non-null`` () =
        let value = { Record.Field3 = 1 }
        let inputRecords = [| { Input.Field1 = Union.Case1 value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = value }
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

module ``{ optional=true; allowNull=true } serialize with optional field`` =
    type Union = Case1 of field2:int option
    type UnionRecord = { field2: int option }
    type Input = { [<ParquetSingleCaseUnion(Optional = true, AllowNull = true)>] Field1: Union }
    type Output = { Field1: UnionRecord option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
                    Assert.Field.isOptional
                    Assert.Field.Type.isInt32
                    Assert.Field.LogicalType.isInteger 32 true
                    Assert.Field.ConvertedType.isInt32
                    Assert.Field.hasNoChildren ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    let NonNull = [|
        [| box<int option> <| (**) Option.None (**) |]
        [| box<int option> <| (**) Option.Some 1 (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Union.Case1 value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = value }
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

module ``{ optional=true; allowNull=true } serialize with multiple fields`` =
    type Union = Case1 of field2:int * field3:bool * field4:float
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Input = { [<ParquetSingleCaseUnion(Optional = true, AllowNull = true)>] Field1: Union }
    type Output = { Field1: UnionRecord option }

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
                        Assert.Field.nameEquals "field2"
                        Assert.Field.isRequired
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "field3"
                        Assert.Field.isRequired
                        Assert.Field.Type.isBool
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "field4"
                        Assert.Field.isRequired
                        Assert.Field.Type.isFloat64
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Union> } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``non-null`` () =
        let inputRecords = [| { Input.Field1 = Union.Case1 (1, true, 1.23) } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = 1; field3 = true; field4 = 1.23 }
        test <@ outputRecords = [| { Output.Field1 = Option.Some expectedValue } |] @>

module ``{ default } deserialize with atomic field`` =
    type UnionRecord = { field2: int }
    type Union = Case1 of field2:int
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    [<Fact>]
    let ``value`` () =
        let unionRecord = { UnionRecord.field2 = 1 }
        let inputRecords = [| { Input.Field1 = unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 1 } |] @>

module ``{ default } deserialize with list field`` =
    type UnionRecord = { field2: int list }
    type Union = Case1 of field2:int list
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    let Value = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let unionRecord = { UnionRecord.field2 = value }
        let inputRecords = [| { Input.Field1 = unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 value } |] @>

module ``{ default } deserialize with record field`` =
    type Record = { Field3: int }
    type UnionRecord = { field2: Record }
    type Union = Case1 of field2:Record
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    [<Fact>]
    let ``value`` () =
        let value = { Record.Field3 = 1 }
        let unionRecord = { UnionRecord.field2 = value }
        let inputRecords = [| { Input.Field1 = unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 value } |] @>

module ``{ default } deserialize with optional field`` =
    type UnionRecord = { field2: int option }
    type Union = Case1 of field2:int option
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    let Value = [|
        [| box<int option> <| (**) Option.None (**) |]
        [| box<int option> <| (**) Option.Some 1 (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let unionRecord = { UnionRecord.field2 = value }
        let inputRecords = [| { Input.Field1 = unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 value } |] @>

module ``{ default } deserialize with multiple fields`` =
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Union = Case1 of field2:int * field3:bool * field4:float
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    [<Fact>]
    let ``value`` () =
        let unionRecord = { UnionRecord.field2 = 1; field3 = true; field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 (1, true, 2.34) } |] @>

module ``{ default } deserialize with out-of-order fields`` =
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Union = Case1 of field3:bool * field4:float * field2:int
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    [<Fact>]
    let ``value`` () =
        let unionRecord = { UnionRecord.field2 = 1; field3 = true; field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 (true, 2.34, 1) } |] @>

module ``{ default } deserialize with subset of fields`` =
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Union = Case1 of field2:int * field4:float
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    [<Fact>]
    let ``value`` () =
        let unionRecord = { UnionRecord.field2 = 1; field3 = true; field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 (1, 2.34) } |] @>

module ``{ optional=true } deserialize with atomic field`` =
    type UnionRecord = { field2: int }
    type Union = Case1 of field2:int
    type Input = { Field1: UnionRecord option }
    type Output = { [<ParquetSingleCaseUnion(Optional = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"null value encountered during deserialization for type '{typeof<Union>}'"
                    + " for which nulls are not allowed by default" @>)

    [<Fact>]
    let ``non-null`` () =
        let unionRecord = { UnionRecord.field2 = 1 }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 1 } |] @>

module ``{ optional=true } deserialize with list field`` =
    type UnionRecord = { field2: int list }
    type Union = Case1 of field2:int list
    type Input = { Field1: UnionRecord option }
    type Output = { [<ParquetSingleCaseUnion(Optional = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"null value encountered during deserialization for type '{typeof<Union>}'"
                    + " for which nulls are not allowed by default" @>)

    let NonNull = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let unionRecord = { UnionRecord.field2 = value }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 value } |] @>

module ``{ optional=true } deserialize with record field`` =
    type Record = { Field3: int }
    type UnionRecord = { field2: Record }
    type Union = Case1 of field2:Record
    type Input = { Field1: UnionRecord option }
    type Output = { [<ParquetSingleCaseUnion(Optional = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"null value encountered during deserialization for type '{typeof<Union>}'"
                    + " for which nulls are not allowed by default" @>)

    [<Fact>]
    let ``non-null`` () =
        let value = { Record.Field3 = 1 }
        let unionRecord = { UnionRecord.field2 = value }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 value } |] @>

module ``{ optional=true } deserialize with optional field`` =
    type UnionRecord = { field2: int option }
    type Union = Case1 of field2:int option
    type Input = { Field1: UnionRecord option }
    type Output = { [<ParquetSingleCaseUnion(Optional = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"null value encountered during deserialization for type '{typeof<Union>}'"
                    + " for which nulls are not allowed by default" @>)

    let NonNull = [|
        [| box<int option> <| (**) Option.None (**) |]
        [| box<int option> <| (**) Option.Some 1 (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let unionRecord = { UnionRecord.field2 = value }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 value } |] @>

module ``{ optional=true } deserialize with multiple fields`` =
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Union = Case1 of field2:int * field3:bool * field4:float
    type Input = { Field1: UnionRecord option }
    type Output = { [<ParquetSingleCaseUnion(Optional = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"null value encountered during deserialization for type '{typeof<Union>}'"
                    + " for which nulls are not allowed by default" @>)

    [<Fact>]
    let ``non-null`` () =
        let unionRecord = { UnionRecord.field2 = 1; field3 = true; field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 (1, true, 2.34) } |] @>

module ``{ optional=true } deserialize with out-of-order fields`` =
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Union = Case1 of field3:bool * field4:float * field2:int
    type Input = { Field1: UnionRecord option }
    type Output = { [<ParquetSingleCaseUnion(Optional = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"null value encountered during deserialization for type '{typeof<Union>}'"
                    + " for which nulls are not allowed by default" @>)

    [<Fact>]
    let ``non-null`` () =
        let unionRecord = { UnionRecord.field2 = 1; field3 = true; field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 (true, 2.34, 1) } |] @>

module ``{ optional=true } deserialize with subset of fields`` =
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Union = Case1 of field2:int * field4:float
    type Input = { Field1: UnionRecord option }
    type Output = { [<ParquetSingleCaseUnion(Optional = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"null value encountered during deserialization for type '{typeof<Union>}'"
                    + " for which nulls are not allowed by default" @>)

    [<Fact>]
    let ``non-null`` () =
        let unionRecord = { UnionRecord.field2 = 1; field3 = true; field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 (1, 2.34) } |] @>

module ``{ optional=true; allowNull=true } deserialize with atomic field`` =
    type UnionRecord = { field2: int }
    type Union = Case1 of field2:int
    type Input = { Field1: UnionRecord option }
    type Output = { [<ParquetSingleCaseUnion(Optional = true, AllowNull = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Asserting against the entire output record or output record array
        // results in a null reference exception, presumably because there's an
        // assumption the union is not null. Instead, assert against the field.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = Unchecked.defaultof<Union> @>

    [<Fact>]
    let ``non-null`` () =
        let unionRecord = { UnionRecord.field2 = 1 }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 1 } |] @>

module ``{ optional=true; allowNull=true } deserialize with list field`` =
    type UnionRecord = { field2: int list }
    type Union = Case1 of field2:int list
    type Input = { Field1: UnionRecord option }
    type Output = { [<ParquetSingleCaseUnion(Optional = true, AllowNull = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Asserting against the entire output record or output record array
        // results in a null reference exception, presumably because there's an
        // assumption the union is not null. Instead, assert against the field.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = Unchecked.defaultof<Union> @>

    let NonNull = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let unionRecord = { UnionRecord.field2 = value }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 value } |] @>

module ``{ optional=true; allowNull=true } deserialize with record field`` =
    type Record = { Field3: int }
    type UnionRecord = { field2: Record }
    type Union = Case1 of field2:Record
    type Input = { Field1: UnionRecord option }
    type Output = { [<ParquetSingleCaseUnion(Optional = true, AllowNull = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Asserting against the entire output record or output record array
        // results in a null reference exception, presumably because there's an
        // assumption the union is not null. Instead, assert against the field.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = Unchecked.defaultof<Union> @>

    [<Fact>]
    let ``non-null`` () =
        let value = { Record.Field3 = 1 }
        let unionRecord = { UnionRecord.field2 = value }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 value } |] @>

module ``{ optional=true; allowNull=true } deserialize with optional field`` =
    type UnionRecord = { field2: int option }
    type Union = Case1 of field2:int option
    type Input = { Field1: UnionRecord option }
    type Output = { [<ParquetSingleCaseUnion(Optional = true, AllowNull = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Asserting against the entire output record or output record array
        // results in a null reference exception, presumably because there's an
        // assumption the union is not null. Instead, assert against the field.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = Unchecked.defaultof<Union> @>

    let NonNull = [|
        [| box<int option> <| (**) Option.None (**) |]
        [| box<int option> <| (**) Option.Some 1 (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let unionRecord = { UnionRecord.field2 = value }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 value } |] @>

module ``{ optional=true; allowNull=true } deserialize with multiple fields`` =
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Union = Case1 of field2:int * field3:bool * field4:float
    type Input = { Field1: UnionRecord option }
    type Output = { [<ParquetSingleCaseUnion(Optional = true, AllowNull = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Asserting against the entire output record or output record array
        // results in a null reference exception, presumably because there's an
        // assumption the union is not null. Instead, assert against the field.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = Unchecked.defaultof<Union> @>

    [<Fact>]
    let ``non-null`` () =
        let unionRecord = { UnionRecord.field2 = 1; field3 = true; field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 (1, true, 2.34) } |] @>

module ``{ optional=true; allowNull=true } deserialize with out-of-order fields`` =
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Union = Case1 of field3:bool * field4:float * field2:int
    type Input = { Field1: UnionRecord option }
    type Output = { [<ParquetSingleCaseUnion(Optional = true, AllowNull = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Asserting against the entire output record or output record array
        // results in a null reference exception, presumably because there's an
        // assumption the union is not null. Instead, assert against the field.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = Unchecked.defaultof<Union> @>

    [<Fact>]
    let ``non-null`` () =
        let unionRecord = { UnionRecord.field2 = 1; field3 = true; field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 (true, 2.34, 1) } |] @>

module ``{ optional=true; allowNull=true } deserialize with subset of fields`` =
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Union = Case1 of field2:int * field4:float
    type Input = { Field1: UnionRecord option }
    type Output = { [<ParquetSingleCaseUnion(Optional = true, AllowNull = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Asserting against the entire output record or output record array
        // results in a null reference exception, presumably because there's an
        // assumption the union is not null. Instead, assert against the field.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = Unchecked.defaultof<Union> @>

    [<Fact>]
    let ``non-null`` () =
        let unionRecord = { UnionRecord.field2 = 1; field3 = true; field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 (1, 2.34) } |] @>
