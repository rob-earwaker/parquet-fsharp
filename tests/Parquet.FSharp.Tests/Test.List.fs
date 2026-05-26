namespace Parquet.FSharp.Tests.List

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``{ default } serialize with atomic elements`` =
    type Input = { Field1: int list }
    type Output = { Field1: int list }

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
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<int list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<int list>.FullName}' for which nulls are not"
                    + " allowed by default" @>)

    // TODO: These are common across a lot of tests in this file. Can we just re-use?
    // Same for other files!
    let NonNull = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ default } serialize with list elements`` =
    type Input = { Field1: int list list }
    type Output = { Field1: int list list }

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
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<int list list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<int list list>.FullName}' for which nulls are"
                    + " not allowed by default" @>)

    let NonNull = [|
        [| box<int list list> (**) [] (**) |]
        [| box<int list list> (**) [ [] ] (**) |]
        [| box<int list list> (**) [ [ 1; 2; 3 ] ] (**) |]
        [| box<int list list> (**) [ []; []; [] ] (**) |]
        [| box<int list list> (**) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ default } serialize with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record list }
    type Output = { Field1: Record list }

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
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.child [
                            Assert.Field.nameEquals "Field2"
                            Assert.Field.isRequired
                            Assert.Field.Type.isInt32
                            Assert.Field.LogicalType.isInteger 32 true
                            Assert.Field.ConvertedType.isInt32
                            Assert.Field.hasNoChildren ] ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Record list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Record list>.FullName}' for which nulls are"
                    + " not allowed by default" @>)

    let NonNull = [|
        [| box<Record list> (**) [] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 } ] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 }; { Field2 = 2 } ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ default } serialize with optional elements`` =
    type Input = { Field1: int option list }
    type Output = { Field1: int option list }

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
                        Assert.Field.isOptional
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<int option list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<int option list>.FullName}' for which nulls"
                    + " are not allowed by default" @>)

    let NonNull = [|
        [| box<int option list> (**) [] (**) |]
        [| box<int option list> (**) [ Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1 ] (**) |]
        [| box<int option list> (**) [ Option.None; Option.None; Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.None; Option.Some 3 ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.Some 2 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } serialize with atomic elements`` =
    type Input = { [<ParquetListField(Optional = true)>] Field1: int list }
    type Output = { Field1: int list option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<int list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<int list>.FullName}' for which nulls are not"
                    + " allowed by default" @>)

    let NonNull = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ optional=true } serialize with list elements`` =
    type Input = { [<ParquetListField(Optional = true)>] Field1: int list list }
    type Output = { Field1: int list list option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<int list list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<int list list>.FullName}' for which nulls are"
                    + " not allowed by default" @>)

    let NonNull = [|
        [| box<int list list> (**) [] (**) |]
        [| box<int list list> (**) [ [] ] (**) |]
        [| box<int list list> (**) [ [ 1; 2; 3 ] ] (**) |]
        [| box<int list list> (**) [ []; []; [] ] (**) |]
        [| box<int list list> (**) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ optional=true } serialize with record elements`` =
    type Record = { Field2: int }
    type Input = { [<ParquetListField(Optional = true)>] Field1: Record list }
    type Output = { Field1: Record list option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.child [
                            Assert.Field.nameEquals "Field2"
                            Assert.Field.isRequired
                            Assert.Field.Type.isInt32
                            Assert.Field.LogicalType.isInteger 32 true
                            Assert.Field.ConvertedType.isInt32
                            Assert.Field.hasNoChildren ] ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Record list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Record list>.FullName}' for which nulls are"
                    + " not allowed by default" @>)

    let NonNull = [|
        [| box<Record list> (**) [] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 } ] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 }; { Field2 = 2 } ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ optional=true } serialize with optional elements`` =
    type Input = { [<ParquetListField(Optional = true)>] Field1: int option list }
    type Output = { Field1: int option list option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
                        Assert.Field.isOptional
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<int option list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<int option list>.FullName}' for which nulls"
                    + " are not allowed by default" @>)

    let NonNull = [|
        [| box<int option list> (**) [] (**) |]
        [| box<int option list> (**) [ Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1 ] (**) |]
        [| box<int option list> (**) [ Option.None; Option.None; Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.None; Option.Some 3 ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.Some 2 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ optional=true; allowNull=true } serialize with atomic elements`` =
    type Input = { [<ParquetListField(Optional = true, AllowNull = true)>] Field1: int list }
    type Output = { Field1: int list option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<int list> } |]
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
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ optional=true; allowNull=true } serialize with list elements`` =
    type Input = { [<ParquetListField(Optional = true, AllowNull = true)>] Field1: int list list }
    type Output = { Field1: int list list option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<int list list> } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    let NonNull = [|
        [| box<int list list> (**) [] (**) |]
        [| box<int list list> (**) [ [] ] (**) |]
        [| box<int list list> (**) [ [ 1; 2; 3 ] ] (**) |]
        [| box<int list list> (**) [ []; []; [] ] (**) |]
        [| box<int list list> (**) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ optional=true; allowNull=true } serialize with record elements`` =
    type Record = { Field2: int }
    type Input = { [<ParquetListField(Optional = true, AllowNull = true)>] Field1: Record list }
    type Output = { Field1: Record list option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.child [
                            Assert.Field.nameEquals "Field2"
                            Assert.Field.isRequired
                            Assert.Field.Type.isInt32
                            Assert.Field.LogicalType.isInteger 32 true
                            Assert.Field.ConvertedType.isInt32
                            Assert.Field.hasNoChildren ] ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Record list> } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    let NonNull = [|
        [| box<Record list> (**) [] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 } ] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 }; { Field2 = 2 } ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ optional=true; allowNull=true } serialize with optional elements`` =
    type Input = { [<ParquetListField(Optional = true, AllowNull = true)>] Field1: int option list }
    type Output = { Field1: int option list option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
                        Assert.Field.isOptional
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<int option list> } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    let NonNull = [|
        [| box<int option list> (**) [] (**) |]
        [| box<int option list> (**) [ Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1 ] (**) |]
        [| box<int option list> (**) [ Option.None; Option.None; Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.None; Option.Some 3 ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.Some 2 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ allowNull=true } serialize with atomic elements`` =
    type Input = { [<ParquetListField(AllowNull = true)>] Field1: int list }
    type Output = { Field1: int list }

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
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<int list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<int list>.FullName}' which is not optional"
                    + " by default" @>)

    let NonNull = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ allowNull=true } serialize with list elements`` =
    type Input = { [<ParquetListField(AllowNull = true)>] Field1: int list list }
    type Output = { Field1: int list list }

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
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<int list list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<int list list>.FullName}' which is not"
                    + " optional by default" @>)

    let NonNull = [|
        [| box<int list list> (**) [] (**) |]
        [| box<int list list> (**) [ [] ] (**) |]
        [| box<int list list> (**) [ [ 1; 2; 3 ] ] (**) |]
        [| box<int list list> (**) [ []; []; [] ] (**) |]
        [| box<int list list> (**) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ allowNull=true } serialize with record elements`` =
    type Record = { Field2: int }
    type Input = { [<ParquetListField(AllowNull = true)>] Field1: Record list }
    type Output = { Field1: Record list }

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
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.child [
                            Assert.Field.nameEquals "Field2"
                            Assert.Field.isRequired
                            Assert.Field.Type.isInt32
                            Assert.Field.LogicalType.isInteger 32 true
                            Assert.Field.ConvertedType.isInt32
                            Assert.Field.hasNoChildren ] ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<Record list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Record list>.FullName}' which is not optional"
                    + " by default" @>)

    let NonNull = [|
        [| box<Record list> (**) [] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 } ] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 }; { Field2 = 2 } ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ allowNull=true } serialize with optional elements`` =
    type Input = { [<ParquetListField(AllowNull = true)>] Field1: int option list }
    type Output = { Field1: int option list }

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
                        Assert.Field.isOptional
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<int option list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<int option list>.FullName}' which is not"
                    + " optional by default" @>)

    let NonNull = [|
        [| box<int option list> (**) [] (**) |]
        [| box<int option list> (**) [ Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1 ] (**) |]
        [| box<int option list> (**) [ Option.None; Option.None; Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.None; Option.Some 3 ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.Some 2 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ default } deserialize with atomic elements`` =
    type Input = { Field1: int list }
    type Output = { Field1: int list }

    let Value = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ default } deserialize with list elements`` =
    type Input = { Field1: int list list }
    type Output = { Field1: int list list }

    let Value = [|
        [| box<int list list> (**) [] (**) |]
        [| box<int list list> (**) [ [] ] (**) |]
        [| box<int list list> (**) [ [ 1; 2; 3 ] ] (**) |]
        [| box<int list list> (**) [ []; []; [] ] (**) |]
        [| box<int list list> (**) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ default } deserialize with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record list }
    type Output = { Field1: Record list }

    let Value = [|
        [| box<Record list> (**) [] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 } ] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 }; { Field2 = 2 } ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ default } deserialize with optional elements`` =
    type Input = { Field1: int option list }
    type Output = { Field1: int option list }

    let Value = [|
        [| box<int option list> (**) [] (**) |]
        [| box<int option list> (**) [ Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1 ] (**) |]
        [| box<int option list> (**) [ Option.None; Option.None; Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.None; Option.Some 3 ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.Some 2 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } deserialize with atomic elements`` =
    type Input = { Field1: int list option }
    type Output = { [<ParquetListField(Optional = true)>] Field1: int list }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<int list>.FullName}' for which nulls are not"
                    + " allowed by default" @>)

    let NonNull = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } deserialize with list elements`` =
    type Input = { Field1: int list list option }
    type Output = { [<ParquetListField(Optional = true)>] Field1: int list list }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<int list list>.FullName}' for which nulls are"
                    + " not allowed by default" @>)

    let NonNull = [|
        [| box<int list list> (**) [] (**) |]
        [| box<int list list> (**) [ [] ] (**) |]
        [| box<int list list> (**) [ [ 1; 2; 3 ] ] (**) |]
        [| box<int list list> (**) [ []; []; [] ] (**) |]
        [| box<int list list> (**) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } deserialize with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record list option }
    type Output = { [<ParquetListField(Optional = true)>] Field1: Record list }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<Record list>.FullName}' for which nulls are"
                    + " not allowed by default" @>)

    let NonNull = [|
        [| box<Record list> (**) [] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 } ] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 }; { Field2 = 2 } ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } deserialize with optional elements`` =
    type Input = { Field1: int option list option }
    type Output = { [<ParquetListField(Optional = true)>] Field1: int option list }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<int option list>.FullName}' for which nulls"
                    + " are not allowed by default" @>)

    let NonNull = [|
        [| box<int option list> (**) [] (**) |]
        [| box<int option list> (**) [ Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1 ] (**) |]
        [| box<int option list> (**) [ Option.None; Option.None; Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.None; Option.Some 3 ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.Some 2 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true; allowNull=true } deserialize with atomic elements`` =
    type Input = { Field1: int list option }
    type Output = { [<ParquetListField(Optional = true, AllowNull = true)>] Field1: int list }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Asserting against the entire output record or output record array
        // results in a null reference exception, presumably because there's an
        // assumption the list is not null. Instead, assert against the field.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = Unchecked.defaultof<int list> @>

    let NonNull = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true; allowNull=true } deserialize with list elements`` =
    type Input = { Field1: int list list option }
    type Output = { [<ParquetListField(Optional = true, AllowNull = true)>] Field1: int list list }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Asserting against the entire output record or output record array
        // results in a null reference exception, presumably because there's an
        // assumption the list is not null. Instead, assert against the field.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = Unchecked.defaultof<int list list> @>

    let NonNull = [|
        [| box<int list list> (**) [] (**) |]
        [| box<int list list> (**) [ [] ] (**) |]
        [| box<int list list> (**) [ [ 1; 2; 3 ] ] (**) |]
        [| box<int list list> (**) [ []; []; [] ] (**) |]
        [| box<int list list> (**) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true; allowNull=true } deserialize with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record list option }
    type Output = { [<ParquetListField(Optional = true, AllowNull = true)>] Field1: Record list }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Asserting against the entire output record or output record array
        // results in a null reference exception, presumably because there's an
        // assumption the list is not null. Instead, assert against the field.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = Unchecked.defaultof<Record list> @>

    let NonNull = [|
        [| box<Record list> (**) [] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 } ] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 }; { Field2 = 2 } ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true; allowNull=true } deserialize with optional elements`` =
    type Input = { Field1: int option list option }
    type Output = { [<ParquetListField(Optional = true, AllowNull = true)>] Field1: int option list }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        // Asserting against the entire output record or output record array
        // results in a null reference exception, presumably because there's an
        // assumption the list is not null. Instead, assert against the field.
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1 = Unchecked.defaultof<int option list> @>

    let NonNull = [|
        [| box<int option list> (**) [] (**) |]
        [| box<int option list> (**) [ Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1 ] (**) |]
        [| box<int option list> (**) [ Option.None; Option.None; Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.None; Option.Some 3 ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.Some 2 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ allowNull=true } deserialize with atomic elements`` =
    type Input = { Field1: int list }
    type Output = { [<ParquetListField(AllowNull = true)>] Field1: int list }

    let Value = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ allowNull=true } deserialize with list elements`` =
    type Input = { Field1: int list list }
    type Output = { [<ParquetListField(AllowNull = true)>] Field1: int list list }

    let Value = [|
        [| box<int list list> (**) [] (**) |]
        [| box<int list list> (**) [ [] ] (**) |]
        [| box<int list list> (**) [ [ 1; 2; 3 ] ] (**) |]
        [| box<int list list> (**) [ []; []; [] ] (**) |]
        [| box<int list list> (**) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ allowNull=true } deserialize with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record list }
    type Output = { [<ParquetListField(AllowNull = true)>] Field1: Record list }

    let Value = [|
        [| box<Record list> (**) [] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 } ] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 }; { Field2 = 2 } ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ allowNull=true } deserialize with optional elements`` =
    type Input = { Field1: int option list }
    type Output = { [<ParquetListField(AllowNull = true)>] Field1: int option list }

    let Value = [|
        [| box<int option list> (**) [] (**) |]
        [| box<int option list> (**) [ Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1 ] (**) |]
        [| box<int option list> (**) [ Option.None; Option.None; Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.None; Option.Some 3 ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.Some 2 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
