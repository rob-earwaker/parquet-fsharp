namespace Parquet.FSharp.Tests.ResizeArray

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``{ default } serialize with atomic elements`` =
    type Input = { Field1: ResizeArray<int> }
    type Output = { Field1: ResizeArray<int> }

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
                    + $" '{typeof<ResizeArray<int>>}' which is not optional by default" @>)

    let NonNull = [|
        [| box <| (**) ResizeArray<int>() (**) |]
        [| box <| (**) ResizeArray<int>([ 1 ]) (**) |]
        [| box <| (**) ResizeArray<int>([ 1; 2; 3 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ default } serialize with list elements`` =
    type Input = { Field1: ResizeArray<int list> }
    type Output = { Field1: ResizeArray<int list> }

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
                    + $" '{typeof<ResizeArray<int list>>}' which is not optional by default" @>)

    let NonNull = [|
        [| box <| (**) ResizeArray<int list>() (**) |]
        [| box <| (**) ResizeArray<int list>([ [] ]) (**) |]
        [| box <| (**) ResizeArray<int list>([ [ 1; 2; 3 ] ]) (**) |]
        [| box <| (**) ResizeArray<int list>([ []; []; [] ]) (**) |]
        [| box <| (**) ResizeArray<int list>([ [ 1 ]; []; [ 2; 3; 4 ] ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ default } serialize with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: ResizeArray<Record> }
    type Output = { Field1: ResizeArray<Record> }

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
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<ResizeArray<Record>>}' which is not optional by default" @>)

    let NonNull = [|
        [| box <| (**) ResizeArray<Record>() (**) |]
        [| box <| (**) ResizeArray<Record>([ { Field2 = 1 } ]) (**) |]
        [| box <| (**) ResizeArray<Record>([ { Field2 = 1 }; { Field2 = 2 } ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ default } serialize with optional elements`` =
    type Input = { Field1: ResizeArray<int option> }
    type Output = { Field1: ResizeArray<int option> }

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
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<ResizeArray<int option>>}' which is not optional by default" @>)

    let NonNull = [|
        [| box <| (**) ResizeArray<int option>() (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.None ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.Some 1 ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.None; Option.None; Option.None ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.Some 1; Option.None; Option.Some 3 ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.Some 1; Option.Some 2 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ allowNull=true } serialize with atomic elements`` =
    type Input = { [<ParquetResizeArray(AllowNull = true)>] Field1: ResizeArray<int> }
    type Output = { Field1: ResizeArray<int> }

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
                    + $" '{typeof<ResizeArray<int>>}' which is not optional by default" @>)

    let NonNull = [|
        [| box <| (**) ResizeArray<int>() (**) |]
        [| box <| (**) ResizeArray<int>([ 1 ]) (**) |]
        [| box <| (**) ResizeArray<int>([ 1; 2; 3 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ allowNull=true } serialize with list elements`` =
    type Input = { [<ParquetResizeArray(AllowNull = true)>] Field1: ResizeArray<int list> }
    type Output = { Field1: ResizeArray<int list> }

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
                    + $" '{typeof<ResizeArray<int list>>}' which is not optional by default" @>)

    let NonNull = [|
        [| box <| (**) ResizeArray<int list>() (**) |]
        [| box <| (**) ResizeArray<int list>([ [] ]) (**) |]
        [| box <| (**) ResizeArray<int list>([ [ 1; 2; 3 ] ]) (**) |]
        [| box <| (**) ResizeArray<int list>([ []; []; [] ]) (**) |]
        [| box <| (**) ResizeArray<int list>([ [ 1 ]; []; [ 2; 3; 4 ] ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ allowNull=true } serialize with record elements`` =
    type Record = { Field2: int }
    type Input = { [<ParquetResizeArray(AllowNull = true)>] Field1: ResizeArray<Record> }
    type Output = { Field1: ResizeArray<Record> }

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
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<ResizeArray<Record>>}' which is not optional by default" @>)

    let NonNull = [|
        [| box <| (**) ResizeArray<Record>() (**) |]
        [| box <| (**) ResizeArray<Record>([ { Field2 = 1 } ]) (**) |]
        [| box <| (**) ResizeArray<Record>([ { Field2 = 1 }; { Field2 = 2 } ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ allowNull=true } serialize with optional elements`` =
    type Input = { [<ParquetResizeArray(AllowNull = true)>] Field1: ResizeArray<int option> }
    type Output = { Field1: ResizeArray<int option> }

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
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<ResizeArray<int option>>}' which is not optional by default" @>)

    let NonNull = [|
        [| box <| (**) ResizeArray<int option>() (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.None ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.Some 1 ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.None; Option.None; Option.None ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.Some 1; Option.None; Option.Some 3 ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.Some 1; Option.Some 2 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ optional=true } serialize with atomic elements`` =
    type Input = { [<ParquetResizeArray(Optional = true)>] Field1: ResizeArray<int> }
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
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<ResizeArray<int>>}' for which nulls are not allowed by default" @>)

    let NonNull = [|
        [| box <| (* inputValue *) ResizeArray<int>() (**);
            box<int list> <| (* outputValue *) [] (**) |]

        [| box <| (* inputValue *) ResizeArray<int>([ 1 ]) (**);
            box<int list> <| (* outputValue *) [ 1 ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int>([ 1; 2; 3 ]) (**);
            box<int list> <| (* outputValue *) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some outputValue } |] @>

module ``{ optional=true } serialize with list elements`` =
    type Input = { [<ParquetResizeArray(Optional = true)>] Field1: ResizeArray<int list> }
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
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<ResizeArray<int list>>}' for which nulls are not allowed by default" @>)

    let NonNull = [|
        [| box <| (* inputValue *) ResizeArray<int list>() (**);
            box<int list list> <| (* outputValue *) [] (**) |]

        [| box <| (* inputValue *) ResizeArray<int list>([ [] ]) (**);
            box<int list list> <| (* outputValue *) [ [] ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int list>([ [ 1; 2; 3 ] ]) (**);
            box<int list list> <| (* outputValue *) [ [ 1; 2; 3 ] ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int list>([ []; []; [] ]) (**);
            box<int list list> <| (* outputValue *) [ []; []; [] ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int list>([ [ 1 ]; []; [ 2; 3; 4 ] ]) (**);
            box<int list list> <| (* outputValue *) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some outputValue } |] @>

module ``{ optional=true } serialize with record elements`` =
    type Record = { Field2: int }
    type Input = { [<ParquetResizeArray(Optional = true)>] Field1: ResizeArray<Record> }
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
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<ResizeArray<Record>>}' for which nulls are not allowed by default" @>)

    let NonNull = [|
        [| box <| (* inputValue *) ResizeArray<Record>() (**);
            box<Record list> <| (* outputValue *) [] (**) |]

        [| box <| (* inputValue *) ResizeArray<Record>([ { Field2 = 1 } ]) (**);
            box<Record list> <| (* outputValue *) [ { Field2 = 1 } ] (**) |]

        [| box <| (* inputValue *) ResizeArray<Record>([ { Field2 = 1 }; { Field2 = 2 } ]) (**);
            box<Record list> <| (* outputValue *) [ { Field2 = 1 }; { Field2 = 2 } ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some outputValue } |] @>

module ``{ optional=true } serialize with optional elements`` =
    type Input = { [<ParquetResizeArray(Optional = true)>] Field1: ResizeArray<int option> }
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
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<ResizeArray<int option>>}' for which nulls are not allowed by default" @>)

    let NonNull = [|
        [| box <| (* inputValue *) ResizeArray<int option>() (**);
            box<int option list> <| (* outputValue *) [] (**) |]

        [| box <| (* inputValue *) ResizeArray<int option>([ Option.None ]) (**);
            box<int option list> <| (* outputValue *) [ Option.None ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int option>([ Option.Some 1 ]) (**);
            box<int option list> <| (* outputValue *) [ Option.Some 1 ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int option>([ Option.None; Option.None; Option.None ]) (**);
            box<int option list> <| (* outputValue *) [ Option.None; Option.None; Option.None ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int option>([ Option.Some 1; Option.None; Option.Some 3 ]) (**);
            box<int option list> <| (* outputValue *) [ Option.Some 1; Option.None; Option.Some 3 ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int option>([ Option.Some 1; Option.Some 2 ]) (**);
            box<int option list> <| (* outputValue *) [ Option.Some 1; Option.Some 2 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some outputValue } |] @>

module ``{ optional=true; allowNull=true } serialize with atomic elements`` =
    type Input = {
        [<ParquetResizeArray(Optional = true, AllowNull = true)>]
        Field1: ResizeArray<int> }

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
        let inputRecords = [| { Input.Field1 = null } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = Option.None } |]
        Assert.seqEqual expectedRecords outputRecords

    let NonNull = [|
        [| box <| (* inputValue *) ResizeArray<int>() (**);
            box<int list> <| (* outputValue *) [] (**) |]

        [| box <| (* inputValue *) ResizeArray<int>([ 1 ]) (**);
            box<int list> <| (* outputValue *) [ 1 ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int>([ 1; 2; 3 ]) (**);
            box<int list> <| (* outputValue *) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some outputValue } |] @>

module ``{ optional=true; allowNull=true } serialize with list elements`` =
    type Input = {
        [<ParquetResizeArray(Optional = true, AllowNull = true)>]
        Field1: ResizeArray<int list> }

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
        let inputRecords = [| { Input.Field1 = null } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = Option.None } |]
        Assert.seqEqual expectedRecords outputRecords

    let NonNull = [|
        [| box <| (* inputValue *) ResizeArray<int list>() (**);
            box<int list list> <| (* outputValue *) [] (**) |]

        [| box <| (* inputValue *) ResizeArray<int list>([ [] ]) (**);
            box<int list list> <| (* outputValue *) [ [] ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int list>([ [ 1; 2; 3 ] ]) (**);
            box<int list list> <| (* outputValue *) [ [ 1; 2; 3 ] ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int list>([ []; []; [] ]) (**);
            box<int list list> <| (* outputValue *) [ []; []; [] ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int list>([ [ 1 ]; []; [ 2; 3; 4 ] ]) (**);
            box<int list list> <| (* outputValue *) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some outputValue } |] @>

module ``{ optional=true; allowNull=true } serialize with record elements`` =
    type Record = { Field2: int }

    type Input = {
        [<ParquetResizeArray(Optional = true, AllowNull = true)>]
        Field1: ResizeArray<Record> }

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
        let inputRecords = [| { Input.Field1 = null } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = Option.None } |]
        Assert.seqEqual expectedRecords outputRecords

    let NonNull = [|
        [| box <| (* inputValue *) ResizeArray<Record>() (**);
            box<Record list> <| (* outputValue *) [] (**) |]

        [| box <| (* inputValue *) ResizeArray<Record>([ { Field2 = 1 } ]) (**);
            box<Record list> <| (* outputValue *) [ { Field2 = 1 } ] (**) |]

        [| box <| (* inputValue *) ResizeArray<Record>([ { Field2 = 1 }; { Field2 = 2 } ]) (**);
            box<Record list> <| (* outputValue *) [ { Field2 = 1 }; { Field2 = 2 } ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some outputValue } |] @>

module ``{ optional=true; allowNull=true } serialize with optional elements`` =
    type Input = {
        [<ParquetResizeArray(Optional = true, AllowNull = true)>]
        Field1: ResizeArray<int option> }

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
        let inputRecords = [| { Input.Field1 = null } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = Option.None } |]
        Assert.seqEqual expectedRecords outputRecords

    let NonNull = [|
        [| box <| (* inputValue *) ResizeArray<int option>() (**);
            box<int option list> <| (* outputValue *) [] (**) |]

        [| box <| (* inputValue *) ResizeArray<int option>([ Option.None ]) (**);
            box<int option list> <| (* outputValue *) [ Option.None ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int option>([ Option.Some 1 ]) (**);
            box<int option list> <| (* outputValue *) [ Option.Some 1 ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int option>([ Option.None; Option.None; Option.None ]) (**);
            box<int option list> <| (* outputValue *) [ Option.None; Option.None; Option.None ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int option>([ Option.Some 1; Option.None; Option.Some 3 ]) (**);
            box<int option list> <| (* outputValue *) [ Option.Some 1; Option.None; Option.Some 3 ] (**) |]

        [| box <| (* inputValue *) ResizeArray<int option>([ Option.Some 1; Option.Some 2 ]) (**);
            box<int option list> <| (* outputValue *) [ Option.Some 1; Option.Some 2 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some outputValue } |] @>

module ``{ default } deserialize with atomic elements`` =
    type Input = { Field1: int list }
    type Output = { Field1: ResizeArray<int> }

    let Value = [|
        [| box<int list> (* inputValue *) [] (**);
            box <| (* outputValue *) ResizeArray<int>() (**) |]

        [| box<int list> (* inputValue *) [ 1 ] (**);
            box <| (* outputValue *) ResizeArray<int>([ 1 ]) (**) |]

        [| box<int list> (* inputValue *) [ 1; 2; 3 ] (**);
            box <| (* outputValue *) ResizeArray<int>([ 1; 2; 3 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = outputValue } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ default } deserialize with list elements`` =
    type Input = { Field1: int list list }
    type Output = { Field1: ResizeArray<int list> }

    let Value = [|
        [| box<int list list> (* inputValue *) [] (**);
            box <| (* outputValue*) ResizeArray<int list>() (**) |]

        [| box<int list list> (* inputValue *) [ [] ] (**);
            box <| (* outputValue*) ResizeArray<int list>([ [] ]) (**) |]

        [| box<int list list> (* inputValue *) [ [ 1; 2; 3 ] ] (**);
            box <| (* outputValue*) ResizeArray<int list>([ [ 1; 2; 3 ] ]) (**) |]

        [| box<int list list> (* inputValue *) [ []; []; [] ] (**);
            box <| (* outputValue*) ResizeArray<int list>([ []; []; [] ]) (**) |]

        [| box<int list list> (* inputValue *) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**);
            box <| (* outputValue*) ResizeArray<int list>([ [ 1 ]; []; [ 2; 3; 4 ] ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = outputValue } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ default } deserialize with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record list }
    type Output = { Field1: ResizeArray<Record> }

    let Value = [|
        [| box<Record list> (* inputValue *) [] (**);
            box <| (* outputValue *) ResizeArray<Record>() (**) |]

        [| box<Record list> (* inputValue *) [ { Field2 = 1 } ] (**);
            box <| (* outputValue *) ResizeArray<Record>([ { Field2 = 1 } ]) (**) |]

        [| box<Record list> (* inputValue *) [ { Field2 = 1 }; { Field2 = 2 } ] (**);
            box <| (* outputValue *) ResizeArray<Record>([ { Field2 = 1 }; { Field2 = 2 } ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = outputValue } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ default } deserialize with optional elements`` =
    type Input = { Field1: int option list }
    type Output = { Field1: ResizeArray<int option> }

    let Value = [|
        [| box<int option list> (* inputValue *) [] (**);
            box <| (* outputValue *) ResizeArray<int option>() (**) |]

        [| box<int option list> (* inputValue *) [ Option.None ] (**);
            box <| (* outputValue *) ResizeArray<int option>([ Option.None ]) (**) |]

        [| box<int option list> (* inputValue *) [ Option.Some 1 ] (**);
            box <| (* outputValue *) ResizeArray<int option>([ Option.Some 1 ]) (**) |]

        [| box<int option list> (* inputValue *) [ Option.None; Option.None; Option.None ] (**);
            box <| (* outputValue *) ResizeArray<int option>([ Option.None; Option.None; Option.None ]) (**) |]

        [| box<int option list> (* inputValue *) [ Option.Some 1; Option.None; Option.Some 3 ] (**);
            box <| (* outputValue *) ResizeArray<int option>([ Option.Some 1; Option.None; Option.Some 3 ]) (**) |]

        [| box<int option list> (* inputValue *) [ Option.Some 1; Option.Some 2 ] (**);
            box <| (* outputValue *) ResizeArray<int option>([ Option.Some 1; Option.Some 2 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = outputValue } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ optional=true } deserialize with atomic elements`` =
    type Input = { Field1: int list option }
    type Output = { [<ParquetResizeArray(Optional = true)>] Field1: ResizeArray<int> }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<ResizeArray<int>>}' for which nulls"
                    + " are not allowed by default" @>)

    let NonNull = [|
        [| box<int list> (* inputValue *) [] (**);
            box <| (* outputValue *) ResizeArray<int>() (**) |]

        [| box<int list> (* inputValue *) [ 1 ] (**);
            box <| (* outputValue *) ResizeArray<int>([ 1 ]) (**) |]

        [| box<int list> (* inputValue *) [ 1; 2; 3 ] (**);
            box <| (* outputValue *) ResizeArray<int>([ 1; 2; 3 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = outputValue } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ optional=true } deserialize with list elements`` =
    type Input = { Field1: int list list option }
    type Output = { [<ParquetResizeArray(Optional = true)>] Field1: ResizeArray<int list> }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<ResizeArray<int list>>}' for which"
                    + " nulls are not allowed by default" @>)

    let NonNull = [|
        [| box<int list list> (* inputValue *) [] (**);
            box <| (* outputValue*) ResizeArray<int list>() (**) |]

        [| box<int list list> (* inputValue *) [ [] ] (**);
            box <| (* outputValue*) ResizeArray<int list>([ [] ]) (**) |]

        [| box<int list list> (* inputValue *) [ [ 1; 2; 3 ] ] (**);
            box <| (* outputValue*) ResizeArray<int list>([ [ 1; 2; 3 ] ]) (**) |]

        [| box<int list list> (* inputValue *) [ []; []; [] ] (**);
            box <| (* outputValue*) ResizeArray<int list>([ []; []; [] ]) (**) |]

        [| box<int list list> (* inputValue *) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**);
            box <| (* outputValue*) ResizeArray<int list>([ [ 1 ]; []; [ 2; 3; 4 ] ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = outputValue } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ optional=true } deserialize with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record list option }
    type Output = { [<ParquetResizeArray(Optional = true)>] Field1: ResizeArray<Record> }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<ResizeArray<Record>>}' for which"
                    + " nulls are not allowed by default" @>)

    let NonNull = [|
        [| box<Record list> (* inputValue *) [] (**);
            box <| (* outputValue *) ResizeArray<Record>() (**) |]

        [| box<Record list> (* inputValue *) [ { Field2 = 1 } ] (**);
            box <| (* outputValue *) ResizeArray<Record>([ { Field2 = 1 } ]) (**) |]

        [| box<Record list> (* inputValue *) [ { Field2 = 1 }; { Field2 = 2 } ] (**);
            box <| (* outputValue *) ResizeArray<Record>([ { Field2 = 1 }; { Field2 = 2 } ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = outputValue } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ optional=true } deserialize with optional elements`` =
    type Input = { Field1: int option list option }
    type Output = { [<ParquetResizeArray(Optional = true)>] Field1: ResizeArray<int option> }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<ResizeArray<int option>>}' for which"
                    + " nulls are not allowed by default" @>)

    let NonNull = [|
        [| box<int option list> (* inputValue *) [] (**);
            box <| (* outputValue *) ResizeArray<int option>() (**) |]

        [| box<int option list> (* inputValue *) [ Option.None ] (**);
            box <| (* outputValue *) ResizeArray<int option>([ Option.None ]) (**) |]

        [| box<int option list> (* inputValue *) [ Option.Some 1 ] (**);
            box <| (* outputValue *) ResizeArray<int option>([ Option.Some 1 ]) (**) |]

        [| box<int option list> (* inputValue *) [ Option.None; Option.None; Option.None ] (**);
            box <| (* outputValue *) ResizeArray<int option>([ Option.None; Option.None; Option.None ]) (**) |]

        [| box<int option list> (* inputValue *) [ Option.Some 1; Option.None; Option.Some 3 ] (**);
            box <| (* outputValue *) ResizeArray<int option>([ Option.Some 1; Option.None; Option.Some 3 ]) (**) |]

        [| box<int option list> (* inputValue *) [ Option.Some 1; Option.Some 2 ] (**);
            box <| (* outputValue *) ResizeArray<int option>([ Option.Some 1; Option.Some 2 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = outputValue } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ optional=true; allowNull=true } deserialize with atomic elements`` =
    type Input = { Field1: int list option }

    type Output = {
        [<ParquetResizeArray(Optional = true, AllowNull = true)>]
        Field1: ResizeArray<int> }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = null } |]
        Assert.seqEqual expectedRecords outputRecords

    let NonNull = [|
        [| box<int list> (* inputValue *) [] (**);
            box <| (* outputValue *) ResizeArray<int>() (**) |]

        [| box<int list> (* inputValue *) [ 1 ] (**);
            box <| (* outputValue *) ResizeArray<int>([ 1 ]) (**) |]

        [| box<int list> (* inputValue *) [ 1; 2; 3 ] (**);
            box <| (* outputValue *) ResizeArray<int>([ 1; 2; 3 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = outputValue } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ optional=true; allowNull=true } deserialize with list elements`` =
    type Input = { Field1: int list list option }

    type Output = {
        [<ParquetResizeArray(Optional = true, AllowNull = true)>]
        Field1: ResizeArray<int list> }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = null } |]
        Assert.seqEqual expectedRecords outputRecords

    let NonNull = [|
        [| box<int list list> (* inputValue *) [] (**);
            box <| (* outputValue*) ResizeArray<int list>() (**) |]

        [| box<int list list> (* inputValue *) [ [] ] (**);
            box <| (* outputValue*) ResizeArray<int list>([ [] ]) (**) |]

        [| box<int list list> (* inputValue *) [ [ 1; 2; 3 ] ] (**);
            box <| (* outputValue*) ResizeArray<int list>([ [ 1; 2; 3 ] ]) (**) |]

        [| box<int list list> (* inputValue *) [ []; []; [] ] (**);
            box <| (* outputValue*) ResizeArray<int list>([ []; []; [] ]) (**) |]

        [| box<int list list> (* inputValue *) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**);
            box <| (* outputValue*) ResizeArray<int list>([ [ 1 ]; []; [ 2; 3; 4 ] ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = outputValue } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ optional=true; allowNull=true } deserialize with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record list option }

    type Output = {
        [<ParquetResizeArray(Optional = true, AllowNull = true)>]
        Field1: ResizeArray<Record> }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = null } |]
        Assert.seqEqual expectedRecords outputRecords

    let NonNull = [|
        [| box<Record list> (* inputValue *) [] (**);
            box <| (* outputValue *) ResizeArray<Record>() (**) |]

        [| box<Record list> (* inputValue *) [ { Field2 = 1 } ] (**);
            box <| (* outputValue *) ResizeArray<Record>([ { Field2 = 1 } ]) (**) |]

        [| box<Record list> (* inputValue *) [ { Field2 = 1 }; { Field2 = 2 } ] (**);
            box <| (* outputValue *) ResizeArray<Record>([ { Field2 = 1 }; { Field2 = 2 } ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = outputValue } |]
        Assert.seqEqual expectedRecords outputRecords

module ``{ optional=true; allowNull=true } deserialize with optional elements`` =
    type Input = { Field1: int option list option }

    type Output = {
        [<ParquetResizeArray(Optional = true, AllowNull = true)>]
        Field1: ResizeArray<int option> }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = null } |]
        Assert.seqEqual expectedRecords outputRecords

    let NonNull = [|
        [| box<int option list> (* inputValue *) [] (**);
            box <| (* outputValue *) ResizeArray<int option>() (**) |]

        [| box<int option list> (* inputValue *) [ Option.None ] (**);
            box <| (* outputValue *) ResizeArray<int option>([ Option.None ]) (**) |]

        [| box<int option list> (* inputValue *) [ Option.Some 1 ] (**);
            box <| (* outputValue *) ResizeArray<int option>([ Option.Some 1 ]) (**) |]

        [| box<int option list> (* inputValue *) [ Option.None; Option.None; Option.None ] (**);
            box <| (* outputValue *) ResizeArray<int option>([ Option.None; Option.None; Option.None ]) (**) |]

        [| box<int option list> (* inputValue *) [ Option.Some 1; Option.None; Option.Some 3 ] (**);
            box <| (* outputValue *) ResizeArray<int option>([ Option.Some 1; Option.None; Option.Some 3 ]) (**) |]

        [| box<int option list> (* inputValue *) [ Option.Some 1; Option.Some 2 ] (**);
            box <| (* outputValue *) ResizeArray<int option>([ Option.Some 1; Option.Some 2 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = outputValue } |]
        Assert.seqEqual expectedRecords outputRecords
