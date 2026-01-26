namespace Parquet.FSharp.Tests.Array1d

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``serialize 1d array with atomic elements`` =
    type Input = { Field1: int array }
    type Output = { Field1: int array }

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
                    + $" '{typeof<int array>.FullName}' which is not treated as"
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

module ``serialize 1d array with list elements`` =
    type Input = { Field1: int list array }
    type Output = { Field1: int list array }

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
                    + $" '{typeof<int list array>.FullName}' which is not"
                    + " treated as nullable by default" @>)

    let NonNull = [|
        [| box<int list array> (**) [||] (**) |]
        [| box<int list array> (**) [| [] |] (**) |]
        [| box<int list array> (**) [| [ 1; 2; 3 ] |] (**) |]
        [| box<int list array> (**) [| []; []; [] |] (**) |]
        [| box<int list array> (**) [| [ 1 ]; []; [ 2; 3; 4 ] |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize 1d array with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record array }
    type Output = { Field1: Record array }

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
                    + $" '{typeof<Record array>.FullName}' which is not treated"
                    + " as nullable by default" @>)

    let NonNull = [|
        [| box<Record array> (**) [||] (**) |]
        [| box<Record array> (**) [| { Field2 = 1 } |] (**) |]
        [| box<Record array> (**) [| { Field2 = 1 }; { Field2 = 2 } |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize 1d array with optional elements`` =
    type Input = { Field1: int option array }
    type Output = { Field1: int option array }

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
                    + $" '{typeof<int option array>.FullName}' which is not"
                    + " treated as nullable by default" @>)

    let NonNull = [|
        [| box<int option array> (**) [||] (**) |]
        [| box<int option array> (**) [| Option.None |] (**) |]
        [| box<int option array> (**) [| Option.Some 1 |] (**) |]
        [| box<int option array> (**) [| Option.None; Option.None; Option.None |] (**) |]
        [| box<int option array> (**) [| Option.Some 1; Option.None; Option.Some 3 |] (**) |]
        [| box<int option array> (**) [| Option.Some 1; Option.Some 2 |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize 1d array with atomic elements from required list with atomic elements`` =
    type Input = { Field1: int list }
    type Output = { Field1: int array }

    let Value = [|
        [| box<int list> (* inputValue *) [] (**);
            box<int array> (* outputValue *) [||] (**) |]

        [| box<int list> (* inputValue *) [ 1 ] (**);
            box<int array> (* outputValue *) [| 1 |] (**) |]

        [| box<int list> (* inputValue *) [ 1; 2; 3 ] (**);
            box<int array> (* outputValue *) [| 1; 2; 3 |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
        
module ``deserialize 1d array with atomic elements from optional list with atomic elements`` =
    type Input = { Field1: int list option }
    type Output = { Field1: int array }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<int array>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNullValue = [|
        [| box<int list> (* inputValue *) [] (**);
            box<int array> (* outputValue *) [||] (**) |]

        [| box<int list> (* inputValue *) [ 1 ] (**);
            box<int array> (* outputValue *) [| 1 |] (**) |]

        [| box<int list> (* inputValue *) [ 1; 2; 3 ] (**);
            box<int array> (* outputValue *) [| 1; 2; 3 |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
        
module ``deserialize 1d array with list elements from required list with list elements`` =
    type Input = { Field1: int list list }
    type Output = { Field1: int list array }

    let Value = [|
        [| box<int list list> (* inputValue *) [] (**);
            box<int list array> (* outputValue*) [||] (**) |]

        [| box<int list list> (* inputValue *) [ [] ] (**);
            box<int list array> (* outputValue*) [| [] |] (**) |]

        [| box<int list list> (* inputValue *) [ [ 1; 2; 3 ] ] (**);
            box<int list array> (* outputValue*) [| [ 1; 2; 3 ] |] (**) |]

        [| box<int list list> (* inputValue *) [ []; []; [] ] (**);
            box<int list array> (* outputValue*) [| []; []; [] |] (**) |]

        [| box<int list list> (* inputValue *) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**);
            box<int list array> (* outputValue*) [| [ 1 ]; []; [ 2; 3; 4 ] |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
        
module ``deserialize 1d array with list elements from optional list with list elements`` =
    type Input = { Field1: int list list option }
    type Output = { Field1: int list array }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<int list array>.FullName}' which is not"
                    + " treated as nullable by default" @>)

    let NonNullValue = [|
        [| box<int list list> (* inputValue *) [] (**);
            box<int list array> (* outputValue*) [||] (**) |]

        [| box<int list list> (* inputValue *) [ [] ] (**);
            box<int list array> (* outputValue*) [| [] |] (**) |]

        [| box<int list list> (* inputValue *) [ [ 1; 2; 3 ] ] (**);
            box<int list array> (* outputValue*) [| [ 1; 2; 3 ] |] (**) |]

        [| box<int list list> (* inputValue *) [ []; []; [] ] (**);
            box<int list array> (* outputValue*) [| []; []; [] |] (**) |]

        [| box<int list list> (* inputValue *) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**);
            box<int list array> (* outputValue*) [| [ 1 ]; []; [ 2; 3; 4 ] |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
        
module ``deserialize 1d array with record elements from required list with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record list }
    type Output = { Field1: Record array }

    let Value = [|
        [| box<Record list> (* inputValue *) [] (**);
            box<Record array> (* outputValue *) [||] (**) |]

        [| box<Record list> (* inputValue *) [ { Field2 = 1 } ] (**);
            box<Record array> (* outputValue *) [| { Field2 = 1 } |] (**) |]

        [| box<Record list> (* inputValue *) [ { Field2 = 1 }; { Field2 = 2 } ] (**);
            box<Record array> (* outputValue *) [| { Field2 = 1 }; { Field2 = 2 } |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
        
module ``deserialize 1d array with record elements from optional list with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record list option }
    type Output = { Field1: Record array }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<Record array>.FullName}' which is not treated"
                    + " as nullable by default" @>)

    let NonNullValue = [|
        [| box<Record list> (* inputValue *) [] (**);
            box<Record array> (* outputValue *) [||] (**) |]

        [| box<Record list> (* inputValue *) [ { Field2 = 1 } ] (**);
            box<Record array> (* outputValue *) [| { Field2 = 1 } |] (**) |]

        [| box<Record list> (* inputValue *) [ { Field2 = 1 }; { Field2 = 2 } ] (**);
            box<Record array> (* outputValue *) [| { Field2 = 1 }; { Field2 = 2 } |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
        
module ``deserialize 1d array with optional elements from required list with optional elements`` =
    type Input = { Field1: int option list }
    type Output = { Field1: int option array }

    let Value = [|
        [| box<int option list> (* inputValue *) [] (**);
            box<int option array> (* outputValue *) [||] (**) |]

        [| box<int option list> (* inputValue *) [ Option.None ] (**);
            box<int option array> (* outputValue *) [| Option.None |] (**) |]

        [| box<int option list> (* inputValue *) [ Option.Some 1 ] (**);
            box<int option array> (* outputValue *) [| Option.Some 1 |] (**) |]

        [| box<int option list> (* inputValue *) [ Option.None; Option.None; Option.None ] (**);
            box<int option array> (* outputValue *) [| Option.None; Option.None; Option.None |] (**) |]

        [| box<int option list> (* inputValue *) [ Option.Some 1; Option.None; Option.Some 3 ] (**);
            box<int option array> (* outputValue *) [| Option.Some 1; Option.None; Option.Some 3 |] (**) |]

        [| box<int option list> (* inputValue *) [ Option.Some 1; Option.Some 2 ] (**);
            box<int option array> (* outputValue *) [| Option.Some 1; Option.Some 2 |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
        
module ``deserialize 1d array with optional elements from optional list with optional elements`` =
    type Input = { Field1: int option list option }
    type Output = { Field1: int option array }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<int option array>.FullName}' which is not"
                    + " treated as nullable by default" @>)

    let NonNullValue = [|
        [| box<int option list> (* inputValue *) [] (**);
            box<int option array> (* outputValue *) [||] (**) |]

        [| box<int option list> (* inputValue *) [ Option.None ] (**);
            box<int option array> (* outputValue *) [| Option.None |] (**) |]

        [| box<int option list> (* inputValue *) [ Option.Some 1 ] (**);
            box<int option array> (* outputValue *) [| Option.Some 1 |] (**) |]

        [| box<int option list> (* inputValue *) [ Option.None; Option.None; Option.None ] (**);
            box<int option array> (* outputValue *) [| Option.None; Option.None; Option.None |] (**) |]

        [| box<int option list> (* inputValue *) [ Option.Some 1; Option.None; Option.Some 3 ] (**);
            box<int option array> (* outputValue *) [| Option.Some 1; Option.None; Option.Some 3 |] (**) |]

        [| box<int option list> (* inputValue *) [ Option.Some 1; Option.Some 2 ] (**);
            box<int option array> (* outputValue *) [| Option.Some 1; Option.Some 2 |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
