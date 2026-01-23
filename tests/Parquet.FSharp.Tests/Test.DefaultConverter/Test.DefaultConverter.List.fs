namespace Parquet.FSharp.Tests.DefaultConverter.List

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``serialize list with atomic elements`` =
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

    let Value = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize list with list elements`` =
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
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize list with record elements`` =
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

    let Value = [|
        [| box<Record list> (**) [] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 } ] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 }; { Field2 = 2 } ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize list with optional elements`` =
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
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize list with atomic elements from required list with atomic elements`` =
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
        
module ``deserialize list with atomic elements from optional list with atomic elements`` =
    type Input = { Field1: int list option }
    type Output = { Field1: int list }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<int list>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNullValue = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize list with list elements from required list with list elements`` =
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
        
module ``deserialize list with list elements from optional list with list elements`` =
    type Input = { Field1: int list list option }
    type Output = { Field1: int list list }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<int list list>.FullName}' which is not"
                    + " treated as nullable by default" @>)

    let NonNullValue = [|
        [| box<int list list> (**) [] (**) |]
        [| box<int list list> (**) [ [] ] (**) |]
        [| box<int list list> (**) [ [ 1; 2; 3 ] ] (**) |]
        [| box<int list list> (**) [ []; []; [] ] (**) |]
        [| box<int list list> (**) [ [ 1 ]; []; [ 2; 3; 4 ] ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize list with record elements from required list with record elements`` =
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
        
module ``deserialize list with record elements from optional list with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record list option }
    type Output = { Field1: Record list }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<Record list>.FullName}' which is not treated"
                    + " as nullable by default" @>)

    let NonNullValue = [|
        [| box<Record list> (**) [] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 } ] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 }; { Field2 = 2 } ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize list with optional elements from required list with optional elements`` =
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
        
module ``deserialize list with optional elements from optional list with optional elements`` =
    type Input = { Field1: int option list option }
    type Output = { Field1: int option list }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<int option list>.FullName}' which is not"
                    + " treated as nullable by default" @>)

    let NonNullValue = [|
        [| box<int option list> (**) [] (**) |]
        [| box<int option list> (**) [ Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1 ] (**) |]
        [| box<int option list> (**) [ Option.None; Option.None; Option.None ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.None; Option.Some 3 ] (**) |]
        [| box<int option list> (**) [ Option.Some 1; Option.Some 2 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
