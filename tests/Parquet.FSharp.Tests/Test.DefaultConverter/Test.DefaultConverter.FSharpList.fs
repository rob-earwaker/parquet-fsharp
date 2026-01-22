namespace Parquet.FSharp.Tests.DefaultConverter.FSharpList

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``serialize fsharp list with atomic elements`` =
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

module ``serialize fsharp list with list elements`` =
    type Input = { Field1: int[] list }
    type Output = { Field1: int[] list }

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
        [| box<int[] list> (**) [] (**) |]
        [| box<int[] list> (**) [ [||] ] (**) |]
        [| box<int[] list> (**) [ [| 1; 2; 3 |] ] (**) |]
        [| box<int[] list> (**) [ [||]; [||]; [||] ] (**) |]
        [| box<int[] list> (**) [ [| 1 |]; [||]; [| 2; 3; 4 |] ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize fsharp list with record elements`` =
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
        [| box<Record list> (**) [ { Field2 = 1 }; { Field2 = 2 }; { Field2 = 3 } ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize fsharp list with optional elements`` =
    type Input = { Field1: option<int> list }
    type Output = { Field1: option<int> list }

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
        [| box<option<int> list> (**) [] (**) |]
        [| box<option<int> list> (**) [ Option.None ] (**) |]
        [| box<option<int> list> (**) [ Option.Some 1; ] (**) |]
        [| box<option<int> list> (**) [ Option.None; Option.None; Option.None ] (**) |]
        [| box<option<int> list> (**) [ Option.Some 1; Option.None; Option.Some 3 ] (**) |]
        [| box<option<int> list> (**) [ Option.Some 1; Option.Some 2; Option.Some 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize fsharp list with atomic elements from required list with atomic elements`` =
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
        
module ``deserialize fsharp list with atomic elements from optional list with atomic elements`` =
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
        
module ``deserialize fsharp list with list elements from required list with list elements`` =
    type Input = { Field1: int[] list }
    type Output = { Field1: int[] list }

    let Value = [|
        [| box<int[] list> (**) [] (**) |]
        [| box<int[] list> (**) [ [||] ] (**) |]
        [| box<int[] list> (**) [ [| 1; 2; 3 |] ] (**) |]
        [| box<int[] list> (**) [ [||]; [||]; [||] ] (**) |]
        [| box<int[] list> (**) [ [| 1 |]; [||]; [| 2; 3; 4 |] ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize fsharp list with list elements from optional list with list elements`` =
    type Input = { Field1: int[] list option }
    type Output = { Field1: int[] list }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<int[] list>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNullValue = [|
        [| box<int[] list> (**) [] (**) |]
        [| box<int[] list> (**) [ [||] ] (**) |]
        [| box<int[] list> (**) [ [| 1; 2; 3 |] ] (**) |]
        [| box<int[] list> (**) [ [||]; [||]; [||] ] (**) |]
        [| box<int[] list> (**) [ [| 1 |]; [||]; [| 2; 3; 4 |] ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize fsharp list with record elements from required list with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record list }
    type Output = { Field1: Record list }

    let Value = [|
        [| box<Record list> (**) [] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 } ] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 }; { Field2 = 2 }; { Field2 = 3 } ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize fsharp list with record elements from optional list with record elements`` =
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
                    + $" '{typeof<Record list>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNullValue = [|
        [| box<Record list> (**) [] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 } ] (**) |]
        [| box<Record list> (**) [ { Field2 = 1 }; { Field2 = 2 }; { Field2 = 3 } ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize fsharp list with optional elements from required list with optional elements`` =
    type Input = { Field1: option<int> list }
    type Output = { Field1: option<int> list }

    let Value = [|
        [| box<option<int> list> (**) [] (**) |]
        [| box<option<int> list> (**) [ Option.None ] (**) |]
        [| box<option<int> list> (**) [ Option.Some 1; ] (**) |]
        [| box<option<int> list> (**) [ Option.None; Option.None; Option.None ] (**) |]
        [| box<option<int> list> (**) [ Option.Some 1; Option.None; Option.Some 3 ] (**) |]
        [| box<option<int> list> (**) [ Option.Some 1; Option.Some 2; Option.Some 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize fsharp list with optional elements from optional list with optional elements`` =
    type Input = { Field1: option<int> list option }
    type Output = { Field1: option<int> list }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<option<int> list>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNullValue = [|
        [| box<option<int> list> (**) [] (**) |]
        [| box<option<int> list> (**) [ Option.None ] (**) |]
        [| box<option<int> list> (**) [ Option.Some 1; ] (**) |]
        [| box<option<int> list> (**) [ Option.None; Option.None; Option.None ] (**) |]
        [| box<option<int> list> (**) [ Option.Some 1; Option.None; Option.Some 3 ] (**) |]
        [| box<option<int> list> (**) [ Option.Some 1; Option.Some 2; Option.Some 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
