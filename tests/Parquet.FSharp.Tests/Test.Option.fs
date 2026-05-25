namespace Parquet.FSharp.Tests.Option

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``{ default } serialize with atomic value`` =
    type Input = { Field1: int option }
    type Output = { Field1: int option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 32 true
                Assert.Field.ConvertedType.isInt32
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``none`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``some`` () =
        let inputRecords = [| { Input.Field1 = Option.Some 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some 1 } |] @>

module ``{ default } serialize with list value`` =
    type Input = { Field1: int list option }
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
    let ``none`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    let Some = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Some)>]
    let ``some`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ default } serialize with record value`` =
    type Record = { Field2: int }
    type Input = { Field1: Record option }
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
    let ``none`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``some`` () =
        let value = { Record.Field2 = 1 }
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ required=true } serialize with atomic value`` =
    type Input = { [<ParquetOptionField(Required = true)>] Field1: int option }
    type Output = { Field1: int }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 32 true
                Assert.Field.ConvertedType.isInt32
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``none`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<int option>.FullName}' which has been"
                    + " configured as required" @>)

    [<Fact>]
    let ``some`` () =
        let inputRecords = [| { Input.Field1 = Option.Some 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = 1 } |] @>

module ``{ required=true } serialize with list value`` =
    type Input = { [<ParquetOptionField(Required = true)>] Field1: int list option }
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
    let ``none`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<int list option>.FullName}' which has been"
                    + " configured as required" @>)

    let Some = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Some)>]
    let ``some`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ required=true } serialize with record value`` =
    type Record = { Field2: int }
    type Input = { [<ParquetOptionField(Required = true)>] Field1: Record option }
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
    let ``none`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Record option>.FullName}' which has been"
                    + " configured as required" @>)

    [<Fact>]
    let ``some`` () =
        let value = { Record.Field2 = 1 }
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ required=true } serialize with optional value`` =
    type Input = { [<ParquetOptionField(Required = true)>] Field1: int option option }
    type Output = { Field1: int option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 32 true
                Assert.Field.ConvertedType.isInt32
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``none`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<int option option>.FullName}' which has been"
                    + " configured as required" @>)

    let Some = [|
        [| box<int option> <| (**) Option.None (**) |]
        [| box<int option> <| (**) Option.Some 1 (**) |] |]

    [<Theory>]
    [<MemberData(nameof Some)>]
    let ``some`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ default } deserialize with atomic value`` =
    type Input = { Field1: int option }
    type Output = { Field1: int option }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``non-null`` () =
        let inputRecords = [| { Input.Field1 = Option.Some 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some 1 } |] @>

module ``{ default } deserialize with list value`` =
    type Input = { Field1: int list option }
    type Output = { Field1: int list option }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

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
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ default } deserialize with record value`` =
    type Record = { Field2: int }
    type Input = { Field1: Record option }
    type Output = { Field1: Record option }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``non-null`` () =
        let value = { Record.Field2 = 1 }
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ required=true } deserialize with atomic value`` =
    type Input = { Field1: int }
    type Output = { [<ParquetOptionField(Required = true)>] Field1: int option }

    [<Fact>]
    let ``value`` () =
        let inputRecords = [| { Input.Field1 = 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some 1 } |] @>

module ``{ required=true } deserialize with list value`` =
    type Input = { Field1: int list }
    type Output = { [<ParquetOptionField(Required = true)>] Field1: int list option }

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
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ required=true } deserialize with record value`` =
    type Record = { Field2: int }
    type Input = { Field1: Record }
    type Output = { [<ParquetOptionField(Required = true)>] Field1: Record option }

    [<Fact>]
    let ``value`` () =
        let value = { Record.Field2 = 1 }
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ required=true } deserialize with optional value`` =
    type Input = { Field1: int option }
    type Output = { [<ParquetOptionField(Required = true)>] Field1: int option option }

    let Value = [|
        [| box<int option> <| (**) Option.None (**) |]
        [| box<int option> <| (**) Option.Some 1 (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>
