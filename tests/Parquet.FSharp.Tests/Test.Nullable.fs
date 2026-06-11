namespace Parquet.FSharp.Tests.Nullable

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``{ default } serialize with atomic value`` =
    type Input = { Field1: Nullable<int> }
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
        let inputRecords = [| { Input.Field1 = Nullable() } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``some`` () =
        let inputRecords = [| { Input.Field1 = Nullable(1) } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some 1 } |] @>

module ``{ default } serialize with record value`` =
    type [<Struct>] Record = { Field2: int }
    type Input = { Field1: Nullable<Record> }
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
        let inputRecords = [| { Input.Field1 = Nullable() } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``some`` () =
        let value = { Record.Field2 = 1 }
        let inputRecords = [| { Input.Field1 = Nullable(value) } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ required=false } serialize`` =
    type Input = { [<ParquetNullable(Required = false)>] Field1: Nullable<int> }
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
        let inputRecords = [| { Input.Field1 = Nullable() } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``some`` () =
        let inputRecords = [| { Input.Field1 = Nullable(1) } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some 1 } |] @>

module ``{ required=true } serialize with required value`` =
    type Input = { [<ParquetNullable(Required = true)>] Field1: Nullable<int> }
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
        let inputRecords = [| { Input.Field1 = Nullable() } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Nullable<int>>}' which has been"
                    + " configured as required" @>)

    [<Fact>]
    let ``some`` () =
        let inputRecords = [| { Input.Field1 = Nullable(1) } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = 1 } |] @>

module ``{ required=true } serialize with optional value`` =
    type Input = { [<ParquetNullable(Required = true)>] Field1: Nullable<int voption> }
    type Output = { Field1: int voption }

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
        let inputRecords = [| { Input.Field1 = Nullable() } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<Nullable<int voption>>}' which has been"
                    + " configured as required" @>)

    let Some = [|
        [| box<int voption> <| (**) ValueOption.None (**) |]
        [| box<int voption> <| (**) ValueOption.Some 1 (**) |] |]

    [<Theory>]
    [<MemberData(nameof Some)>]
    let ``some`` value =
        let inputRecords = [| { Input.Field1 = Nullable(value) } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ default } deserialize with atomic value`` =
    type Input = { Field1: int option }
    type Output = { Field1: Nullable<int> }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Nullable() } |] @>

    [<Fact>]
    let ``non-null`` () =
        let inputRecords = [| { Input.Field1 = Option.Some 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Nullable(1) } |] @>

module ``{ default } deserialize with record value`` =
    type [<Struct>] Record = { Field2: int }
    type Input = { Field1: Record option }
    type Output = { Field1: Nullable<Record> }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Nullable() } |] @>

    [<Fact>]
    let ``non-null`` () =
        let value = { Record.Field2 = 1 }
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Nullable(value) } |] @>

module ``{ required=false } deserialize`` =
    type Input = { Field1: int option }
    type Output = { [<ParquetNullable(Required = false)>] Field1: Nullable<int> }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Nullable() } |] @>

    [<Fact>]
    let ``non-null`` () =
        let inputRecords = [| { Input.Field1 = Option.Some 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Nullable(1) } |] @>

module ``{ required=true } deserialize with required value`` =
    type Input = { Field1: int }
    type Output = { [<ParquetNullable(Required = true)>] Field1: Nullable<int> }

    [<Fact>]
    let ``value`` () =
        let inputRecords = [| { Input.Field1 = 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Nullable(1) } |] @>

module ``{ required=true } deserialize with optional value`` =
    type Input = { Field1: int voption }
    type Output = { [<ParquetNullable(Required = true)>] Field1: Nullable<int voption> }

    let Value = [|
        [| box<int voption> <| (**) ValueOption.None (**) |]
        [| box<int voption> <| (**) ValueOption.Some 1 (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Nullable(value) } |] @>
