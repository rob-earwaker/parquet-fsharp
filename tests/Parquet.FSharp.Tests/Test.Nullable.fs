namespace Parquet.FSharp.Tests.Nullable

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize nullable with atomic value`` =
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

module ``serialize nullable with record value`` =
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

module ``deserialize nullable with atomic value from required atomic`` =
    type Input = { Field1: int }
    type Output = { Field1: Nullable<int> }

    [<Fact>]
    let ``value`` () =
        let inputRecords = [| { Input.Field1 = 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Nullable(1) } |] @>

module ``deserialize nullable with atomic value from optional atomic`` =
    type Input = { Field1: int option }
    type Output = { Field1: Nullable<int> }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Nullable() } |] @>

    [<Fact>]
    let ``non-null value`` () =
        let inputRecords = [| { Input.Field1 = Option.Some 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Nullable(1) } |] @>

module ``deserialize nullable with record value from required record`` =
    type [<Struct>] Record = { Field2: int }
    type Input = { Field1: Record }
    type Output = { Field1: Nullable<Record> }

    [<Fact>]
    let ``value`` () =
        let value = { Record.Field2 = 1 }
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Nullable(value) } |] @>

module ``deserialize nullable with record value from optional record`` =
    type [<Struct>] Record = { Field2: int }
    type Input = { Field1: Record option }
    type Output = { Field1: Nullable<Record> }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Nullable() } |] @>

    [<Fact>]
    let ``non-null value`` () =
        let value = { Record.Field2 = 1 }
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Nullable(value) } |] @>
