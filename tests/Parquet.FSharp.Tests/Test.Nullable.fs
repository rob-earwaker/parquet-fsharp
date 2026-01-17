namespace Parquet.FSharp.Tests.Nullable

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``serialize nullable atomic`` =
    type Input = { Field1: Nullable<int> }
    type Output = { Field1: int option }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Nullable() } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 32 true
                Assert.Field.ConvertedType.isInt32
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``non-null value`` () =
        let inputRecords = [| { Input.Field1 = Nullable(1) } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt32
                Assert.Field.LogicalType.isInteger 32 true
                Assert.Field.ConvertedType.isInt32
                Assert.Field.hasNoChildren ] ]
        test <@ outputRecords = [| { Output.Field1 = Option.Some 1 } |] @>

module ``deserialize nullable atomic from required atomic`` =
    type Input = { Field1: int }
    type Output = { Field1: Nullable<int> }

    [<Fact>]
    let ``non-null value`` () =
        let inputRecords = [| { Input.Field1 = 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Nullable(1) } |] @>

module ``deserialize nullable atomic from optional atomic`` =
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
