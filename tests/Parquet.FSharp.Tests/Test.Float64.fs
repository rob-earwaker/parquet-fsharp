namespace Parquet.FSharp.Tests.Float64

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``{ default } serialize`` =
    type Input = { Field1: float }
    type Output = { Field1: float }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isFloat64
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``nan`` () =
        let inputRecords = [| { Input.Field1 = Double.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Double.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Double.NegativeInfinity)>]
    [<InlineData(        Double.MinValue)>]
    [<InlineData(                   -1.0)>]
    [<InlineData(        -Double.Epsilon)>]
    [<InlineData(    Double.NegativeZero)>]
    [<InlineData(                    0.0)>]
    [<InlineData(         Double.Epsilon)>]
    [<InlineData(                    1.0)>]
    [<InlineData(               Double.E)>]
    [<InlineData(              Double.Pi)>]
    [<InlineData(             Double.Tau)>]
    [<InlineData(        Double.MaxValue)>]
    [<InlineData(Double.PositiveInfinity)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=false } serialize`` =
    type Input = { [<ParquetFloat64(Optional = false)>] Field1: float }
    type Output = { Field1: float }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isFloat64
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``nan`` () =
        let inputRecords = [| { Input.Field1 = Double.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Double.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Double.NegativeInfinity)>]
    [<InlineData(        Double.MinValue)>]
    [<InlineData(                   -1.0)>]
    [<InlineData(        -Double.Epsilon)>]
    [<InlineData(    Double.NegativeZero)>]
    [<InlineData(                    0.0)>]
    [<InlineData(         Double.Epsilon)>]
    [<InlineData(                    1.0)>]
    [<InlineData(               Double.E)>]
    [<InlineData(              Double.Pi)>]
    [<InlineData(             Double.Tau)>]
    [<InlineData(        Double.MaxValue)>]
    [<InlineData(Double.PositiveInfinity)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } serialize`` =
    type Input = { [<ParquetFloat64(Optional = true)>] Field1: float }
    type Output = { Field1: float option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isFloat64
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``nan`` () =
        let inputRecords = [| { Input.Field1 = Double.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1.IsSome @>
        test <@ Double.IsNaN(outputRecord.Field1.Value) @>

    [<Theory>]
    [<InlineData(Double.NegativeInfinity)>]
    [<InlineData(        Double.MinValue)>]
    [<InlineData(                   -1.0)>]
    [<InlineData(        -Double.Epsilon)>]
    [<InlineData(    Double.NegativeZero)>]
    [<InlineData(                    0.0)>]
    [<InlineData(         Double.Epsilon)>]
    [<InlineData(                    1.0)>]
    [<InlineData(               Double.E)>]
    [<InlineData(              Double.Pi)>]
    [<InlineData(             Double.Tau)>]
    [<InlineData(        Double.MaxValue)>]
    [<InlineData(Double.PositiveInfinity)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ default } deserialize`` =
    type Input = { Field1: float }
    type Output = { Field1: float }

    [<Fact>]
    let ``nan`` () =
        let inputRecords = [| { Input.Field1 = Double.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Double.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Double.NegativeInfinity)>]
    [<InlineData(        Double.MinValue)>]
    [<InlineData(                   -1.0)>]
    [<InlineData(        -Double.Epsilon)>]
    [<InlineData(    Double.NegativeZero)>]
    [<InlineData(                    0.0)>]
    [<InlineData(         Double.Epsilon)>]
    [<InlineData(                    1.0)>]
    [<InlineData(               Double.E)>]
    [<InlineData(              Double.Pi)>]
    [<InlineData(             Double.Tau)>]
    [<InlineData(        Double.MaxValue)>]
    [<InlineData(Double.PositiveInfinity)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=false } deserialize`` =
    type Input = { Field1: float }
    type Output = { [<ParquetFloat64(Optional = false)>] Field1: float }

    [<Fact>]
    let ``nan`` () =
        let inputRecords = [| { Input.Field1 = Double.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Double.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Double.NegativeInfinity)>]
    [<InlineData(        Double.MinValue)>]
    [<InlineData(                   -1.0)>]
    [<InlineData(        -Double.Epsilon)>]
    [<InlineData(    Double.NegativeZero)>]
    [<InlineData(                    0.0)>]
    [<InlineData(         Double.Epsilon)>]
    [<InlineData(                    1.0)>]
    [<InlineData(               Double.E)>]
    [<InlineData(              Double.Pi)>]
    [<InlineData(             Double.Tau)>]
    [<InlineData(        Double.MaxValue)>]
    [<InlineData(Double.PositiveInfinity)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } deserialize`` =
    type Input = { Field1: float option }
    type Output = { [<ParquetFloat64(Optional = true)>] Field1: float }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<float>}'" @>)

    [<Fact>]
    let ``nan`` () =
        let inputRecords = [| { Input.Field1 = Option.Some Double.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Double.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Double.NegativeInfinity)>]
    [<InlineData(        Double.MinValue)>]
    [<InlineData(                   -1.0)>]
    [<InlineData(        -Double.Epsilon)>]
    [<InlineData(    Double.NegativeZero)>]
    [<InlineData(                    0.0)>]
    [<InlineData(         Double.Epsilon)>]
    [<InlineData(                    1.0)>]
    [<InlineData(               Double.E)>]
    [<InlineData(              Double.Pi)>]
    [<InlineData(             Double.Tau)>]
    [<InlineData(        Double.MaxValue)>]
    [<InlineData(Double.PositiveInfinity)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
