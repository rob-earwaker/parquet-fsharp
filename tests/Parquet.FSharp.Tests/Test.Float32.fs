namespace Parquet.FSharp.Tests.Float32

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``{ default } serialize`` =
    type Input = { Field1: float32 }
    type Output = { Field1: float32 }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isFloat32
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``nan`` () =
        let inputRecords = [| { Input.Field1 = Single.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Single.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Single.NegativeInfinity)>]
    [<InlineData(        Single.MinValue)>]
    [<InlineData(                  -1.0f)>]
    [<InlineData(        -Single.Epsilon)>]
    [<InlineData(    Single.NegativeZero)>]
    [<InlineData(                   0.0f)>]
    [<InlineData(         Single.Epsilon)>]
    [<InlineData(                   1.0f)>]
    [<InlineData(               Single.E)>]
    [<InlineData(              Single.Pi)>]
    [<InlineData(             Single.Tau)>]
    [<InlineData(        Single.MaxValue)>]
    [<InlineData(Single.PositiveInfinity)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=false } serialize`` =
    type Input = { [<ParquetFloat32(Optional = false)>] Field1: float32 }
    type Output = { Field1: float32 }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isFloat32
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``nan`` () =
        let inputRecords = [| { Input.Field1 = Single.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Single.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Single.NegativeInfinity)>]
    [<InlineData(        Single.MinValue)>]
    [<InlineData(                  -1.0f)>]
    [<InlineData(        -Single.Epsilon)>]
    [<InlineData(    Single.NegativeZero)>]
    [<InlineData(                   0.0f)>]
    [<InlineData(         Single.Epsilon)>]
    [<InlineData(                   1.0f)>]
    [<InlineData(               Single.E)>]
    [<InlineData(              Single.Pi)>]
    [<InlineData(             Single.Tau)>]
    [<InlineData(        Single.MaxValue)>]
    [<InlineData(Single.PositiveInfinity)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } serialize`` =
    type Input = { [<ParquetFloat32(Optional = true)>] Field1: float32 }
    type Output = { Field1: float32 option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isFloat32
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``nan`` () =
        let inputRecords = [| { Input.Field1 = Single.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ outputRecord.Field1.IsSome @>
        test <@ Single.IsNaN(outputRecord.Field1.Value) @>

    [<Theory>]
    [<InlineData(Single.NegativeInfinity)>]
    [<InlineData(        Single.MinValue)>]
    [<InlineData(                  -1.0f)>]
    [<InlineData(        -Single.Epsilon)>]
    [<InlineData(    Single.NegativeZero)>]
    [<InlineData(                   0.0f)>]
    [<InlineData(         Single.Epsilon)>]
    [<InlineData(                   1.0f)>]
    [<InlineData(               Single.E)>]
    [<InlineData(              Single.Pi)>]
    [<InlineData(             Single.Tau)>]
    [<InlineData(        Single.MaxValue)>]
    [<InlineData(Single.PositiveInfinity)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ default } deserialize`` =
    type Input = { Field1: float32 }
    type Output = { Field1: float32 }

    [<Fact>]
    let ``nan`` () =
        let inputRecords = [| { Input.Field1 = Single.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Single.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Single.NegativeInfinity)>]
    [<InlineData(        Single.MinValue)>]
    [<InlineData(                  -1.0f)>]
    [<InlineData(        -Single.Epsilon)>]
    [<InlineData(    Single.NegativeZero)>]
    [<InlineData(                   0.0f)>]
    [<InlineData(         Single.Epsilon)>]
    [<InlineData(                   1.0f)>]
    [<InlineData(               Single.E)>]
    [<InlineData(              Single.Pi)>]
    [<InlineData(             Single.Tau)>]
    [<InlineData(        Single.MaxValue)>]
    [<InlineData(Single.PositiveInfinity)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=false } deserialize`` =
    type Input = { Field1: float32 }
    type Output = { [<ParquetFloat32(Optional = false)>] Field1: float32 }

    [<Fact>]
    let ``nan`` () =
        let inputRecords = [| { Input.Field1 = Single.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Single.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Single.NegativeInfinity)>]
    [<InlineData(        Single.MinValue)>]
    [<InlineData(                  -1.0f)>]
    [<InlineData(        -Single.Epsilon)>]
    [<InlineData(    Single.NegativeZero)>]
    [<InlineData(                   0.0f)>]
    [<InlineData(         Single.Epsilon)>]
    [<InlineData(                   1.0f)>]
    [<InlineData(               Single.E)>]
    [<InlineData(              Single.Pi)>]
    [<InlineData(             Single.Tau)>]
    [<InlineData(        Single.MaxValue)>]
    [<InlineData(Single.PositiveInfinity)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } deserialize`` =
    type Input = { Field1: float32 option }
    type Output = { [<ParquetFloat32(Optional = true)>] Field1: float32 }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<float32>}'" @>)

    [<Fact>]
    let ``nan`` () =
        let inputRecords = [| { Input.Field1 = Option.Some Single.NaN } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords.Length = 1 @>
        let outputRecord = outputRecords[0]
        test <@ Single.IsNaN(outputRecord.Field1) @>

    [<Theory>]
    [<InlineData(Single.NegativeInfinity)>]
    [<InlineData(        Single.MinValue)>]
    [<InlineData(                  -1.0f)>]
    [<InlineData(        -Single.Epsilon)>]
    [<InlineData(    Single.NegativeZero)>]
    [<InlineData(                   0.0f)>]
    [<InlineData(         Single.Epsilon)>]
    [<InlineData(                   1.0f)>]
    [<InlineData(               Single.E)>]
    [<InlineData(              Single.Pi)>]
    [<InlineData(             Single.Tau)>]
    [<InlineData(        Single.MaxValue)>]
    [<InlineData(Single.PositiveInfinity)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
