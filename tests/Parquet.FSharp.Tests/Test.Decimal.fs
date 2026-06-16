namespace Parquet.FSharp.Tests.Decimal

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``{ default } serialize`` =
    type Input = { Field1: decimal }
    type Output = { Field1: decimal }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isFixedLengthByteArray 16
                Assert.Field.LogicalType.isDecimal 38 18
                Assert.Field.ConvertedType.isDecimal 38 18
                Assert.Field.hasNoChildren ] ]

    let Value = [|
        [| box -99999999999999999999.999999990000000000M |]
        [| box                    -1.000000000000000000M |]
        [| box                    -0.000000000000000001M |]
        [| box                     0.000000000000000000M |]
        [| box                     0.000000000000000001M |]
        [| box                     1.000000000000000000M |]
        [| box  99999999999999999999.999999990000000000M |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ precision=38; scale=18 } serialize`` =
    type Input = { [<ParquetDecimal(Precision = 38, Scale = 18)>] Field1: decimal }
    type Output = { Field1: decimal }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isFixedLengthByteArray 16
                Assert.Field.LogicalType.isDecimal 38 18
                Assert.Field.ConvertedType.isDecimal 38 18
                Assert.Field.hasNoChildren ] ]

    let Value = [|
        [| box -99999999999999999999.999999990000000000M |]
        [| box                    -1.000000000000000000M |]
        [| box                    -0.000000000000000001M |]
        [| box                     0.000000000000000000M |]
        [| box                     0.000000000000000001M |]
        [| box                     1.000000000000000000M |]
        [| box  99999999999999999999.999999990000000000M |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ precision=29; scale=0 } serialize`` =
    type Input = { [<ParquetDecimal(Precision = 29, Scale = 0)>] Field1: decimal }
    type Output = { [<ParquetDecimal(Precision = 29, Scale = 0)>] Field1: decimal }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isFixedLengthByteArray 13
                Assert.Field.LogicalType.isDecimal 29 0
                Assert.Field.ConvertedType.isDecimal 29 0
                Assert.Field.hasNoChildren ] ]

    let Value = [|
        [| box -79228162514264337593543950335M |]
        [| box                             -1M |]
        [| box                             -0M |]
        [| box                              0M |]
        [| box                              1M |]
        [| box  79228162514264337593543950335M |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ precision=28; scale=28 } serialize`` =
    type Input = { [<ParquetDecimal(Precision = 28, Scale = 28)>] Field1: decimal }
    type Output = { [<ParquetDecimal(Precision = 28, Scale = 28)>] Field1: decimal }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isFixedLengthByteArray 12
                Assert.Field.LogicalType.isDecimal 28 28
                Assert.Field.ConvertedType.isDecimal 28 28
                Assert.Field.hasNoChildren ] ]

    let Value = [|
        [| box -0.9999999999999999999999999999M |]
        [| box -0.0000000000000000000000000001M |]
        [| box -0.0000000000000000000000000000M |]
        [| box  0.0000000000000000000000000000M |]
        [| box  0.0000000000000000000000000001M |]
        [| box  0.9999999999999999999999999999M |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=false } serialize`` =
    type Input = { [<ParquetDecimal(Optional = false)>] Field1: decimal }
    type Output = { Field1: decimal }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isFixedLengthByteArray 16
                Assert.Field.LogicalType.isDecimal 38 18
                Assert.Field.ConvertedType.isDecimal 38 18
                Assert.Field.hasNoChildren ] ]

    let Value = [|
        [| box -99999999999999999999.999999990000000000M |]
        [| box                    -1.000000000000000000M |]
        [| box                    -0.000000000000000001M |]
        [| box                     0.000000000000000000M |]
        [| box                     0.000000000000000001M |]
        [| box                     1.000000000000000000M |]
        [| box  99999999999999999999.999999990000000000M |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } serialize`` =
    type Input = { [<ParquetDecimal(Optional = true)>] Field1: decimal }
    type Output = { Field1: decimal option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isFixedLengthByteArray 16
                Assert.Field.LogicalType.isDecimal 38 18
                Assert.Field.ConvertedType.isDecimal 38 18
                Assert.Field.hasNoChildren ] ]

    let Value = [|
        [| box -99999999999999999999.999999990000000000M |]
        [| box                    -1.000000000000000000M |]
        [| box                    -0.000000000000000001M |]
        [| box                     0.000000000000000000M |]
        [| box                     0.000000000000000001M |]
        [| box                     1.000000000000000000M |]
        [| box  99999999999999999999.999999990000000000M |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``{ default } deserialize`` =
    type Input = { Field1: decimal }
    type Output = { Field1: decimal }

    let Value = [|
        [| box -99999999999999999999.999999990000000000M |]
        [| box                    -1.000000000000000000M |]
        [| box                    -0.000000000000000001M |]
        [| box                     0.000000000000000000M |]
        [| box                     0.000000000000000001M |]
        [| box                     1.000000000000000000M |]
        [| box  99999999999999999999.999999990000000000M |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ precision=38; scale=18 } deserialize`` =
    type Input = { Field1: decimal }
    type Output = { [<ParquetDecimal(Precision = 38, Scale = 18)>] Field1: decimal }

    let Value = [|
        [| box -99999999999999999999.999999990000000000M |]
        [| box                    -1.000000000000000000M |]
        [| box                    -0.000000000000000001M |]
        [| box                     0.000000000000000000M |]
        [| box                     0.000000000000000001M |]
        [| box                     1.000000000000000000M |]
        [| box  99999999999999999999.999999990000000000M |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ precision=29; scale=0 } deserialize`` =
    type Input = { [<ParquetDecimal(Precision = 29, Scale = 0)>] Field1: decimal }
    type Output = { [<ParquetDecimal(Precision = 29, Scale = 0)>] Field1: decimal }

    let Value = [|
        [| box -79228162514264337593543950335M |]
        [| box                             -1M |]
        [| box                             -0M |]
        [| box                              0M |]
        [| box                              1M |]
        [| box  79228162514264337593543950335M |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ precision=28; scale=28 } deserialize`` =
    type Input = { [<ParquetDecimal(Precision = 28, Scale = 28)>] Field1: decimal }
    type Output = { [<ParquetDecimal(Precision = 28, Scale = 28)>] Field1: decimal }

    let Value = [|
        [| box -0.9999999999999999999999999999M |]
        [| box -0.0000000000000000000000000001M |]
        [| box -0.0000000000000000000000000000M |]
        [| box  0.0000000000000000000000000000M |]
        [| box  0.0000000000000000000000000001M |]
        [| box  0.9999999999999999999999999999M |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=false } deserialize`` =
    type Input = { Field1: decimal }
    type Output = { [<ParquetDecimal(Optional = false)>] Field1: decimal }

    let Value = [|
        [| box -99999999999999999999.999999990000000000M |]
        [| box                    -1.000000000000000000M |]
        [| box                    -0.000000000000000001M |]
        [| box                     0.000000000000000000M |]
        [| box                     0.000000000000000001M |]
        [| box                     1.000000000000000000M |]
        [| box  99999999999999999999.999999990000000000M |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``{ optional=true } deserialize`` =
    type Input = { Field1: decimal option }
    type Output = { [<ParquetDecimal(Optional = true)>] Field1: decimal }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<decimal>}'" @>)

    let NonNull = [|
        [| box -99999999999999999999.999999990000000000M |]
        [| box                    -1.000000000000000000M |]
        [| box                    -0.000000000000000001M |]
        [| box                     0.000000000000000000M |]
        [| box                     0.000000000000000001M |]
        [| box                     1.000000000000000000M |]
        [| box  99999999999999999999.999999990000000000M |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
