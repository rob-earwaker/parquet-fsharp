namespace Parquet.FSharp.Tests.DefaultConverter.Union

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``serialize enum union with single case`` =
    type Union = Case1
    type Input = { Field1: Union }
    type Output = { Field1: string }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isByteArray
                Assert.Field.LogicalType.isString
                Assert.Field.ConvertedType.isUtf8
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``value`` () =
        let inputRecords = [| { Input.Field1 = Union.Case1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = "Case1" } |] @>

module ``serialize enum union with multiple cases`` =
    type Union = Case1 | Case2 | Case3
    type Input = { Field1: Union }
    type Output = { Field1: string }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isByteArray
                Assert.Field.LogicalType.isString
                Assert.Field.ConvertedType.isUtf8
                Assert.Field.hasNoChildren ] ]

    let Value = [|
        [| (* inputValue *) box Union.Case1; (* outputValue *) box "Case1" |]
        [| (* inputValue *) box Union.Case2; (* outputValue *) box "Case2" |]
        [| (* inputValue *) box Union.Case3; (* outputValue *) box "Case3" |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize enum union with single case from required string`` =
    type Union = Case1
    type Input = { Field1: string }
    type Output = { Field1: Union }

    [<Fact>]
    let ``value`` () =
        let inputRecords = [| { Input.Field1 = "Case1" } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 } |] @>

module ``deserialize enum union with single case from optional string`` =
    type Union = Case1
    type Input = { Field1: string option }
    type Output = { Field1: Union }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<Union>.FullName}'" @>)

    [<Fact>]
    let ``non-null value`` () =
        let inputRecords = [| { Input.Field1 = Option.Some "Case1" } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 } |] @>

module ``deserialize enum union with multiple cases from required string`` =
    type Union = Case1 | Case2 | Case3
    type Input = { Field1: string }
    type Output = { Field1: Union }

    let Value = [|
        [| (* inputValue *) box "Case1"; (* outputValue *) box Union.Case1 |]
        [| (* inputValue *) box "Case2"; (* outputValue *) box Union.Case2 |]
        [| (* inputValue *) box "Case3"; (* outputValue *) box Union.Case3 |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize enum union with multiple cases from optional string`` =
    type Union = Case1 | Case2 | Case3
    type Input = { Field1: string option }
    type Output = { Field1: Union }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<Union>.FullName}'" @>)

    let NonNullValue = [|
        [| (* inputValue *) box "Case1"; (* outputValue *) box Union.Case1 |]
        [| (* inputValue *) box "Case2"; (* outputValue *) box Union.Case2 |]
        [| (* inputValue *) box "Case3"; (* outputValue *) box Union.Case3 |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize enum union from required string with unknown case name`` =
    type Union = Case1 | Case2 | Case3
    type Input = { Field1: string }
    type Output = { Field1: Union }

    [<Theory>]
    [<InlineData("Unknown")>]
    [<InlineData("case1")>]
    [<InlineData("case_2")>]
    [<InlineData("Case4")>]
    let ``value`` caseName =
        let inputRecords = [| { Input.Field1 = caseName } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"encountered invalid case name '{caseName}' during"
                    + " deserialization of enum union type"
                    + $" '{typeof<Union>.FullName}'" @>)

module ``deserialize enum union from optional string with unknown case name`` =
    type Union = Case1 | Case2 | Case3
    type Input = { Field1: string option }
    type Output = { Field1: Union }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for"
                    + $" non-nullable type '{typeof<Union>.FullName}'" @>)

    [<Theory>]
    [<InlineData("Unknown")>]
    [<InlineData("case1")>]
    [<InlineData("case_2")>]
    [<InlineData("Case4")>]
    let ``value`` caseName =
        let inputRecords = [| { Input.Field1 = Option.Some caseName } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"encountered invalid case name '{caseName}' during"
                    + " deserialization of enum union type"
                    + $" '{typeof<Union>.FullName}'" @>)
