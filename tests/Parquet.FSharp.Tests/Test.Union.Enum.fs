namespace Parquet.FSharp.Tests.Union.Enum

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

// TODO: Add tests for serializing null values
// TODO: Add tests for struct unions

module ``{ default } serialize with single case`` =
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

module ``{ default } serialize with multiple cases`` =
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

module ``{ optional=true } serialize with single case`` =
    type Union = Case1
    type Input = { [<ParquetUnionEnum(Optional = true)>] Field1: Union }
    type Output = { Field1: string option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
        test <@ outputRecords = [| { Output.Field1 = Option.Some "Case1" } |] @>

module ``{ optional=true } serialize with multiple cases`` =
    type Union = Case1 | Case2 | Case3
    type Input = { [<ParquetUnionEnum(Optional = true)>] Field1: Union }
    type Output = { Field1: string option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
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
        test <@ outputRecords = [| { Output.Field1 = Option.Some outputValue } |] @>

module ``{ default } deserialize with single case`` =
    type Union = Case1
    type Input = { Field1: string }
    type Output = { Field1: Union }

    [<Fact>]
    let ``value`` () =
        let inputRecords = [| { Input.Field1 = "Case1" } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 } |] @>

    [<Theory>]
    [<InlineData("Unknown")>]
    [<InlineData("case1")>]
    [<InlineData("case_2")>]
    [<InlineData("Case4")>]
    let ``invalid case name`` caseName =
        let inputRecords = [| { Input.Field1 = caseName } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"encountered invalid case name '{caseName}' during"
                    + $" deserialization of enum union type '{typeof<Union>}'" @>)

module ``{ default } deserialize with multiple cases`` =
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

    [<Theory>]
    [<InlineData("Unknown")>]
    [<InlineData("case1")>]
    [<InlineData("case_2")>]
    [<InlineData("Case4")>]
    let ``invalid case name`` caseName =
        let inputRecords = [| { Input.Field1 = caseName } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"encountered invalid case name '{caseName}' during"
                    + $" deserialization of enum union type '{typeof<Union>}'" @>)

module ``{ optional=true } deserialize with single case`` =
    type Union = Case1
    type Input = { Field1: string option }
    type Output = { [<ParquetUnionEnum(Optional = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"null value encountered during deserialization for type '{typeof<Union>}' for"
                    + " which nulls are not allowed by default" @>)

    [<Fact>]
    let ``non-null`` () =
        let inputRecords = [| { Input.Field1 = Option.Some "Case1" } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 } |] @>

    [<Theory>]
    [<InlineData("Unknown")>]
    [<InlineData("case1")>]
    [<InlineData("case_2")>]
    [<InlineData("Case4")>]
    let ``invalid case name`` caseName =
        let inputRecords = [| { Input.Field1 = Option.Some caseName } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"encountered invalid case name '{caseName}' during"
                    + $" deserialization of enum union type '{typeof<Union>}'" @>)

module ``{ optional=true } deserialize with multiple cases`` =
    type Union = Case1 | Case2 | Case3
    type Input = { Field1: string option }
    type Output = { [<ParquetUnionEnum(Optional = true)>] Field1: Union }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"null value encountered during deserialization for type '{typeof<Union>}' for"
                    + " which nulls are not allowed by default" @>)

    let NonNull = [|
        [| (* inputValue *) box "Case1"; (* outputValue *) box Union.Case1 |]
        [| (* inputValue *) box "Case2"; (* outputValue *) box Union.Case2 |]
        [| (* inputValue *) box "Case3"; (* outputValue *) box Union.Case3 |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

    [<Theory>]
    [<InlineData("Unknown")>]
    [<InlineData("case1")>]
    [<InlineData("case_2")>]
    [<InlineData("Case4")>]
    let ``invalid case name`` caseName =
        let inputRecords = [| { Input.Field1 = Option.Some caseName } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    $"encountered invalid case name '{caseName}' during"
                    + $" deserialization of enum union type '{typeof<Union>}'" @>)
