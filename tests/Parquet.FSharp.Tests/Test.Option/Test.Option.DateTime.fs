namespace Parquet.FSharp.Tests.Option.DateTime

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``option:{ default } dateTime:{ default } serialize`` =
    type Input = {
        Field1: DateTime option }

    type Output = {
        Field1: DateTime option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isTimestamp "utc" "microseconds"
                Assert.Field.ConvertedType.hasNoValue
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
        let value = DateTime.UnixEpoch.AddDays(1)
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``option:{ default } dateTime:{ non-default } serialize`` =
    type Input = {
        [<ParquetNestedDateTime(Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime option }

    type Output = {
        [<ParquetNestedDateTime(Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime option }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isOptional
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isTimestamp "utc" "milliseconds"
                Assert.Field.ConvertedType.hasNoValue
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
        let value = DateTime.UnixEpoch.AddDays(1)
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``option:{ required=true } dateTime:{ default } serialize`` =
    type Input = {
        [<ParquetOptionField(Required = true)>]
        Field1: DateTime option }

    type Output = {
        Field1: DateTime }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isTimestamp "utc" "microseconds"
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``none`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<DateTime option>}' which has been"
                    + " configured as required" @>)

    [<Fact>]
    let ``some`` () =
        let value = DateTime.UnixEpoch.AddDays(1)
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``option:{ required=true } dateTime:{ non-default } serialize`` =
    type Input = {
        [<ParquetOptionField(Required = true)>]
        [<ParquetNestedDateTime(Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime option }

    type Output = {
        [<ParquetDateTimeField(Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.isInt64
                Assert.Field.LogicalType.isTimestamp "utc" "milliseconds"
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.hasNoChildren ] ]

    [<Fact>]
    let ``none`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<DateTime option>}' which has been"
                    + " configured as required" @>)

    [<Fact>]
    let ``some`` () =
        let value = DateTime.UnixEpoch.AddDays(1)
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``option:{ default } dateTime:{ default } deserialize`` =
    type Input = {
        Field1: DateTime option }

    type Output = {
        Field1: DateTime option }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``non-null`` () =
        let value = DateTime.UnixEpoch.AddDays(1)
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``option:{ default } dateTime:{ non-default } deserialize`` =
    type Input = {
        [<ParquetNestedDateTime(Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime option }

    type Output = {
        [<ParquetNestedDateTime(Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime option }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.None } |] @>

    [<Fact>]
    let ``non-null`` () =
        let value = DateTime.UnixEpoch.AddDays(1)
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``option:{ required=true } dateTime:{ default } deserialize`` =
    type Input = {
        Field1: DateTime }

    type Output = {
        [<ParquetOptionField(Required = true)>]
        Field1: DateTime option }

    [<Fact>]
    let ``value`` () =
        let value = DateTime.UnixEpoch.AddDays(1)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>

module ``option:{ required=true } dateTime:{ non-default } deserialize`` =
    type Input = {
        [<ParquetDateTimeField(Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime }

    type Output = {
        [<ParquetOptionField(Required = true)>]
        [<ParquetNestedDateTime(Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime option }

    [<Fact>]
    let ``value`` () =
        let value = DateTime.UnixEpoch.AddDays(1)
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>
