namespace Parquet.FSharp.Tests.List.DateTime

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open System
open Xunit

module ``list:{ default } dateTime:{ default } serialize`` =
    type Input = {
        Field1: DateTime list }

    type Output = {
        Field1: DateTime list }

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
                        Assert.Field.Type.isInt64
                        Assert.Field.LogicalType.isTimestamp "utc" "microseconds"
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<DateTime list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<DateTime list>}' for which nulls are not"
                    + " allowed by default" @>)

    let NonNull = [|
        [| box<DateTime list> (**) [] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch ] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch; DateTime.UnixEpoch.AddDays(1) ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        for value in outputRecords[0].Field1 do
            let kind = value.Kind
            test <@ kind = DateTimeKind.Utc @>

module ``list:{ default } dateTime:{ non-default } serialize`` =
    type Input = {
        [<ParquetDateTime(NestingLevel = 1, Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime list }

    type Output = {
        [<ParquetDateTime(NestingLevel = 1, Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime list }

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
                        Assert.Field.Type.isInt64
                        Assert.Field.LogicalType.isTimestamp "utc" "milliseconds"
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<DateTime list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<DateTime list>}' for which nulls are not"
                    + " allowed by default" @>)

    let NonNull = [|
        [| box<DateTime list> (**) [] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch ] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch; DateTime.UnixEpoch.AddDays(1) ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        for value in outputRecords[0].Field1 do
            let kind = value.Kind
            test <@ kind = DateTimeKind.Utc @>

module ``list:{ optional=true } dateTime:{ default } serialize`` =
    type Input = {
        [<ParquetList(Optional = true)>]
        Field1: DateTime list }

    type Output = {
        Field1: DateTime list option }

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
                        Assert.Field.Type.isInt64
                        Assert.Field.LogicalType.isTimestamp "utc" "microseconds"
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<DateTime list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<DateTime list>}' for which nulls are not"
                    + " allowed by default" @>)

    let NonNull = [|
        [| box<DateTime list> (**) [] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch ] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch; DateTime.UnixEpoch.AddDays(1) ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        for value in outputRecords[0].Field1.Value do
            let kind = value.Kind
            test <@ kind = DateTimeKind.Utc @>

module ``list:{ optional=true } dateTime:{ non-default } serialize`` =
    type Input = {
        [<ParquetList(Optional = true)>]
        [<ParquetDateTime(NestingLevel = 1, Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime list }

    type Output = {
        [<ParquetDateTime(NestingLevel = 2, Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime list option }

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
                        Assert.Field.Type.isInt64
                        Assert.Field.LogicalType.isTimestamp "utc" "milliseconds"
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Unchecked.defaultof<DateTime list> } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<DateTime list>}' for which nulls are not"
                    + " allowed by default" @>)

    let NonNull = [|
        [| box<DateTime list> (**) [] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch ] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch; DateTime.UnixEpoch.AddDays(1) ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Option.Some value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        for value in outputRecords[0].Field1.Value do
            let kind = value.Kind
            test <@ kind = DateTimeKind.Utc @>

module ``list:{ default } dateTime:{ default } deserialize`` =
    type Input = {
        Field1: DateTime list }

    type Output = {
        Field1: DateTime list }

    let Value = [|
        [| box<DateTime list> (**) [] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch ] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch; DateTime.UnixEpoch.AddDays(1) ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        for value in outputRecords[0].Field1 do
            let kind = value.Kind
            test <@ kind = DateTimeKind.Utc @>

module ``list:{ default } dateTime:{ non-default } deserialize`` =
    type Input = {
        [<ParquetDateTime(NestingLevel = 1, Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime list }

    type Output = {
        [<ParquetDateTime(NestingLevel = 1, Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime list }

    let Value = [|
        [| box<DateTime list> (**) [] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch ] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch; DateTime.UnixEpoch.AddDays(1) ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        for value in outputRecords[0].Field1 do
            let kind = value.Kind
            test <@ kind = DateTimeKind.Utc @>

module ``list:{ optional=true } dateTime:{ default } deserialize`` =
    type Input = {
        Field1: DateTime list option }

    type Output = {
        [<ParquetList(Optional = true)>]
        Field1: DateTime list }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<DateTime list>}' for which nulls are not"
                    + " allowed by default" @>)

    let NonNull = [|
        [| box<DateTime list> (**) [] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch ] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch; DateTime.UnixEpoch.AddDays(1) ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        for value in outputRecords[0].Field1 do
            let kind = value.Kind
            test <@ kind = DateTimeKind.Utc @>

module ``list:{ optional=true } dateTime:{ non-default } deserialize`` =
    type Input = {
        [<ParquetDateTime(NestingLevel = 2, Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime list option }

    type Output = {
        [<ParquetList(Optional = true)>]
        [<ParquetDateTime(NestingLevel = 1, Unit = TimeUnit.Milliseconds)>]
        Field1: DateTime list }

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<DateTime list>}' for which nulls are not"
                    + " allowed by default" @>)

    let NonNull = [|
        [| box<DateTime list> (**) [] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch ] (**) |]
        [| box<DateTime list> (**) [ DateTime.UnixEpoch; DateTime.UnixEpoch.AddDays(1) ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        // Default {DateTime} equality only compares the number of ticks and
        // ignores the {DateTimeKind}, so we need to check this separately.
        for value in outputRecords[0].Field1 do
            let kind = value.Kind
            test <@ kind = DateTimeKind.Utc @>
