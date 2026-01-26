namespace Parquet.FSharp.Tests.DefaultConverter.Union

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``serialize single case union with atomic field`` =
    type Union = Case1 of field2:int
    type UnionRecord = { field2: int }
    type Input = { Field1: Union }
    type Output = { Field1: UnionRecord }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
                    Assert.Field.isRequired
                    Assert.Field.Type.isInt32
                    Assert.Field.LogicalType.isInteger 32 true
                    Assert.Field.ConvertedType.isInt32
                    Assert.Field.hasNoChildren ] ] ]

    [<Fact>]
    let ``value`` () =
        let inputRecords = [| { Input.Field1 = Union.Case1 1 } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = 1 }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``serialize single case union with list field`` =
    type Union = Case1 of field2:int list
    type UnionRecord = { field2: int list }
    type Input = { Field1: Union }
    type Output = { Field1: UnionRecord }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
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
                            Assert.Field.Type.isInt32
                            Assert.Field.LogicalType.isInteger 32 true
                            Assert.Field.ConvertedType.isInt32
                            Assert.Field.hasNoChildren ] ] ] ] ]

    let Value = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = Union.Case1 value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = value }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``serialize single case union with record field`` =
    type Record = { Field3: int }
    type Union = Case1 of field2:Record
    type UnionRecord = { field2: Record }
    type Input = { Field1: Union }
    type Output = { Field1: UnionRecord }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
                    Assert.Field.isRequired
                    Assert.Field.Type.hasNoValue
                    Assert.Field.LogicalType.hasNoValue
                    Assert.Field.ConvertedType.hasNoValue
                    Assert.Field.child [
                        Assert.Field.nameEquals "Field3"
                        Assert.Field.isRequired
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``value`` () =
        let value = { Record.Field3 = 1 }
        let inputRecords = [| { Input.Field1 = Union.Case1 value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = value }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``serialize single case union with optional field`` =
    type Union = Case1 of field2:int option
    type UnionRecord = { field2: int option }
    type Input = { Field1: Union }
    type Output = { Field1: UnionRecord }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.child [
                    Assert.Field.nameEquals "field2"
                    Assert.Field.isOptional
                    Assert.Field.Type.isInt32
                    Assert.Field.LogicalType.isInteger 32 true
                    Assert.Field.ConvertedType.isInt32
                    Assert.Field.hasNoChildren ] ] ]

    [<Fact>]
    let ``value`` () =
        let value = Option.Some 1
        let inputRecords = [| { Input.Field1 = Union.Case1 value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = value }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``serialize single case union with multiple fields`` =
    type Union = Case1 of field2:int * field3:int * field4:int
    type UnionRecord = { field2: int; field3: int; field4: int }
    type Input = { Field1: Union }
    type Output = { Field1: UnionRecord }

    let assertSchemaMatchesExpected schema =
        Assert.schema schema [
            Assert.field [
                Assert.Field.nameEquals "Field1"
                Assert.Field.isRequired
                Assert.Field.Type.hasNoValue
                Assert.Field.LogicalType.hasNoValue
                Assert.Field.ConvertedType.hasNoValue
                Assert.Field.children [
                    Assert.field [
                        Assert.Field.nameEquals "field2"
                        Assert.Field.isRequired
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "field3"
                        Assert.Field.isRequired
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "field4"
                        Assert.Field.isRequired
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``value`` () =
        let inputRecords = [| { Input.Field1 = Union.Case1 (1, 2, 3) } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedValue = { UnionRecord.field2 = 1; field3 = 2; field4 = 3 }
        test <@ outputRecords = [| { Output.Field1 = expectedValue } |] @>

module ``deserialize single case union with atomic field from required record`` =
    type UnionRecord = { field2: int }
    type Union = Case1 of field2:int
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    [<Fact>]
    let ``value`` () =
        let unionRecord = { UnionRecord.field2 = 1 }
        let inputRecords = [| { Input.Field1 = unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 1 } |] @>

module ``deserialize single case union with atomic field from optional record`` =
    type UnionRecord = { field2: int }
    type Union = Case1 of field2:int
    type Input = { Field1: UnionRecord option }
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
        let unionRecord = { UnionRecord.field2 = 1 }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 1 } |] @>

module ``deserialize single case union with list field from required record`` =
    type UnionRecord = { field2: int list }
    type Union = Case1 of field2:int list
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    let Value = [|
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let unionRecord = { UnionRecord.field2 = value }
        let inputRecords = [| { Input.Field1 = unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 value } |] @>

module ``deserialize single case union with list field from optional record`` =
    type UnionRecord = { field2: int list }
    type Union = Case1 of field2:int list
    type Input = { Field1: UnionRecord option }
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
        [| box<int list> (**) [] (**) |]
        [| box<int list> (**) [ 1 ] (**) |]
        [| box<int list> (**) [ 1; 2; 3 ] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let unionRecord = { UnionRecord.field2 = value }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 value } |] @>

module ``deserialize single case union with record field from required record`` =
    type Record = { Field3: int }
    type UnionRecord = { field2: Record }
    type Union = Case1 of field2:Record
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    [<Fact>]
    let ``value`` () =
        let value = { Record.Field3 = 1 }
        let unionRecord = { UnionRecord.field2 = value }
        let inputRecords = [| { Input.Field1 = unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 value } |] @>

module ``deserialize single case union with record field from optional record`` =
    type Record = { Field3: int }
    type UnionRecord = { field2: Record }
    type Union = Case1 of field2:Record
    type Input = { Field1: UnionRecord option }
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
        let value = { Record.Field3 = 1 }
        let unionRecord = { UnionRecord.field2 = value }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 value } |] @>

module ``deserialize single case union with optional field from required record`` =
    type UnionRecord = { field2: int option }
    type Union = Case1 of field2:int option
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    let Value = [|
        [| box<int option> <| (**) Option.None (**) |]
        [| box<int option> <| (**) Option.Some 1 (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let unionRecord = { UnionRecord.field2 = value }
        let inputRecords = [| { Input.Field1 = unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 value } |] @>

module ``deserialize single case union with optional field from optional record`` =
    type UnionRecord = { field2: int option }
    type Union = Case1 of field2:int option
    type Input = { Field1: UnionRecord option }
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
        [| box<int option> <| (**) Option.None (**) |]
        [| box<int option> <| (**) Option.Some 1 (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let unionRecord = { UnionRecord.field2 = value }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 value } |] @>

module ``deserialize single case union with multiple fields from required record`` =
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Union = Case1 of field2:int * field3:bool * field4:float
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    [<Fact>]
    let ``value`` () =
        let unionRecord = { UnionRecord.field2 = 1; field3 = true; field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 (1, true, 2.34) } |] @>

module ``deserialize single case union with multiple fields from optional record`` =
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Union = Case1 of field2:int * field3:bool * field4:float
    type Input = { Field1: UnionRecord option }
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
        let unionRecord = { UnionRecord.field2 = 1; field3 = true; field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 (1, true, 2.34) } |] @>

module ``deserialize single case union with out-of-order fields from required record`` =
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Union = Case1 of field3:bool * field4:float * field2:int
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    [<Fact>]
    let ``value`` () =
        let unionRecord = { UnionRecord.field2 = 1; field3 = true; field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 (true, 2.34, 1) } |] @>

module ``deserialize single case union with out-of-order fields from optional record`` =
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Union = Case1 of field3:bool * field4:float * field2:int
    type Input = { Field1: UnionRecord option }
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
        let unionRecord = { UnionRecord.field2 = 1; field3 = true; field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 (true, 2.34, 1) } |] @>

module ``deserialize single case union with subset of fields from required record`` =
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Union = Case1 of field2:int * field4:float
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    [<Fact>]
    let ``value`` () =
        let unionRecord = { UnionRecord.field2 = 1; field3 = true; field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 (1, 2.34) } |] @>

module ``deserialize single case union with subset of fields from optional record`` =
    type UnionRecord = { field2: int; field3: bool; field4: float }
    type Union = Case1 of field2:int * field4:float
    type Input = { Field1: UnionRecord option }
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
        let unionRecord = { UnionRecord.field2 = 1; field3 = true; field4 = 2.34 }
        let inputRecords = [| { Input.Field1 = Option.Some unionRecord } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = Union.Case1 (1, 2.34) } |] @>
