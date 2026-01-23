namespace Parquet.FSharp.Tests.DefaultConverter.ResizeArray

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``serialize resize array with atomic elements`` =
    type Input = { Field1: ResizeArray<int> }
    type Output = { Field1: ResizeArray<int> }

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
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<ResizeArray<int>>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNull = [|
        [| box <| (**) ResizeArray<int>() (**) |]
        [| box <| (**) ResizeArray<int>([ 1 ]) (**) |]
        [| box <| (**) ResizeArray<int>([ 1; 2; 3 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords

module ``serialize resize array with list elements`` =
    type Input = { Field1: ResizeArray<int[]> }
    type Output = { Field1: ResizeArray<int[]> }

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
                                Assert.Field.hasNoChildren ] ] ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<ResizeArray<int[]>>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNull = [|
        [| box <| (**) ResizeArray<int[]>() (**) |]
        [| box <| (**) ResizeArray<int[]>([ [||] ]) (**) |]
        [| box <| (**) ResizeArray<int[]>([ [| 1; 2; 3 |] ]) (**) |]
        [| box <| (**) ResizeArray<int[]>([ [||]; [||]; [||] ]) (**) |]
        [| box <| (**) ResizeArray<int[]>([ [| 1 |]; [||]; [| 2; 3; 4 |] ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords

module ``serialize resize array with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: ResizeArray<Record> }
    type Output = { Field1: ResizeArray<Record> }

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
                        Assert.Field.Type.hasNoValue
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.child [
                            Assert.Field.nameEquals "Field2"
                            Assert.Field.isRequired
                            Assert.Field.Type.isInt32
                            Assert.Field.LogicalType.isInteger 32 true
                            Assert.Field.ConvertedType.isInt32
                            Assert.Field.hasNoChildren ] ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<ResizeArray<Record>>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNull = [|
        [| box <| (**) ResizeArray<Record>() (**) |]
        [| box <| (**) ResizeArray<Record>([ { Field2 = 1 } ]) (**) |]
        [| box <| (**) ResizeArray<Record>([ { Field2 = 1 }; { Field2 = 2 }; { Field2 = 3 } ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords

module ``serialize resize array with optional elements`` =
    type Input = { Field1: ResizeArray<int option> }
    type Output = { Field1: ResizeArray<int option> }

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
                        Assert.Field.isOptional
                        Assert.Field.Type.isInt32
                        Assert.Field.LogicalType.isInteger 32 true
                        Assert.Field.ConvertedType.isInt32
                        Assert.Field.hasNoChildren ] ] ] ]

    [<Fact>]
    let ``null`` () =
        let inputRecords = [| { Input.Field1 = null } |]
        raisesWith<SerializationException>
            <@ ParquetSerializer.Serialize(inputRecords) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during serialization for type"
                    + $" '{typeof<ResizeArray<int option>>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNull = [|
        [| box <| (**) ResizeArray<int option>([]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.None ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.Some 1; ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.None; Option.None; Option.None ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.Some 1; Option.None; Option.Some 3 ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.Some 1; Option.Some 2; Option.Some 3 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords
        
module ``deserialize resize array with atomic elements from required list with atomic elements`` =
    type Input = { Field1: ResizeArray<int> }
    type Output = { Field1: ResizeArray<int> }

    let Value = [|
        [| box <| (**) ResizeArray<int>() (**) |]
        [| box <| (**) ResizeArray<int>([ 1 ]) (**) |]
        [| box <| (**) ResizeArray<int>([ 1; 2; 3 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords
        
module ``deserialize resize array with atomic elements from optional list with atomic elements`` =
    type Input = { Field1: ResizeArray<int> option }
    type Output = { Field1: ResizeArray<int> }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<ResizeArray<int>>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNullValue = [|
        [| box <| (**) ResizeArray<int>() (**) |]
        [| box <| (**) ResizeArray<int>([ 1 ]) (**) |]
        [| box <| (**) ResizeArray<int>([ 1; 2; 3 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords
        
module ``deserialize resize array with list elements from required list with list elements`` =
    type Input = { Field1: ResizeArray<int[]> }
    type Output = { Field1: ResizeArray<int[]> }

    let Value = [|
        [| box <| (**) ResizeArray<int[]>() (**) |]
        [| box <| (**) ResizeArray<int[]>([ [||] ]) (**) |]
        [| box <| (**) ResizeArray<int[]>([ [| 1; 2; 3 |] ]) (**) |]
        [| box <| (**) ResizeArray<int[]>([ [||]; [||]; [||] ]) (**) |]
        [| box <| (**) ResizeArray<int[]>([ [| 1 |]; [||]; [| 2; 3; 4 |] ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords
        
module ``deserialize resize array with list elements from optional list with list elements`` =
    type Input = { Field1: ResizeArray<int[]> option }
    type Output = { Field1: ResizeArray<int[]> }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<ResizeArray<int[]>>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNullValue = [|
        [| box <| (**) ResizeArray<int[]>() (**) |]
        [| box <| (**) ResizeArray<int[]>([ [||] ]) (**) |]
        [| box <| (**) ResizeArray<int[]>([ [| 1; 2; 3 |] ]) (**) |]
        [| box <| (**) ResizeArray<int[]>([ [||]; [||]; [||] ]) (**) |]
        [| box <| (**) ResizeArray<int[]>([ [| 1 |]; [||]; [| 2; 3; 4 |] ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords
        
module ``deserialize resize array with record elements from required list with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: ResizeArray<Record> }
    type Output = { Field1: ResizeArray<Record> }

    let Value = [|
        [| box <| (**) ResizeArray<Record>() (**) |]
        [| box <| (**) ResizeArray<Record>([ { Field2 = 1 } ]) (**) |]
        [| box <| (**) ResizeArray<Record>([ { Field2 = 1 }; { Field2 = 2 }; { Field2 = 3 } ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords
        
module ``deserialize resize array with record elements from optional list with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: ResizeArray<Record> option }
    type Output = { Field1: ResizeArray<Record> }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<ResizeArray<Record>>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNullValue = [|
        [| box <| (**) ResizeArray<Record>() (**) |]
        [| box <| (**) ResizeArray<Record>([ { Field2 = 1 } ]) (**) |]
        [| box <| (**) ResizeArray<Record>([ { Field2 = 1 }; { Field2 = 2 }; { Field2 = 3 } ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords
        
module ``deserialize resize array with optional elements from required list with optional elements`` =
    type Input = { Field1: ResizeArray<int option> }
    type Output = { Field1: ResizeArray<int option> }

    let Value = [|
        [| box <| (**) ResizeArray<int option>([]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.None ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.Some 1; ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.None; Option.None; Option.None ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.Some 1; Option.None; Option.Some 3 ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.Some 1; Option.Some 2; Option.Some 3 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords
        
module ``deserialize resize array with optional elements from optional list with optional elements`` =
    type Input = { Field1: ResizeArray<int option> option }
    type Output = { Field1: ResizeArray<int option> }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<ResizeArray<int option>>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNullValue = [|
        [| box <| (**) ResizeArray<int option>([]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.None ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.Some 1; ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.None; Option.None; Option.None ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.Some 1; Option.None; Option.Some 3 ]) (**) |]
        [| box <| (**) ResizeArray<int option>([ Option.Some 1; Option.Some 2; Option.Some 3 ]) (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        let expectedRecords = [| { Output.Field1 = value } |]
        Assert.seqEqual expectedRecords outputRecords
