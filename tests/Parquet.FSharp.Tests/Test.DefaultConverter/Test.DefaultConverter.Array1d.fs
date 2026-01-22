namespace Parquet.FSharp.Tests.DefaultConverter.Array1d

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``serialize 1d array with atomic elements`` =
    type Input = { Field1: int[] }
    type Output = { Field1: int[] }

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
                    + $" '{typeof<int[]>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNull = [|
        [| box<int[]> (**) [||] (**) |]
        [| box<int[]> (**) [| 1 |] (**) |]
        [| box<int[]> (**) [| 1; 2; 3 |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize 1d array with list elements`` =
    type Input = { Field1: int[][] }
    type Output = { Field1: int[][] }

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
                    + $" '{typeof<int[][]>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNull = [|
        [| box<int[][]> (**) [||] (**) |]
        [| box<int[][]> (**) [| [||] |] (**) |]
        [| box<int[][]> (**) [| [| 1; 2; 3 |] |] (**) |]
        [| box<int[][]> (**) [| [||]; [||]; [||] |] (**) |]
        [| box<int[][]> (**) [| [| 1 |]; [||]; [| 2; 3; 4 |] |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize 1d array with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record[] }
    type Output = { Field1: Record[] }

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
                    + $" '{typeof<Record[]>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNull = [|
        [| box<Record[]> (**) [||] (**) |]
        [| box<Record[]> (**) [| { Field2 = 1 } |] (**) |]
        [| box<Record[]> (**) [| { Field2 = 1 }; { Field2 = 2 }; { Field2 = 3 } |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>

module ``serialize 1d array with optional elements`` =
    type Input = { Field1: option<int>[] }
    type Output = { Field1: option<int>[] }

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
                    + $" '{typeof<option<int>[]>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNull = [|
        [| box<option<int>[]> (**) [||] (**) |]
        [| box<option<int>[]> (**) [| Option.None |] (**) |]
        [| box<option<int>[]> (**) [| Option.Some 1; |] (**) |]
        [| box<option<int>[]> (**) [| Option.None; Option.None; Option.None |] (**) |]
        [| box<option<int>[]> (**) [| Option.Some 1; Option.None; Option.Some 3 |] (**) |]
        [| box<option<int>[]> (**) [| Option.Some 1; Option.Some 2; Option.Some 3 |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNull)>]
    let ``non-null`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize 1d array with atomic elements from required list with atomic elements`` =
    type Input = { Field1: int[] }
    type Output = { Field1: int[] }

    let Value = [|
        [| box<int[]> (**) [||] (**) |]
        [| box<int[]> (**) [| 1 |] (**) |]
        [| box<int[]> (**) [| 1; 2; 3 |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize 1d array with atomic elements from optional list with atomic elements`` =
    type Input = { Field1: int[] option }
    type Output = { Field1: int[] }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<int[]>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNullValue = [|
        [| box<int[]> (**) [||] (**) |]
        [| box<int[]> (**) [| 1 |] (**) |]
        [| box<int[]> (**) [| 1; 2; 3 |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize 1d array with list elements from required list with list elements`` =
    type Input = { Field1: int[][] }
    type Output = { Field1: int[][] }

    let Value = [|
        [| box<int[][]> (**) [||] (**) |]
        [| box<int[][]> (**) [| [||] |] (**) |]
        [| box<int[][]> (**) [| [| 1; 2; 3 |] |] (**) |]
        [| box<int[][]> (**) [| [||]; [||]; [||] |] (**) |]
        [| box<int[][]> (**) [| [| 1 |]; [||]; [| 2; 3; 4 |] |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize 1d array with list elements from optional list with list elements`` =
    type Input = { Field1: int[][] option }
    type Output = { Field1: int[][] }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<int[][]>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNullValue = [|
        [| box<int[][]> (**) [||] (**) |]
        [| box<int[][]> (**) [| [||] |] (**) |]
        [| box<int[][]> (**) [| [| 1; 2; 3 |] |] (**) |]
        [| box<int[][]> (**) [| [||]; [||]; [||] |] (**) |]
        [| box<int[][]> (**) [| [| 1 |]; [||]; [| 2; 3; 4 |] |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize 1d array with record elements from required list with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record[] }
    type Output = { Field1: Record[] }

    let Value = [|
        [| box<Record[]> (**) [||] (**) |]
        [| box<Record[]> (**) [| { Field2 = 1 } |] (**) |]
        [| box<Record[]> (**) [| { Field2 = 1 }; { Field2 = 2 }; { Field2 = 3 } |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize 1d array with record elements from optional list with record elements`` =
    type Record = { Field2: int }
    type Input = { Field1: Record[] option }
    type Output = { Field1: Record[] }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<Record[]>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNullValue = [|
        [| box<Record[]> (**) [||] (**) |]
        [| box<Record[]> (**) [| { Field2 = 1 } |] (**) |]
        [| box<Record[]> (**) [| { Field2 = 1 }; { Field2 = 2 }; { Field2 = 3 } |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize 1d array with optional elements from required list with optional elements`` =
    type Input = { Field1: option<int>[] }
    type Output = { Field1: option<int>[] }

    let Value = [|
        [| box<option<int>[]> (**) [||] (**) |]
        [| box<option<int>[]> (**) [| Option.None |] (**) |]
        [| box<option<int>[]> (**) [| Option.Some 1; |] (**) |]
        [| box<option<int>[]> (**) [| Option.None; Option.None; Option.None |] (**) |]
        [| box<option<int>[]> (**) [| Option.Some 1; Option.None; Option.Some 3 |] (**) |]
        [| box<option<int>[]> (**) [| Option.Some 1; Option.Some 2; Option.Some 3 |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` value =
        let inputRecords = [| { Input.Field1 = value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
        
module ``deserialize 1d array with optional elements from optional list with optional elements`` =
    type Input = { Field1: option<int>[] option }
    type Output = { Field1: option<int>[] }

    [<Fact>]
    let ``null value`` () =
        let inputRecords = [| { Input.Field1 = Option.None } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        raisesWith<SerializationException>
            <@ ParquetSerializer.Deserialize<Output>(bytes) @>
            (fun exn ->
                <@ exn.Message =
                    "null value encountered during deserialization for type"
                    + $" '{typeof<option<int>[]>.FullName}' which is not treated as"
                    + " nullable by default" @>)

    let NonNullValue = [|
        [| box<option<int>[]> (**) [||] (**) |]
        [| box<option<int>[]> (**) [| Option.None |] (**) |]
        [| box<option<int>[]> (**) [| Option.Some 1; |] (**) |]
        [| box<option<int>[]> (**) [| Option.None; Option.None; Option.None |] (**) |]
        [| box<option<int>[]> (**) [| Option.Some 1; Option.None; Option.Some 3 |] (**) |]
        [| box<option<int>[]> (**) [| Option.Some 1; Option.Some 2; Option.Some 3 |] (**) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` value =
        let inputRecords = [| { Input.Field1 = Option.Some value } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = value } |] @>
