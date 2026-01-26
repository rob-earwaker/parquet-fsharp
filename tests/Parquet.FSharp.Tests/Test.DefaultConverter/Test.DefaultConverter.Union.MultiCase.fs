namespace Parquet.FSharp.Tests.DefaultConverter.Union

open Parquet.FSharp
open Parquet.FSharp.Tests
open Swensen.Unquote
open Xunit

module ``serialize multi case union with atomic field`` =
    type Union = Case1 | Case2 of field2:int
    type Case2Record = { field2: int }
    type UnionRecord = { Type: string; Case2: Case2Record option }
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
                        Assert.Field.nameEquals "Type"
                        Assert.Field.isRequired
                        Assert.Field.Type.isByteArray
                        Assert.Field.LogicalType.isString
                        Assert.Field.ConvertedType.isUtf8
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "Case2"
                        Assert.Field.isOptional
                        Assert.Field.Type.hasNoValue
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.child [
                            Assert.Field.nameEquals "field2"
                            Assert.Field.isRequired
                            Assert.Field.Type.isInt32
                            Assert.Field.LogicalType.isInteger 32 true
                            Assert.Field.ConvertedType.isInt32
                            Assert.Field.hasNoChildren ] ] ] ] ]

    let Value = [|
        [| (* inputValue *) box <| Union.Case1;
            (* outputValue *) box {
                UnionRecord.Type = "Case1"; Case2 = Option.None } |]

        [| (* inputValue *) box <| Union.Case2 1;
            (* outputValue *) box {
                UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = 1 } } |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``serialize multi case union with list field`` =
    type Union = Case1 | Case2 of field2:int list
    type Case2Record = { field2: int list }
    type UnionRecord = { Type: string; Case2: Case2Record option }
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
                        Assert.Field.nameEquals "Type"
                        Assert.Field.isRequired
                        Assert.Field.Type.isByteArray
                        Assert.Field.LogicalType.isString
                        Assert.Field.ConvertedType.isUtf8
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "Case2"
                        Assert.Field.isOptional
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
                                    Assert.Field.hasNoChildren ] ] ] ] ] ] ]

    let Value = [|
        [| (* inputValue *) box <| Union.Case1;
            (* outputValue *) box {
                UnionRecord.Type = "Case1"; Case2 = Option.None } |]

        [| (* inputValue *) box <| Union.Case2 [];
            (* outputValue *) box {
                UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = [] } } |]

        [| (* inputValue *) box <| Union.Case2 [ 1 ];
            (* outputValue *) box {
                UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = [ 1 ] } } |]

        [| (* inputValue *) box <| Union.Case2 [ 1; 2; 3 ];
            (* outputValue *) box {
                UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = [ 1; 2; 3 ] } } |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``serialize multi case union with record field`` =
    type Record = { Field3: int }
    type Union = Case1 | Case2 of field2:Record
    type Case2Record = { field2: Record }
    type UnionRecord = { Type: string; Case2: Case2Record option }
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
                        Assert.Field.nameEquals "Type"
                        Assert.Field.isRequired
                        Assert.Field.Type.isByteArray
                        Assert.Field.LogicalType.isString
                        Assert.Field.ConvertedType.isUtf8
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "Case2"
                        Assert.Field.isOptional
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
                                Assert.Field.hasNoChildren ] ] ] ] ] ]

    let Value = [|
        [| (* inputValue *) box <| Union.Case1;
            (* outputValue *) box {
                UnionRecord.Type = "Case1"; Case2 = Option.None } |]
        [| (* inputValue *) box <| Union.Case2 { Field3 = 1 };
            (* outputValue *) box {
                UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = { Field3 = 1 } } } |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``serialize multi case union with optional field`` =
    type Union = Case1 | Case2 of field2:int option
    type Case2Record = { field2: int option }
    type UnionRecord = { Type: string; Case2: Case2Record option }
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
                        Assert.Field.nameEquals "Type"
                        Assert.Field.isRequired
                        Assert.Field.Type.isByteArray
                        Assert.Field.LogicalType.isString
                        Assert.Field.ConvertedType.isUtf8
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "Case2"
                        Assert.Field.isOptional
                        Assert.Field.Type.hasNoValue
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.child [
                            Assert.Field.nameEquals "field2"
                            Assert.Field.isOptional
                            Assert.Field.Type.isInt32
                            Assert.Field.LogicalType.isInteger 32 true
                            Assert.Field.ConvertedType.isInt32
                            Assert.Field.hasNoChildren ] ] ] ] ]

    let Value = [|
        [| (* inputValue *) box <| Union.Case1;
            (* outputValue *) box {
                UnionRecord.Type = "Case1"; Case2 = Option.None } |]

        [| (* inputValue *) box <| Union.Case2 Option.None;
            (* outputValue *) box {
                UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = Option.None } } |]

        [| (* inputValue *) box <| Union.Case2 (Option.Some 1);
            (* outputValue *) box {
                UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = Option.Some 1 } } |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``serialize multi case union with multiple fields`` =
    type Union = Case1 | Case2 of field2:int * field3:bool | Case3 of field4:float
    type Case2Record = { field2: int; field3: bool }
    type Case3Record = { field4: float }
    type UnionRecord = { Type: string; Case2: Case2Record option; Case3: Case3Record option }
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
                        Assert.Field.nameEquals "Type"
                        Assert.Field.isRequired
                        Assert.Field.Type.isByteArray
                        Assert.Field.LogicalType.isString
                        Assert.Field.ConvertedType.isUtf8
                        Assert.Field.hasNoChildren ]
                    Assert.field [
                        Assert.Field.nameEquals "Case2"
                        Assert.Field.isOptional
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
                                Assert.Field.Type.isBool
                                Assert.Field.LogicalType.hasNoValue
                                Assert.Field.ConvertedType.hasNoValue
                                Assert.Field.hasNoChildren ] ] ]
                    Assert.field [
                        Assert.Field.nameEquals "Case3"
                        Assert.Field.isOptional
                        Assert.Field.Type.hasNoValue
                        Assert.Field.LogicalType.hasNoValue
                        Assert.Field.ConvertedType.hasNoValue
                        Assert.Field.child [
                            Assert.Field.nameEquals "field4"
                            Assert.Field.isRequired
                            Assert.Field.Type.isFloat64
                            Assert.Field.LogicalType.hasNoValue
                            Assert.Field.ConvertedType.hasNoValue
                            Assert.Field.hasNoChildren ] ] ] ] ]

    let Value = [|
        [| (* inputValue *) box <| Union.Case1;
            (* outputValue *) box<UnionRecord> {
                Type = "Case1"
                Case2 = Option.None
                Case3 = Option.None } |]

        [| (* inputValue *) box <| Union.Case2 (1, true);
            (* outputValue *) box<UnionRecord> {
                Type = "Case2"
                Case2 = Option.Some { field2 = 1; field3 = true }
                Case3 = Option.None } |]

        [| (* inputValue *) box <| Union.Case3 2.34;
            (* outputValue *) box<UnionRecord> {
                Type = "Case3"
                Case2 = Option.None
                Case3 = Option.Some { field4 = 2.34 } } |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let schema = ParquetFile.readSchema bytes
        assertSchemaMatchesExpected schema
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize multi case union with atomic field from required record`` =
    type Case2Record = { field2: int }
    type UnionRecord = { Type: string; Case2: Case2Record option }
    type Union = Case1 | Case2 of field2:int
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    let Value = [|
        [| (* inputValue *) box {
            UnionRecord.Type = "Case1"; Case2 = Option.None };
            (* outputValue *) box <| Union.Case1 |]

        [| (* inputValue *) box {
            UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = 1 } };
            (* outputValue *) box <| Union.Case2 1 |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize multi case union with atomic field from optional record`` =
    type Case2Record = { field2: int }
    type UnionRecord = { Type: string; Case2: Case2Record option }
    type Union = Case1 | Case2 of field2:int
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
        [| (* inputValue *) box {
            UnionRecord.Type = "Case1"; Case2 = Option.None };
            (* outputValue *) box <| Union.Case1 |]

        [| (* inputValue *) box {
            UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = 1 } };
            (* outputValue *) box <| Union.Case2 1 |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize multi case union with list field from required record`` =
    type Case2Record = { field2: int list }
    type UnionRecord = { Type: string; Case2: Case2Record option }
    type Union = Case1 | Case2 of field2:int list
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    let Value = [|
        [| (* inputValue *) box {
            UnionRecord.Type = "Case1"; Case2 = Option.None };
            (* outputValue *) box <| Union.Case1 |]

        [| (* inputValue *) box {
            UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = [] } };
            (* outputValue *) box <| Union.Case2 [] |]

        [| (* inputValue *) box {
            UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = [ 1 ] } };
            (* outputValue *) box <| Union.Case2 [ 1 ] |]

        [| (* inputValue *) box {
            UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = [ 1; 2; 3 ] } };
            (* outputValue *) box <| Union.Case2 [ 1; 2; 3 ] |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize multi case union with list field from optional record`` =
    type Case2Record = { field2: int list }
    type UnionRecord = { Type: string; Case2: Case2Record option }
    type Union = Case1 | Case2 of field2:int list
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
        [| (* inputValue *) box {
            UnionRecord.Type = "Case1"; Case2 = Option.None };
            (* outputValue *) box <| Union.Case1 |]

        [| (* inputValue *) box {
            UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = [] } };
            (* outputValue *) box <| Union.Case2 [] |]

        [| (* inputValue *) box {
            UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = [ 1 ] } };
            (* outputValue *) box <| Union.Case2 [ 1 ] |]

        [| (* inputValue *) box {
            UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = [ 1; 2; 3 ] } };
            (* outputValue *) box <| Union.Case2 [ 1; 2; 3 ] |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize multi case union with record field from required record`` =
    type Record = { Field3: int }
    type Case2Record = { field2: Record }
    type UnionRecord = { Type: string; Case2: Case2Record option }
    type Union = Case1 | Case2 of field2:Record
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    let Value = [|
        [| (* inputValue *) box {
            UnionRecord.Type = "Case1"; Case2 = Option.None };
            (* outputValue *) box <| Union.Case1 |]

        [| (* inputValue *) box {
            UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = { Field3 = 1 } } };
            (* outputValue *) box <| Union.Case2 { Field3 = 1 } |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize multi case union with record field from optional record`` =
    type Record = { Field3: int }
    type Case2Record = { field2: Record }
    type UnionRecord = { Type: string; Case2: Case2Record option }
    type Union = Case1 | Case2 of field2:Record
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
        [| (* inputValue *) box {
            UnionRecord.Type = "Case1"; Case2 = Option.None };
            (* outputValue *) box <| Union.Case1 |]

        [| (* inputValue *) box {
            UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = { Field3 = 1 } } };
            (* outputValue *) box <| Union.Case2 { Field3 = 1 } |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize multi case union with optional field from required record`` =
    type Case2Record = { field2: int option }
    type UnionRecord = { Type: string; Case2: Case2Record option }
    type Union = Case1 | Case2 of field2:int option
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    let Value = [|
        [| (* inputValue *) box {
            UnionRecord.Type = "Case1"; Case2 = Option.None };
            (* outputValue *) box <| Union.Case1 |]

        [| (* inputValue *) box {
            UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = Option.None } };
            (* outputValue *) box <| Union.Case2 Option.None |]

        [| (* inputValue *) box {
            UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = Option.Some 1 } };
            (* outputValue *) box <| Union.Case2 (Option.Some 1) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize multi case union with optional field from optional record`` =
    type Case2Record = { field2: int option }
    type UnionRecord = { Type: string; Case2: Case2Record option }
    type Union = Case1 | Case2 of field2:int option
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
        [| (* inputValue *) box {
            UnionRecord.Type = "Case1"; Case2 = Option.None };
            (* outputValue *) box <| Union.Case1 |]

        [| (* inputValue *) box {
            UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = Option.None } };
            (* outputValue *) box <| Union.Case2 Option.None |]

        [| (* inputValue *) box {
            UnionRecord.Type = "Case2"; Case2 = Option.Some { field2 = Option.Some 1 } };
            (* outputValue *) box <| Union.Case2 (Option.Some 1) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize multi case union with multiple fields from required record`` =
    type Case2Record = { field2: int; field3: bool }
    type Case3Record = { field4: float }
    type UnionRecord = { Type: string; Case2: Case2Record option; Case3: Case3Record option }
    type Union = Case1 | Case2 of field2:int * field3:bool | Case3 of field4:float
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    let Value = [|
        [| (* inputValue *) box<UnionRecord> {
                Type = "Case1"
                Case2 = Option.None
                Case3 = Option.None };
            (* outputValue *) box <| Union.Case1 |]

        [| (* inputValue *) box<UnionRecord> {
                Type = "Case2"
                Case2 = Option.Some { field2 = 1; field3 = true }
                Case3 = Option.None };
            (* outputValue *) box <| Union.Case2 (1, true) |]

        [| (* inputValue *) box<UnionRecord> {
                Type = "Case3"
                Case2 = Option.None
                Case3 = Option.Some { field4 = 2.34 } };
            (* outputValue *) box <| Union.Case3 2.34 |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize multi case union with multiple fields from optional record`` =
    type Case2Record = { field2: int; field3: bool }
    type Case3Record = { field4: float }
    type UnionRecord = { Type: string; Case2: Case2Record option; Case3: Case3Record option }
    type Union = Case1 | Case2 of field2:int * field3:bool | Case3 of field4:float
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
        [| (* inputValue *) box<UnionRecord> {
                Type = "Case1"
                Case2 = Option.None
                Case3 = Option.None };
            (* outputValue *) box <| Union.Case1 |]

        [| (* inputValue *) box<UnionRecord> {
                Type = "Case2"
                Case2 = Option.Some { field2 = 1; field3 = true }
                Case3 = Option.None };
            (* outputValue *) box <| Union.Case2 (1, true) |]

        [| (* inputValue *) box<UnionRecord> {
                Type = "Case3"
                Case2 = Option.None
                Case3 = Option.Some { field4 = 2.34 } };
            (* outputValue *) box <| Union.Case3 2.34 |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize multi case union with out-of-order fields from required record`` =
    type Case2Record = { field2: int; field3: bool; field4: float }
    type UnionRecord = { Type: string; Case2: Case2Record option }
    type Union = Case1 | Case2 of field3:bool * field4:float * field2:int
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    let Value = [|
        [| (* inputValue *) box<UnionRecord> {
                Type = "Case1"
                Case2 = Option.None };
            (* outputValue *) box <| Union.Case1 |]

        [| (* inputValue *) box<UnionRecord> {
                Type = "Case2"
                Case2 = Option.Some { field2 = 1; field3 = true; field4 = 2.34 } };
            (* outputValue *) box <| Union.Case2 (true, 2.34, 1) |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize multi case union with out-of-order fields from optional record`` =
    type Case2Record = { field2: int; field3: bool; field4: float }
    type UnionRecord = { Type: string; Case2: Case2Record option }
    type Union = Case1 | Case2 of field3:bool * field4:float * field2:int
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
        [| (* inputValue *) box<UnionRecord> {
                Type = "Case1"
                Case2 = Option.None };
            (* outputValue *) box <| Union.Case1 |]

        [| (* inputValue *) box<UnionRecord> {
                Type = "Case2"
                Case2 = Option.Some { field2 = 1; field3 = true; field4 = 2.34 } };
            (* outputValue *) box <| Union.Case2 (true, 2.34, 1) |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize multi case union with subset of fields from required record`` =
    type Case2Record = { field2: int; field3: bool }
    type Case3Record = { field4: float }
    type UnionRecord = { Type: string; Case2: Case2Record option; Case3: Case3Record option }
    type Union = Case1 | Case2 of field3:bool | Case3
    type Input = { Field1: UnionRecord }
    type Output = { Field1: Union }

    let Value = [|
        [| (* inputValue *) box<UnionRecord> {
                Type = "Case1"
                Case2 = Option.None
                Case3 = Option.None };
            (* outputValue *) box <| Union.Case1 |]

        [| (* inputValue *) box<UnionRecord> {
                Type = "Case2"
                Case2 = Option.Some { field2 = 1; field3 = true }
                Case3 = Option.None };
            (* outputValue *) box <| Union.Case2 true |]

        [| (* inputValue *) box<UnionRecord> {
                Type = "Case3"
                Case2 = Option.None
                Case3 = Option.Some { field4 = 2.34 } };
            (* outputValue *) box <| Union.Case3 |] |]

    [<Theory>]
    [<MemberData(nameof Value)>]
    let ``value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>

module ``deserialize multi case union with subset of fields from optional record`` =
    type Case2Record = { field2: int; field3: bool }
    type Case3Record = { field4: float }
    type UnionRecord = { Type: string; Case2: Case2Record option; Case3: Case3Record option }
    type Union = Case1 | Case2 of field3:bool | Case3
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
        [| (* inputValue *) box<UnionRecord> {
                Type = "Case1"
                Case2 = Option.None
                Case3 = Option.None };
            (* outputValue *) box <| Union.Case1 |]

        [| (* inputValue *) box<UnionRecord> {
                Type = "Case2"
                Case2 = Option.Some { field2 = 1; field3 = true }
                Case3 = Option.None };
            (* outputValue *) box <| Union.Case2 true |]

        [| (* inputValue *) box<UnionRecord> {
                Type = "Case3"
                Case2 = Option.None
                Case3 = Option.Some { field4 = 2.34 } };
            (* outputValue *) box <| Union.Case3 |] |]

    [<Theory>]
    [<MemberData(nameof NonNullValue)>]
    let ``non-null value`` inputValue outputValue =
        let inputRecords = [| { Input.Field1 = Option.Some inputValue } |]
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output>(bytes)
        test <@ outputRecords = [| { Output.Field1 = outputValue } |] @>
