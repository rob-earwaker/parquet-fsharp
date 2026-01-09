namespace Parquet.FSharp.Tests.Deserialize.DifferentSchema

open FsCheck.Xunit
open Parquet.FSharp
open Parquet.FSharp.Tests
open System

// TODO: If this file gets large, could split into PartialSchema and ExtendedSchema.

// TODO: Record with fields in different order
// TODO: Record with additional optional field?

module ``fsharp record with subset of fields`` =
    module Input =
        type Record1 = {
            Field1: int
            Field2: float
            Field3: string
            Field4: bool[] }

    module Output =
        type Record1 = {
            Field1: int
            Field4: bool[] }

    [<Property>]
    let ``test`` (inputRecords: Input.Record1[]) =
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output.Record1>(bytes)
        Assert.arrayLengthEqual inputRecords outputRecords
        for inputRecord, outputRecord in Array.zip inputRecords outputRecords do
            Assert.equal inputRecord.Field1 outputRecord.Field1
            Assert.equal inputRecord.Field4 outputRecord.Field4

module ``simple union with additional cases`` =
    module Input =
        type Record1 = {
            Field1: Union1 }

        and Union1 =
            | Case1
            | Case3

    module Output =
        type Record1 = {
            Field1: Union1 }

        and Union1 =
            | Case1
            | Case2
            | Case3
            | Case4

    [<Property>]
    let ``test`` (inputRecords: Input.Record1[]) =
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output.Record1>(bytes)
        Assert.arrayLengthEqual inputRecords outputRecords
        for inputRecord, outputRecord in Array.zip inputRecords outputRecords do
            match inputRecord.Field1, outputRecord.Field1 with
            | Input.Union1.Case1, Output.Union1.Case1 -> ()
            | _, Output.Union1.Case2 -> Assert.failWith "case does not exist in the input"
            | Input.Union1.Case3, Output.Union1.Case3 -> ()
            | _, Output.Union1.Case4 -> Assert.failWith "case does not exist in the input"
            | _ -> Assert.failWith "case names do not match"

module ``complex union with additional simple cases`` =
    module Input =
        type Record1 = {
            Field1: Union1 }

        and Union1 =
            | Case2 of field1:int * field2:bool
            | Case3 of field1:string

    module Output =
        type Record1 = {
            Field1: Union1 }

        and Union1 =
            | Case1
            | Case2 of field1:int * field2:bool
            | Case3 of field1:string
            | Case4

    [<Property>]
    let ``test`` (inputRecords: Input.Record1[]) =
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output.Record1>(bytes)
        Assert.arrayLengthEqual inputRecords outputRecords
        for inputRecord, outputRecord in Array.zip inputRecords outputRecords do
            match inputRecord.Field1, outputRecord.Field1 with
            | _, Output.Union1.Case1 -> Assert.failWith "case does not exist in the input"
            | Input.Union1.Case2 (inputField1, inputField2),
                Output.Union1.Case2 (outputField1, outputField2) ->
                Assert.equal inputField1 outputField1
                Assert.equal inputField2 outputField2
            | Input.Union1.Case3 inputField1,
                Output.Union1.Case3 outputField1 ->
                Assert.equal inputField1 outputField1
            | _, Output.Union1.Case4 -> Assert.failWith "case does not exist in the input"
            | _ -> Assert.failWith "case names do not match"

module ``complex union with additional complex cases`` =
    module Input =
        type Record1 = {
            Field1: Union1 }

        and Union1 =
            | Case2 of field1:int * field2:bool
            | Case3 of field1:string

    module Output =
        type Record1 = {
            Field1: Union1 }

        and Union1 =
            | Case1 of field1:DateTimeOffset * field2:float
            | Case2 of field1:int * field2:bool
            | Case3 of field1:string
            | Case4 of field1:decimal[]

    [<Property(Skip = "this is not yet supported")>]
    let ``test`` (inputRecords: Input.Record1[]) =
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output.Record1>(bytes)
        Assert.arrayLengthEqual inputRecords outputRecords
        for inputRecord, outputRecord in Array.zip inputRecords outputRecords do
            match inputRecord.Field1, outputRecord.Field1 with
            | _, Output.Union1.Case1 _ -> Assert.failWith "case does not exist in the input"
            | Input.Union1.Case2 (inputField1, inputField2),
                Output.Union1.Case2 (outputField1, outputField2) ->
                Assert.equal inputField1 outputField1
                Assert.equal inputField2 outputField2
            | Input.Union1.Case3 inputField1,
                Output.Union1.Case3 outputField1 ->
                Assert.equal inputField1 outputField1
            | _, Output.Union1.Case4 _ -> Assert.failWith "case does not exist in the input"
            | _ -> Assert.failWith "case names do not match"

module ``complex union cases without fields`` =
    module Input =
        type Record1 = {
            Field1: Union1 }

        and Union1 =
            | Case1
            | Case2 of field1:int * field2:bool
            | Case3 of field1:Record2
            | Case4 of field1:decimal

        and Record2 = {
            Field1: float
            Field2: decimal }

    module Output =
        type Record1 = {
            Field1: Union1 }

        and Union1 =
            | Case1
            | Case2
            | Case3
            | Case4 of field1:decimal

    [<Property>]
    let ``test`` (inputRecords: Input.Record1[]) =
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output.Record1>(bytes)
        Assert.arrayLengthEqual inputRecords outputRecords
        for inputRecord, outputRecord in Array.zip inputRecords outputRecords do
            match inputRecord.Field1, outputRecord.Field1 with
            | Input.Union1.Case1, Output.Union1.Case1 -> ()
            | Input.Union1.Case2 _, Output.Union1.Case2 -> ()
            | Input.Union1.Case3 _, Output.Union1.Case3 -> ()
            | Input.Union1.Case4 inputField1,
                Output.Union1.Case4 outputField1 ->
                Assert.equal inputField1 outputField1
            | _ -> Assert.failWith "case names do not match"

module ``complex union cases with subset of fields`` =
    module Input =
        type Record1 = {
            Field1: Union1 }

        and Union1 =
            | Case1
            | Case2 of field1:int * field2:bool * field3:string
            | Case3 of field1:DateTimeOffset * field2:DateTimeOffset

    module Output =
        type Record1 = {
            Field1: Union1 }

        and Union1 =
            | Case1
            | Case2 of field1:int * field3:string
            | Case3 of field2:DateTimeOffset

    [<Property>]
    let ``test`` (inputRecords: Input.Record1[]) =
        let bytes = ParquetSerializer.Serialize(inputRecords)
        let outputRecords = ParquetSerializer.Deserialize<Output.Record1>(bytes)
        Assert.arrayLengthEqual inputRecords outputRecords
        for inputRecord, outputRecord in Array.zip inputRecords outputRecords do
            match inputRecord.Field1, outputRecord.Field1 with
            | Input.Union1.Case1, Output.Union1.Case1 -> ()
            | Input.Union1.Case2 (inputField1, _, inputField3),
                Output.Union1.Case2 (outputField1, outputField3) ->
                Assert.equal inputField1 outputField1
                Assert.equal inputField3 outputField3
            | Input.Union1.Case3 (_, inputField2),
                Output.Union1.Case3 outputField2 ->
                Assert.equal inputField2 outputField2
            | _ -> Assert.failWith "case names do not match"
