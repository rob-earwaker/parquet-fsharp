namespace Parquet.FSharp.Tests.Deserialize

open FsCheck.Xunit
open Parquet.FSharp
open Parquet.FSharp.Tests

module ``record with subset of fields`` =
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
