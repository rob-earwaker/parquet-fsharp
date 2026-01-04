module Parquet.FSharp.Tests.Deserialize.Overloads

open Parquet.FSharp
open Parquet.FSharp.Tests
open System.IO
open Xunit

type Record = { Field1: int }

let Records = [|
    { Record.Field1 = 0 }
    { Record.Field1 = 1 }
    { Record.Field1 = 2 }
    { Record.Field1 = 3 } |]

let Bytes = ParquetSerializer.Serialize(Records)

[<Fact>]
let ``deserialize from byte array`` () =
    let deserializedRecords = ParquetSerializer.Deserialize<Record>(Bytes)
    Assert.equal Records deserializedRecords

[<Fact>]
let ``deserialize from memory stream`` () =
    let stream = new MemoryStream(Bytes)
    let deserializedRecords = ParquetSerializer.Deserialize<Record>(stream)
    Assert.equal Records deserializedRecords

[<Fact>]
let ``deserialize from file read stream`` () =
    let filePath = Path.GetTempFileName()
    File.WriteAllBytes(filePath, Bytes)
    let stream = File.OpenRead(filePath)
    let deserializedRecords = ParquetSerializer.Deserialize<Record>(stream)
    Assert.equal Records deserializedRecords

[<Fact>]
let ``async deserialize from byte array`` () =
    let deserializedRecords =
        ParquetSerializer.AsyncDeserialize<Record>(Bytes)
        |> Async.RunSynchronously
    Assert.equal Records deserializedRecords

[<Fact>]
let ``async deserialize from memory stream`` () =
    let stream = new MemoryStream(Bytes)
    let deserializedRecords =
        ParquetSerializer.AsyncDeserialize<Record>(stream)
        |> Async.RunSynchronously
    Assert.equal Records deserializedRecords

[<Fact>]
let ``async deserialize from file read stream`` () =
    let filePath = Path.GetTempFileName()
    File.WriteAllBytes(filePath, Bytes)
    let stream = File.OpenRead(filePath)
    let deserializedRecords =
        ParquetSerializer.AsyncDeserialize<Record>(stream)
        |> Async.RunSynchronously
    Assert.equal Records deserializedRecords
