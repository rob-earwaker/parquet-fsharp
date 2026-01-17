module Parquet.FSharp.Tests.Serialize.Overloads

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
let ``serialize to byte array`` () =
    let serializedBytes = ParquetSerializer.Serialize(Records)
    Assert.equal Bytes serializedBytes

[<Fact>]
let ``serialize to memory stream`` () =
    use stream = new MemoryStream()
    ParquetSerializer.Serialize(Records, stream)
    let serializedBytes = stream.ToArray()
    Assert.equal Bytes serializedBytes

[<Fact>]
let ``serialize to file write stream`` () =
    let filePath = Path.GetTempFileName()
    use stream = File.OpenWrite(filePath)
    ParquetSerializer.Serialize(Records, stream)
    stream.Close()
    let serializedBytes = File.ReadAllBytes(filePath)
    Assert.equal Bytes serializedBytes

[<Fact>]
let ``async serialize to byte array`` () =
    let serializedBytes =
        ParquetSerializer.AsyncSerialize(Records)
        |> Async.RunSynchronously
    Assert.equal Bytes serializedBytes

[<Fact>]
let ``async serialize to memory stream`` () =
    use stream = new MemoryStream()
    ParquetSerializer.AsyncSerialize(Records, stream)
    |> Async.RunSynchronously
    let serializedBytes = stream.ToArray()
    Assert.equal Bytes serializedBytes

[<Fact>]
let ``async serialize to file write stream`` () =
    let filePath = Path.GetTempFileName()
    use stream = File.OpenWrite(filePath)
    ParquetSerializer.AsyncSerialize(Records, stream)
    |> Async.RunSynchronously
    stream.Close()
    let serializedBytes = File.ReadAllBytes(filePath)
    Assert.equal Bytes serializedBytes

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
