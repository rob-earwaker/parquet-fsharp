module Parquet.FSharp.Benchmarks.Program

open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running
open Microsoft.VSDiagnostics
open System
open System.IO

module ParquetNet =
    let serialize (records: 'Record seq) =
        use stream = new MemoryStream()
        Parquet.Serialization.ParquetSerializer.SerializeAsync(records, stream)
        |> Async.AwaitTask
        |> Async.Ignore
        |> Async.RunSynchronously
        stream.ToArray()

    let deserialize<'Record when 'Record : (new: unit -> 'Record)> (bytes: byte[]) =
        use stream = new MemoryStream(bytes)
        Parquet.Serialization.ParquetSerializer.DeserializeAsync<'Record>(stream)
        |> Async.AwaitTask
        |> Async.RunSynchronously

module ParquetFSharp =
    let serialize (records: 'Record seq) =
        use stream = new MemoryStream()
        Parquet.FSharp.ParquetSerializer.Serialize(records, stream)
        stream.ToArray()

    let deserialize<'Record> (bytes: byte[]) =
        use stream = new MemoryStream(bytes)
        Parquet.FSharp.ParquetSerializer.Deserialize<'Record>(stream)

module Random =
    let private Random = Random()

    let bytes count =
        let bytes = Array.zeroCreate<byte> count
        Random.NextBytes(bytes)
        bytes

    let bool () =
        Random.NextDouble() < 0.5

    let int () =
        BitConverter.ToInt32(bytes 4)

    let float () =
        BitConverter.ToDouble(bytes 8)

    let decimal () =
        decimal (Random.NextDouble() * 10. ** Random.Next(0, 16))

    let guid () =
        Guid.NewGuid()

    let array count (createItem: unit -> 'Item) =
        Array.init count (fun _ -> createItem ())

    let genericList count (createItem: unit -> 'Item) =
        ResizeArray(Array.init count (fun _ -> createItem ()))

[<CLIMutable>]
type Inner = {
    Field1: bool
    Field2: int }

[<CLIMutable>]
type Record = {
    Field1: bool
    Field2: int
    Field3: float
    Field4: decimal
    Field5: Guid
    Field6: ResizeArray<int>
    Field7: ResizeArray<Inner> }

let RowCount = 100_000

let Records =
    Array.init RowCount (fun _ ->
        { Record.Field1 = Random.bool ()
          Record.Field2 = Random.int ()
          Record.Field3 = Random.float ()
          Record.Field4 = Random.decimal ()
          Record.Field5 = Random.guid ()
          Record.Field6 = Random.genericList 100 Random.int
          Record.Field7 = Random.genericList 10 (fun () ->
            { Inner.Field1 = Random.bool ()
              Inner.Field2 = Random.int () }) })

let Bytes = ParquetNet.serialize Records

[<CPUUsageDiagnoser>]
[<MemoryDiagnoser>]
type Benchmarks() =
    [<Benchmark>]
    member this.Serialize_ParquetNet() =
        ParquetNet.serialize Records

    [<Benchmark>]
    member this.Serialize_ParquetFSharp() =
        ParquetFSharp.serialize Records

    [<Benchmark>]
    member this.Deserialize_ParquetNet() =
        ParquetNet.deserialize<Record> Bytes

    [<Benchmark>]
    member this.Deserialize_ParquetFSharp() =
        ParquetFSharp.deserialize<Record> Bytes

[<EntryPoint>]
let main _ =
    let summary = BenchmarkRunner.Run<Benchmarks>()
    0
