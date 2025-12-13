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

    let array count (createItem: unit -> 'Item) =
        Array.init count (fun _ -> createItem ())

    let genericList count (createItem: unit -> 'Item) =
        ResizeArray(Array.init count (fun _ -> createItem ()))

[<CPUUsageDiagnoser>]
//[<DotNetObjectAllocDiagnoser>]
//[<DotNetObjectAllocJobConfiguration>]
[<MemoryDiagnoser>]
type ParquetSerialization() =
    let rowCount = 1_000
    let records =
        Array.init rowCount (fun _ ->
            {| Field1 = Random.bool ()
               Field2 = Random.int ()
               Field3 = Random.float ()
               Field4 = Random.array 100 (fun () ->
                {| Field41 = Random.bool ()
                   Field42 = Random.int ()
                   Field43 = Random.float () |})|})

    [<Benchmark>]
    member this.ParquetNet() =
        ParquetNet.serialize records

    [<Benchmark>]
    member this.ParquetFSharp() =
        ParquetFSharp.serialize records

[<CLIMutable>]
type Record = {
    Field1: bool
    Field2: int
    Field3: ResizeArray<float> }

[<CPUUsageDiagnoser>]
//[<DotNetObjectAllocDiagnoser>]
//[<DotNetObjectAllocJobConfiguration>]
[<MemoryDiagnoser>]
type ParquetDeserialization() =
    let rowCount = 100_000
    let records =
        Array.init rowCount (fun _ ->
            { Record.Field1 = Random.bool ()
              Record.Field2 = Random.int ()
              Record.Field3 = Random.genericList 100 Random.float })
    let bytes = ParquetNet.serialize records

    [<Benchmark>]
    member this.ParquetNet() =
        ParquetNet.deserialize<Record> bytes

    [<Benchmark>]
    member this.ParquetFSharp() =
        ParquetFSharp.deserialize<Record> bytes

[<EntryPoint>]
let main _ =
    let summary = BenchmarkRunner.Run<ParquetDeserialization>()
    0
