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

module ParquetFSharp =
    let serialize (records: 'Record seq) =
        use stream = new MemoryStream()
        Parquet.FSharp.ParquetSerializer.Serialize(records, stream)

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

[<CPUUsageDiagnoser>]
//[<DotNetObjectAllocDiagnoser>]
//[<DotNetObjectAllocJobConfiguration>]
[<MemoryDiagnoser>]
type ParquetSerialization() =
    let rowCount = 1_000_000
    let records =
        Array.init rowCount (fun _ ->
            {| Field1 = Random.int () |})

    //[<Benchmark>]
    //member this.ParquetNet() =
    //    ParquetNet.serialize records

    [<Benchmark>]
    member this.ParquetFSharp() =
        ParquetFSharp.serialize records

[<EntryPoint>]
let main _ =
    let summary = BenchmarkRunner.Run<ParquetSerialization>()
    0
