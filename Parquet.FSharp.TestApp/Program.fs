module Parquet.FSharp.TestApp.Program

open Parquet.FSharp
open System
open System.IO

type Record1 = {
    Field1: bool
    Field2: int
    Field3: float
    Field4: string }

type Record2 = {
    Field1: bool
    Field2: int
    Field3: Nullable<bool>
    Field4: Nullable<int> }

type Gps = {
    Latitude: float
    Longitude: float }

type Data = {
    Value1: float
    Value2: float
    Value3: float }

type Message = {
    Time: DateTimeOffset
    Level: float
    Count: int
    Samples: int[]
    Gps: Gps
    Values: Data[] }

module Random =
    let private Random = Random()

    let bool () =
        Random.NextDouble() >= 0.4

    let int () =
        Random.Next(0, 100)

    let float () =
        Random.NextDouble() * 10.

    let string () =
        if Random.NextDouble() >= 0.75
        then null
        else Random.NextInt64().ToString("X8")

    let nullableBool () =
        if Random.NextDouble() >= 0.75
        then Nullable<bool>()
        else Nullable<bool>(bool ())

    let nullableInt () =
        if Random.NextDouble() >= 0.75
        then Nullable<int>()
        else Nullable<int>(int ())

    let record1 () =
        { Record1.Field1 = bool ()
          Record1.Field2 = int ()
          Record1.Field3 = float ()
          Record1.Field4 = string () }

    let record2 () =
        { Record2.Field1 = bool ()
          Record2.Field2 = int ()
          Record2.Field3 = nullableBool ()
          Record2.Field4 = nullableInt () }

[<EntryPoint>]
let main _ =
    let records = Array.init 10 (fun _ -> Random.record2 ())
    use stream = new MemoryStream()
    use fileWriter = new ParquetStreamWriter<Record2>(stream)
    fileWriter.WriteHeader()
    fileWriter.WriteRowGroup(records)
    fileWriter.WriteFooter()
    let filePath = @"..\..\..\..\data\data.parquet"
    File.WriteAllBytes(filePath, stream.ToArray())
    0
