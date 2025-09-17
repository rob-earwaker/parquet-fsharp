module Parquet.FSharp.TestApp.Program

open Parquet.FSharp
open System
open System.IO

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

    let dateTimeOffset () =
        let ticks = Random.NextInt64(DateTimeOffset.MaxValue.Ticks)
        DateTimeOffset(ticks, TimeSpan.Zero)

    let string () =
        if Random.NextDouble() >= 0.75
        then null
        else
            let alphabet = "abcdefghijklmnopqrstuvxxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
            Array.init 8 (fun _ -> alphabet[Random.Next(0, alphabet.Length)])
            |> String

    let nullableBool () =
        if Random.NextDouble() >= 0.75
        then Nullable<bool>()
        else Nullable<bool>(bool ())

    let nullableInt () =
        if Random.NextDouble() >= 0.75
        then Nullable<int>()
        else Nullable<int>(int ())

    let boolOption () =
        if Random.NextDouble() >= 0.75
        then Option.None
        else Option.Some (bool ())

    let intOption () =
        if Random.NextDouble() >= 0.75
        then Option.None
        else Option.Some (int ())

    let array count (createItem: unit -> 'Item) =
        if Random.NextDouble() >= 0.75
        then null
        else Array.init count (fun _ -> createItem ())

    let gps () =
        { Gps.Latitude = float ()
          Gps.Longitude = float () }

    let data () =
        { Data.Value1 = float ()
          Data.Value2 = float ()
          Data.Value3 = float () }

    let message () =
        { Message.Time = dateTimeOffset ()
          Message.Level = float ()
          Message.Count = int ()
          Message.Samples = array 5 int
          Message.Gps = gps ()
          Message.Values = array 3 data }

[<EntryPoint>]
let main _ =
    let records = Array.init 20 (fun _ -> Random.message ())
    use stream = new MemoryStream()
    use fileWriter = new ParquetStreamWriter<Message>(stream)
    fileWriter.WriteHeader()
    fileWriter.WriteRowGroup(records)
    fileWriter.WriteFooter()
    let filePath = @"..\..\..\..\data\data.parquet"
    File.WriteAllBytes(filePath, stream.ToArray())
    0
