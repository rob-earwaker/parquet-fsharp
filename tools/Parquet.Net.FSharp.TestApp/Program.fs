module Parquet.Net.FSharp.TestApp.Program

open Parquet.FSharp
open System
open System.IO

type Alternative =
    | OptionA of value1:int * value2:float
    | OptionB of string * bool
    | OptionC

type Gps = {
    Latitude: float
    Longitude: float }

type Data = {
    Value1: float
    Value2: Nullable<float>
    Value3: Nullable<int> }

type Message = {
    Id: Guid
(*    Time: DateTime
    Timestamp: DateTimeOffset*)
    Source: string
    Level: float
    Alternative: Alternative
    (*Flag: Nullable<bool>
    Count: int option
    Samples: int list
    Gps: Gps
    Values: Data[]
    Money: decimal*) }

module Random =
    let private Random = Random()

    let bool () =
        Random.NextDouble() >= 0.4

    let int () =
        Random.Next(0, 100)

    let float () =
        Random.NextDouble() * 10.

    let decimal () =
        decimal (Random.NextDouble() * 10. ** Random.Next(0, 16))

    let dateTimeOffset () =
        let ticks = Random.NextInt64(DateTimeOffset.MaxValue.Ticks)
        DateTimeOffset(ticks, TimeSpan.Zero)

    let dateTime () =
        let dateTimeOffset = dateTimeOffset ()
        dateTimeOffset.UtcDateTime

    let guid () =
        Guid.NewGuid()

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

    let nullableFloat () =
        if Random.NextDouble() >= 0.75
        then Nullable<float>()
        else Nullable<float>(float ())

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

    let list count (createItem: unit -> 'Item) =
        List.init count (fun _ -> createItem ())

    let alternative () =
        let nextDouble = Random.NextDouble()
        if nextDouble < 0.333
        then Alternative.OptionA (int (), float ())
        elif nextDouble < 0.666
        then Alternative.OptionB (string (), bool ())
        else Alternative.OptionC

    let gps () =
        { Gps.Latitude = float ()
          Gps.Longitude = float () }

    let data () =
        { Data.Value1 = float ()
          Data.Value2 = nullableFloat ()
          Data.Value3 = nullableInt () }

    let message () =
        { Message.Id = guid ()
    (*      Message.Time = dateTime ()
          Message.Timestamp = dateTimeOffset ()*)
          Message.Source = string ()
          Message.Level = float ()
          Message.Alternative = alternative ()
         (* Message.Flag = nullableBool ()
          Message.Count = intOption ()
          Message.Samples = list 5 int
          Message.Gps = gps ()
          Message.Values = array 3 data
          Message.Money = decimal ()*) }

[<EntryPoint>]
let main _ =
    let records = Array.init 10 (fun _ -> Random.message ())
    let filePath = @"..\..\..\..\..\data\data.parquet"
    // Write
    use writeStream = new MemoryStream()
    ParquetSerializer.Serialize(records, writeStream)
    File.WriteAllBytes(filePath, writeStream.ToArray())
    // Read
    let fileContent = File.ReadAllBytes(filePath)
    use readStream = new MemoryStream(fileContent)
    let roundtrippedRecords = ParquetSerializer.Deserialize<Message>(readStream)
    0
