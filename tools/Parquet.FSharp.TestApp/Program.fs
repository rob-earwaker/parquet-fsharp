module Parquet.FSharp.TestApp.Program

open Parquet.FSharp
open System
open System.IO

type Alternative =
    | OptionA of value1:int * value2:float
    | OptionB of string * bool
    | OptionC

type FileType =
    | Unknown = 0
    | Jpeg = 1
    | Bmp = 2
    | Png = 3

type Gps = {
    Latitude: float
    Longitude: float }

type Data = {
    Value1: float
    Value2: Nullable<float>
    Value3: Nullable<int> }

open Parquet.Serialization.Attributes

[<CLIMutable>]
type Message = {
    (*Id: Guid*)
    [<ParquetTimestampAttribute(ParquetTimestampResolution.Milliseconds, true, false)>]
    Time: DateTime
    (*Timestamp: DateTimeOffset
    Source: string
    Level: float
    Alternative: Alternative
    FileType: FileType
    Flag: Nullable<bool>
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

    let fileType () =
        let nextDouble = Random.NextDouble()
        if nextDouble < 0.25
        then FileType.Unknown
        elif nextDouble < 0.50
        then FileType.Jpeg
        elif nextDouble < 0.75
        then FileType.Bmp
        else FileType.Png

    let gps () =
        { Gps.Latitude = float ()
          Gps.Longitude = float () }

    let data () =
        { Data.Value1 = float ()
          Data.Value2 = nullableFloat ()
          Data.Value3 = nullableInt () }

    let message () =
        { (*Message.Id = guid ()*)
          Message.Time = dateTime ()
          (*Message.Timestamp = dateTimeOffset ()
          Message.Source = string ()
          Message.Level = float ()
          Message.Alternative = alternative ()
          Message.FileType = fileType ()
          Message.Flag = nullableBool ()
          Message.Count = intOption ()
          Message.Samples = list 5 int
          Message.Gps = gps ()
          Message.Values = array 3 data
          Message.Money = decimal ()*) }

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

[<EntryPoint>]
let main _ =
    let records = [|
        { Message.Time = DateTime(                  0L, DateTimeKind.Unspecified) }
        { Message.Time = DateTime( 621355968000000000L, DateTimeKind.Unspecified) }
        { Message.Time = DateTime( 638752524170000000L, DateTimeKind.Unspecified) }
        { Message.Time = DateTime(3155378975999999999L, DateTimeKind.Unspecified) }
        { Message.Time = DateTime(                  0L, DateTimeKind.Utc) }
        { Message.Time = DateTime( 621355968000000000L, DateTimeKind.Utc) }
        { Message.Time = DateTime( 638752524170000000L, DateTimeKind.Utc) }
        { Message.Time = DateTime(3155378975999999999L, DateTimeKind.Utc) }
        { Message.Time = DateTime(                  0L, DateTimeKind.Local) }
        { Message.Time = DateTime( 621355968000000000L, DateTimeKind.Local) }
        { Message.Time = DateTime( 638752524170000000L, DateTimeKind.Local) }
        { Message.Time = DateTime(3155378975999999999L, DateTimeKind.Local) }
    |]

    let filePath = @"..\..\..\..\..\data\data.parquet"
    // Write
    let bytes = ParquetNet.serialize records
    File.WriteAllBytes(filePath, bytes)
    // Read
    let bytes = File.ReadAllBytes(filePath)
    let roundtrippedRecords = ParquetNet.deserialize<Message> bytes
    0

