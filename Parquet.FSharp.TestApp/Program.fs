module Parquet.FSharp.TestApp.Program

open Parquet.FSharp
open System
open System.IO

type Field5 = {
    Field51: int
    Field52: Nullable<int>
    Field53: int option }

type Field8 = {
    Field81: int
    Field82: int[]
    Field83: int option[] }

type Record1 = {
    (*Field01: bool
    Field02: int
    Field03: Nullable<int>
    Field04: int option
    Field05: Field5*)
    Field06: int[]
    (*Field07: int option[]
    Field08: Field8
    Field09: Field8[]
    Field10: Field8 option[]*) }

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

    let boolOption () =
        if Random.NextDouble() >= 0.75
        then Option.None
        else Option.Some (bool ())

    let intOption () =
        if Random.NextDouble() >= 0.75
        then Option.None
        else Option.Some (int ())

    let array count (createItem: unit -> 'Item) =
        Array.init count (fun _ -> createItem ())

    let arrayOption count (createItem: unit -> 'Item) =
        if Random.NextDouble() >= 0.75
        then Option.None
        else option.Some (array count createItem)

    let field5 () =
        { Field5.Field51 = int ()
          Field5.Field52 = nullableInt ()
          Field5.Field53 = intOption () }

    let field8 () =
        { Field8.Field81 = int ()
          Field8.Field82 = array 3 int
          Field8.Field83 = array 3 intOption }

    let field8Option () =
        if Random.NextDouble() >= 0.75
        then Option.None
        else Option.Some (field8 ())

    let record1 () =
        { (*Record1.Field01 = bool ()
          Record1.Field02 = int ()
          Record1.Field03 = nullableInt ()
          Record1.Field04 = intOption ()
          Record1.Field05 = field5 ()*)
          Record1.Field06 = array 5 int
          (*Record1.Field07 = array 5 intOption
          Record1.Field08 = field8 ()
          Record1.Field09 = array 5 field8
          Record1.Field10 = array 5 field8Option*) }

[<EntryPoint>]
let main _ =
    let records = Array.init 20 (fun _ -> Random.record1 ())
    use stream = new MemoryStream()
    use fileWriter = new ParquetStreamWriter<Record1>(stream)
    fileWriter.WriteHeader()
    fileWriter.WriteRowGroup(records)
    fileWriter.WriteFooter()
    let filePath = @"..\..\..\..\data\data.parquet"
    File.WriteAllBytes(filePath, stream.ToArray())
    0
