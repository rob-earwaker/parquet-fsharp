module Parquet.FSharp.TestApp.Program

open Parquet.FSharp
open System

type Record1 = {
    Field1: bool
    Field2: int
    Field3: float
    Field4: string }

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

    let record1 () =
        { Record1.Field1 = bool ()
          Record1.Field2 = int ()
          Record1.Field3 = float ()
          Record1.Field4 = string () }

type Column = {
    Schema: ColumnSchema
    Values: Array
    RepetitionLevels: int[] option
    DefinitionLevels: int[] option }

and ColumnSchema = {
    DotnetType: Type }

let generateColumns (records: 'Record[]) =
    let recordType = Schema.ofRecord<'Record>
    Seq.empty<Column>

[<EntryPoint>]
let main _ =
    let schema = Schema.ofRecord<Message>
    let fileMetaData = Thrift.FileMetaData.ofSchema schema
    let records = Array.init 10 (fun _ -> Random.record1 ())
    let columns = generateColumns records
    0
