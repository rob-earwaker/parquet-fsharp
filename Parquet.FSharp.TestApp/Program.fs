module Parquet.FSharp.TestApp.Program

open Parquet.FSharp
open System

type Record1 = {
    Field1: bool
    Field2: int
    Field3: float
    Field4: string }

module Random =
    let private Random = Random()

    let bool () =
        Random.NextDouble() >= 0.5

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
    let recordType = RecordType.of'<'Record>
    Seq.empty<Column>

[<EntryPoint>]
let main _ =
    let records = Array.init 10 (fun _ -> Random.record1 ())
    let columns = generateColumns records
    0
