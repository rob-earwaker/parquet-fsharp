module Parquet.Net.FSharp.TestApp.Program

open Parquet.Serialization
open Parquet.Net.FSharp

[<EntryPoint>]
let main _ =
    let message = ParquetSerializer.Hello()
    0
