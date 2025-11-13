namespace Parquet.Net.FSharp

open Parquet.Serialization

[<AutoOpen>]
module Extensions =
    type ParquetSerializer with
        static member Hello() = "hello"
