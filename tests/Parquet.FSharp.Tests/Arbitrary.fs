namespace Parquet.FSharp.Tests

open FsCheck.FSharp
open System

type UtcDateTime =
    static member Arbitrary() =
        ArbMap.defaults
        |> ArbMap.arbitrary<DateTime>
        |> Arb.filter (fun dateTime ->
            dateTime.Kind = DateTimeKind.Utc)
