# Parquet.FSharp

[![NuGet Version](https://img.shields.io/nuget/v/Parquet.FSharp?style=flat-square&label=NuGet&logo=nuget)](https://www.nuget.org/packages/Parquet.FSharp)
[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/rob-earwaker/parquet-fsharp/build.yml?style=flat-square&label=Build&logo=github)](https://github.com/rob-earwaker/parquet-fsharp/actions/workflows/build.yml?query=branch%3Amain)

An F# serailization library for the [Apache Parquet](https://parquet.apache.org/) file format, built on top of the fantastic [Parquet.Net](https://github.com/aloneguid/parquet-dotnet) library. **Parquet.FSharp** adds first-class support for F# types such as records, options, lists and discriminated unions, whilst maintaining the performance of **Parquet.Net**.

```fsharp
open Parquet.FSharp
open System.IO

type Record = {
    Id: string
    Source: int option
    Samples: float list }

let records = [
    { Id = "0aacf928"; Source = Some 12; Samples = [ 2.61 ]       }
    { Id = "2e1674b1"; Source = None   ; Samples = [ 5.28; 2.83 ] }
    { Id = "cec3218d"; Source = Some 37; Samples = [ 1.61; 6.81 ] }
    { Id = "6d393804"; Source = Some 12; Samples = []             }
    { Id = "5a06d6f8"; Source = None   ; Samples = [ 3.32 ]       } ]

// Serialize to file
use file = File.OpenWrite("./data.parquet")
ParquetSerializer.Serialize(records, file)

// Deserialize from file
use file = File.OpenRead("./data.parquet")
let records = ParquetSerializer.Deserialize<Record>(file)
```
