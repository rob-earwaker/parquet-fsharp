# Parquet.FSharp

[![NuGet Version](https://img.shields.io/nuget/v/Parquet.FSharp?style=flat-square&label=NuGet&logo=nuget)](https://www.nuget.org/packages/Parquet.FSharp)
[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/rob-earwaker/parquet-fsharp/build.yml?style=flat-square&label=Build&logo=github)](https://github.com/rob-earwaker/parquet-fsharp/actions/workflows/build.yml?query=branch%3Amain)

An F# serailization library for the [Apache Parquet](https://parquet.apache.org/) file format, built on top of the fantastic [Parquet.Net](https://github.com/aloneguid/parquet-dotnet) library. **Parquet.FSharp** adds first-class support for F# types such as records, options, lists and discriminated unions, whilst maintaining the performance of **Parquet.Net**.

```fsharp
open Parquet.FSharp
open System.IO

type Shape =
    | Circle of radius:int
    | Square of sideLength:int
    | Rectangle of height:int * width:int

type Node = {
    Id: int
    Shape: Shape
    Scale: float option
    Children: int list }

let nodes = [|
    { Id = 0; Shape = Square 1        ; Scale = None    ; Children = [ 1; 2 ] }
    { Id = 1; Shape = Circle 2        ; Scale = Some 1.5; Children = [ 4 ]    }
    { Id = 2; Shape = Square 3        ; Scale = Some 0.5; Children = [ 3 ]    }
    { Id = 3; Shape = Rectangle (1, 2); Scale = None    ; Children = [ 4 ]    }
    { Id = 4; Shape = Circle 1        ; Scale = Some 2.0; Children = []       } |]

// Serialize to file
use file = File.OpenWrite("./nodes.parquet")
ParquetSerializer.Serialize(nodes, file)

// Deserialize from file
use file = File.OpenRead("./nodes.parquet")
let nodes = ParquetSerializer.Deserialize<Node>(file)
```
