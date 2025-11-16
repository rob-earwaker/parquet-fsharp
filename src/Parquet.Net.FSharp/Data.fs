namespace Parquet.FSharp

open Parquet.Schema
open System

// TODO: Can we replace this with Parquet.Net's DataColumn?
type Column = {
    Field: DataField
    ValueCount: int
    // TODO: Rename to Data(Values)?
    Values: Array
    // TODO: Are these even used in record assembly or can we remove?
    MaxRepetitionLevel: int
    MaxDefinitionLevel: int
    RepetitionLevels: int[] option
    DefinitionLevels: int[] option }
