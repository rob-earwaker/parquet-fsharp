namespace Parquet.FSharp

open System

type Column = {
    ValueCount: int
    Values: Array
    MaxRepetitionLevel: int
    MaxDefinitionLevel: int
    RepetitionLevels: int[] option
    DefinitionLevels: int[] option }
