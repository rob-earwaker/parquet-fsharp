namespace Parquet.FSharp

open System

type Column = {
    Values: Array
    RepetitionLevels: int[] option
    DefinitionLevels: int[] option }
