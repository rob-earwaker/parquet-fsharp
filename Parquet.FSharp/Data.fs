namespace Parquet.FSharp

open System

type Column = {
    FieldInfo: FieldInfo
    Values: Array
    RepetitionLevels: int[] option
    DefinitionLevels: int[] option }
