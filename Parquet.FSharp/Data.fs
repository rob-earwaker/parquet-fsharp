namespace Parquet.FSharp

open System

type Column = {
    FieldInfo: FieldInfo
    ValueCount: int
    Values: Array
    RepetitionLevels: int[] option
    DefinitionLevels: int[] option }
