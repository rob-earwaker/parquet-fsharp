namespace Parquet.FSharp

open System

type Column = {
    FieldName: string
    Values: Array
    RepetitionLevels: int[] option
    DefinitionLevels: int[] option }
