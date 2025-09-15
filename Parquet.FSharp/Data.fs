namespace Parquet.FSharp

type ColumnValues =
    | Bool of bool[]
    | Int32 of int[]

type Column = {
    ValueCount: int
    Values: ColumnValues
    MaxRepetitionLevel: int
    MaxDefinitionLevel: int
    RepetitionLevels: int[] option
    DefinitionLevels: int[] option }
