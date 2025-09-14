namespace Parquet.FSharp

type ColumnValues =
    | Bool of bool[]
    | Int32 of int[]

type Column = {
    FieldInfo: FieldInfo
    ValueCount: int
    Values: ColumnValues
    RepetitionLevels: int[] option
    DefinitionLevels: int[] option }
