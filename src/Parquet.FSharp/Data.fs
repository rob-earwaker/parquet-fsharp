namespace Parquet.FSharp

type ColumnValues =
    | Bool of bool[]
    | Int32 of int[]
    | Int64 of int64[]
    | Float32 of float32[]
    | Float64 of float[]
    | ByteArray of byte[][]
    | FixedLengthByteArray of byte[][]

type Column = {
    ValueCount: int
    Values: ColumnValues
    MaxRepetitionLevel: int
    MaxDefinitionLevel: int
    RepetitionLevels: int[] option
    DefinitionLevels: int[] option }
