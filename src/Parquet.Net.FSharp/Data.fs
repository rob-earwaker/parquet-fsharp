namespace Parquet.FSharp

open System

type Column = {
    // TODO: Can probably get rid of the count.
    ValueCount: int
    // TODO: Rename to Data(Values)?
    Values: Array
    // TODO: Are these even used in record assembly or can we remove?
    MaxRepetitionLevel: int
    MaxDefinitionLevel: int
    RepetitionLevels: int[] option
    DefinitionLevels: int[] option }
