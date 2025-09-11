module Parquet.FSharp.Dremel

open System
open System.Collections.Generic

let shred (records: 'Record[]) =
    let recordTypeInfo = RecordTypeInfo.ofRecord<'Record>
    let values = Dictionary()
    for fieldTypeInfo in recordTypeInfo.Fields do
        values[fieldTypeInfo.Name] <- ResizeArray()
    for record in records do
        let record = box record
        for fieldTypeInfo in recordTypeInfo.Fields do
            values[fieldTypeInfo.Name].Add(fieldTypeInfo.GetValue record)
    recordTypeInfo.Fields
    |> Array.map (fun fieldTypeInfo ->
        let values = Array.ofSeq values[fieldTypeInfo.Name] :> Array
        { Column.Values = values
          Column.RepetitionLevels = Option.None
          Column.DefinitionLevels = Option.None })
