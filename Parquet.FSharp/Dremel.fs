module Parquet.FSharp.Dremel

open FSharp.Reflection
open System
open System.Collections.Generic

type private Field = {
    Name: string
    Type: Type
    Value: objnull }

type private FieldInfo = {
    Name: string
    Type: Type
    GetValue: obj -> objnull }

module private FieldInfo =
    let ofRecord<'Record> =
        FSharpType.GetRecordFields(typeof<'Record>)
        |> Array.map (fun field ->
            { FieldInfo.Name = field.Name
              FieldInfo.Type = field.PropertyType
              FieldInfo.GetValue = FSharpValue.PreComputeRecordFieldReader(field) })

let disassemble (records: 'Record[]) =
    let fieldInfo = FieldInfo.ofRecord<'Record>
    let values = Dictionary()
    for fieldInfo in fieldInfo do
        values[fieldInfo.Name] <- ResizeArray()
    for record in records do
        let record = box record
        for fieldInfo in fieldInfo do
            values[fieldInfo.Name].Add(fieldInfo.GetValue record)
    fieldInfo
    |> Array.map (fun fieldInfo ->
        let values = Array.ofSeq values[fieldInfo.Name] :> Array
        { Column.Values = values
          Column.RepetitionLevels = Option.None
          Column.DefinitionLevels = Option.None })
