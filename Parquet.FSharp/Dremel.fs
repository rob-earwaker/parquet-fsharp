module Parquet.FSharp.Dremel

open System
open System.Collections.Generic

type Levels = {
    Repetition: int
    Definition: int }

type Value = {
    FieldName: string
    Object: objnull
    Levels: Levels }

module Levels =
    let Default = {
        Levels.Repetition = 0
        Levels.Definition = 0 }

    //let incrementRepetition (levels: Levels) =
    //    { levels with Repetition = levels.Repetition + 1 }

    //let incrementDefinition (levels: Levels) =
    //    { levels with Definition = levels.Definition + 1 }

let private traverseRecord (recordInfo: RecordInfo) levels recordObj =
    seq {
        for fieldInfo in recordInfo.Fields do
            let valueObj = fieldInfo.GetValue recordObj
            // For now assume every field is required, so don't increment
            // definition level.
            yield {
                Value.FieldName = fieldInfo.Name
                Value.Object = valueObj
                Value.Levels = levels }
    }

type ColumnBuilder(fieldInfo: FieldInfo) =
    // Derive these from schema.
    let repetitionLevelsRequired = false
    let definitionLevelsRequired = false

    let values = ResizeArray()
    let repetitionLevels = ResizeArray()
    let definitionLevels = ResizeArray()

    member this.AddValue(value: Value) =
        values.Add(value.Object)
        if repetitionLevelsRequired then
            repetitionLevels.Add(value.Levels.Repetition)
        if definitionLevelsRequired then
            definitionLevels.Add(value.Levels.Definition)

    member this.BuildColumn() =
        let valuesArray = Array.CreateInstance(fieldInfo.DotnetType, values.Count)
        for index in [ 0 .. values.Count - 1 ] do
            let value = values[index]
            valuesArray.SetValue(value, index)
        let repetitionLevels =
            if repetitionLevelsRequired
            then Option.Some (Array.ofSeq repetitionLevels)
            else Option.None
        let definitionLevels =
            if definitionLevelsRequired
            then Option.Some (Array.ofSeq definitionLevels)
            else Option.None
        { Column.FieldName = fieldInfo.Name
          Column.Values = valuesArray
          Column.RepetitionLevels = repetitionLevels
          Column.DefinitionLevels = definitionLevels }

let shred (records: 'Record[]) =
    let recordInfo = RecordInfo.ofRecord<'Record>
    let columnBuilders = Dictionary()
    for fieldInfo in recordInfo.Fields do
        columnBuilders[fieldInfo.Name] <- ColumnBuilder(fieldInfo)
    // Traverse down through record structure, returning at least one value
    // for each leaf, even if there are no values defined for all of them.
    for record in records do
        let levels = Levels.Default
        for value in traverseRecord recordInfo levels (box record) do
            let columnBuilder = columnBuilders[value.FieldName]
            columnBuilder.AddValue(value)
    recordInfo.Fields
    |> Array.map (fun fieldInfo ->
        let columnBuilder = columnBuilders[fieldInfo.Name]
        columnBuilder.BuildColumn())
