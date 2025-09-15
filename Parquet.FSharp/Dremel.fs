module rec Parquet.FSharp.Dremel

open System
open System.Collections.Generic

type private Levels = {
    Repetition: int
    Definition: int }

module private Levels =
    let Default = {
        Levels.Repetition = 0
        Levels.Definition = 0 }

    let setRepetition (levels: Levels) value =
        { levels with Repetition = value }

    let setDefinition (levels: Levels) value =
        { levels with Definition = value }

    let incrementRepetition levels =
        setRepetition levels (levels.Repetition + 1)

    let incrementDefinition levels =
        setDefinition levels (levels.Definition + 1)

type private AtomicShredder(atomicInfo: AtomicInfo, path, maxLevels: Levels) =
    let repetitionLevelsRequired = maxLevels.Repetition > 0
    let definitionLevelsRequired = maxLevels.Definition > 0

    let mutable valueCount = 0
    let values = ResizeArray()
    let repetitionLevels = ResizeArray()
    let definitionLevels = ResizeArray()

    member this.AtomicInfo = atomicInfo
    member this.MaxLevels = maxLevels

    member this.AddValue(value: obj, levels: Levels) =
        valueCount <- valueCount + 1
        if not (isNull value) then
            values.Add(value)
        if repetitionLevelsRequired then
            repetitionLevels.Add(levels.Repetition)
        if definitionLevelsRequired then
            definitionLevels.Add(levels.Definition)

    member this.BuildColumn() =
        let columnValues =
            match atomicInfo.PrimitiveType with
            | PrimitiveType.Bool ->
                let values = values |> Seq.cast<bool> |> Array.ofSeq
                ColumnValues.Bool values
            | PrimitiveType.Int32 ->
                let values = values |> Seq.cast<int> |> Array.ofSeq
                ColumnValues.Int32 values
        let repetitionLevels =
            if repetitionLevelsRequired
            then Option.Some (Array.ofSeq repetitionLevels)
            else Option.None
        let definitionLevels =
            if definitionLevelsRequired
            then Option.Some (Array.ofSeq definitionLevels)
            else Option.None
        { Column.Path = path
          Column.MaxRepetitionLevel = maxLevels.Repetition
          Column.MaxDefinitionLevel = maxLevels.Definition
          Column.ValueCount = valueCount
          Column.Values = columnValues
          Column.RepetitionLevels = repetitionLevels
          Column.DefinitionLevels = definitionLevels }

type private FieldShredder = {
    FieldInfo: FieldInfo
    ValueShredder: ValueShredder }

type private RecordShredder = {
    RecordInfo: RecordInfo
    MaxLevels: Levels
    FieldShredders: FieldShredder[] }

type private ValueShredder =
    | Atomic of AtomicShredder
    | Record of RecordShredder

module private AtomicShredder =
    let forAtomic (atomicInfo: AtomicInfo) path parentMaxLevels =
        let maxLevels =
            if atomicInfo.IsOptional
            then Levels.incrementDefinition parentMaxLevels
            else parentMaxLevels
        AtomicShredder(atomicInfo, path, maxLevels)

module private RecordShredder =
    let forRecord (recordInfo: RecordInfo) path parentMaxLevels =
        let maxLevels =
            if recordInfo.IsOptional
            then Levels.incrementDefinition parentMaxLevels
            else parentMaxLevels
        let fieldShredders =
            recordInfo.Fields
            |> Array.map (fun fieldInfo ->
                let path = Array.append path [| fieldInfo.Name |]
                let valueShredder = ValueShredder.forValue fieldInfo.ValueInfo path maxLevels
                { FieldShredder.FieldInfo = fieldInfo
                  FieldShredder.ValueShredder = valueShredder })
        { RecordShredder.RecordInfo = recordInfo
          RecordShredder.MaxLevels = maxLevels
          RecordShredder.FieldShredders = fieldShredders }

    let buildColumns (recordShredder: RecordShredder) =
        seq {
            for fieldShredder in recordShredder.FieldShredders do
                yield! ValueShredder.buildColumns fieldShredder.ValueShredder
        }

module private ValueShredder =
    let forValue (valueInfo: ValueInfo) path parentMaxLevels =
        match valueInfo with
        | ValueInfo.Atomic atomicInfo ->
            AtomicShredder.forAtomic atomicInfo path parentMaxLevels
            |> ValueShredder.Atomic
        | ValueInfo.Record recordInfo ->
            RecordShredder.forRecord recordInfo path parentMaxLevels
            |> ValueShredder.Record

    let buildColumns (valueShredder: ValueShredder) =
        seq {
            match valueShredder with
            | ValueShredder.Atomic atomicShredder ->
                yield atomicShredder.BuildColumn()
            | ValueShredder.Record recordShredder ->
                yield! RecordShredder.buildColumns recordShredder
        }

let rec private shredRecord (recordShredder: RecordShredder) parentLevels recordValue =
    for fieldShredder in recordShredder.FieldShredders do
        let fieldValue = fieldShredder.FieldInfo.GetValue recordValue
        match fieldShredder.ValueShredder with
        | ValueShredder.Atomic atomicShredder ->
            let atomicInfo = atomicShredder.AtomicInfo
            let primitiveValue = atomicInfo.ConvertValueToPrimitive fieldValue
            let levels =
                if atomicInfo.IsOptional
                    && not (isNull primitiveValue)
                then Levels.setDefinition parentLevels atomicShredder.MaxLevels.Definition
                else parentLevels
            atomicShredder.AddValue(primitiveValue, levels)
        | ValueShredder.Record recordShredder ->
            let recordInfo = recordShredder.RecordInfo
            let recordValue = fieldValue
            let levels =
                if recordInfo.IsOptional
                    && not (isNull recordValue)
                then Levels.setDefinition parentLevels recordShredder.MaxLevels.Definition
                else parentLevels
            shredRecord recordShredder levels recordValue

let shred (records: 'Record[]) =
    let recordInfo = RecordInfo.ofRecord typeof<'Record>
    let path = [||]
    let maxLevels = Levels.Default
    let recordShredder = RecordShredder.forRecord recordInfo path maxLevels
    for record in records do
        let levels = Levels.Default
        let recordValue = box record
        shredRecord recordShredder levels recordValue
    recordShredder
    |> RecordShredder.buildColumns
    |> Array.ofSeq
