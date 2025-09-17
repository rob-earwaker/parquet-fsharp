module rec Parquet.FSharp.Dremel

type private Levels = {
    Repetition: int
    Definition: int }

module private Levels =
    let create repetition definition =
        { Levels.Repetition = repetition
          Levels.Definition = definition }

    let Default = create 0 0

    let incrementForOptional (levels: Levels) =
        { levels with Definition = levels.Definition + 1 }

    let incrementForRepeated (levels: Levels) =
        { levels with
            Repetition = levels.Repetition + 1
            Definition = levels.Definition + 1 }

type private AtomicShredder(atomicInfo: AtomicInfo, maxLevels: Levels) =
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
            | PrimitiveType.Int64 ->
                let values = values |> Seq.cast<int64> |> Array.ofSeq
                ColumnValues.Int64 values
            | PrimitiveType.Float64 ->
                let values = values |> Seq.cast<float> |> Array.ofSeq
                ColumnValues.Float64 values
            | PrimitiveType.ByteArray ->
                let values = values |> Seq.cast<byte[]> |> Array.ofSeq
                ColumnValues.ByteArray values
        let repetitionLevels =
            if repetitionLevelsRequired
            then Option.Some (Array.ofSeq repetitionLevels)
            else Option.None
        let definitionLevels =
            if definitionLevelsRequired
            then Option.Some (Array.ofSeq definitionLevels)
            else Option.None
        { Column.ValueCount = valueCount
          Column.Values = columnValues
          Column.MaxRepetitionLevel = maxLevels.Repetition
          Column.MaxDefinitionLevel = maxLevels.Definition
          Column.RepetitionLevels = repetitionLevels
          Column.DefinitionLevels = definitionLevels }

type private ListShredder = {
    ListInfo: ListInfo
    MaxLevels: Levels
    ElementShredder: ValueShredder }

type private FieldShredder = {
    FieldInfo: FieldInfo
    ValueShredder: ValueShredder }

type private RecordShredder = {
    RecordInfo: RecordInfo
    MaxLevels: Levels
    FieldShredders: FieldShredder[] }

type private ValueShredder =
    | Atomic of AtomicShredder
    | List of ListShredder
    | Record of RecordShredder
    with
    member this.MaxLevels =
        match this with
        | ValueShredder.Atomic atomicShredder -> atomicShredder.MaxLevels
        | ValueShredder.List listShredder -> listShredder.MaxLevels
        | ValueShredder.Record recordShredder -> recordShredder.MaxLevels

module private AtomicShredder =
    let forAtomic (atomicInfo: AtomicInfo) parentMaxLevels =
        let maxLevels =
            if atomicInfo.IsOptional
            then Levels.incrementForOptional parentMaxLevels
            else parentMaxLevels
        AtomicShredder(atomicInfo, maxLevels)

module private ListShredder =
    let forList (listInfo: ListInfo) parentMaxLevels =
        let listMaxLevels =
            if listInfo.IsOptional
            then Levels.incrementForOptional parentMaxLevels
            else parentMaxLevels
        let elementMaxLevels = Levels.incrementForRepeated listMaxLevels
        let elementShredder = ValueShredder.forValue listInfo.ElementInfo elementMaxLevels
        { ListShredder.ListInfo = listInfo
          ListShredder.MaxLevels = listMaxLevels
          ListShredder.ElementShredder = elementShredder }

    let buildColumns (listShredder: ListShredder) =
        ValueShredder.buildColumns listShredder.ElementShredder

module private RecordShredder =
    let forRecord (recordInfo: RecordInfo) parentMaxLevels =
        let maxLevels =
            if recordInfo.IsOptional
            then Levels.incrementForOptional parentMaxLevels
            else parentMaxLevels
        let fieldShredders =
            recordInfo.Fields
            |> Array.map (fun fieldInfo ->
                let valueShredder = ValueShredder.forValue fieldInfo.ValueInfo maxLevels
                { FieldShredder.FieldInfo = fieldInfo
                  FieldShredder.ValueShredder = valueShredder })
        { RecordShredder.RecordInfo = recordInfo
          RecordShredder.MaxLevels = maxLevels
          RecordShredder.FieldShredders = fieldShredders }

    let buildColumns (recordShredder: RecordShredder) =
        recordShredder.FieldShredders
        |> Seq.map _.ValueShredder
        |> Seq.collect ValueShredder.buildColumns

module private ValueShredder =
    let forValue (valueInfo: ValueInfo) parentMaxLevels =
        match valueInfo with
        | ValueInfo.Atomic atomicInfo ->
            AtomicShredder.forAtomic atomicInfo parentMaxLevels
            |> ValueShredder.Atomic
        | ValueInfo.List listInfo ->
            ListShredder.forList listInfo parentMaxLevels
            |> ValueShredder.List
        | ValueInfo.Record recordInfo ->
            RecordShredder.forRecord recordInfo parentMaxLevels
            |> ValueShredder.Record

    let buildColumns (valueShredder: ValueShredder) =
        seq {
            match valueShredder with
            | ValueShredder.Atomic atomicShredder ->
                yield atomicShredder.BuildColumn()
            | ValueShredder.List listShredder ->
                yield! ListShredder.buildColumns listShredder
            | ValueShredder.Record recordShredder ->
                yield! RecordShredder.buildColumns recordShredder
        }

let private shredAtomic (atomicShredder: AtomicShredder) (parentLevels: Levels) atomicValue =
    let atomicInfo = atomicShredder.AtomicInfo
    let maxLevels = atomicShredder.MaxLevels
    let primitiveValue = atomicInfo.ConvertValueToPrimitive atomicValue
    let levels =
        if atomicInfo.IsOptional
            && not (isNull primitiveValue)
        then Levels.create parentLevels.Repetition maxLevels.Definition
        else parentLevels
    atomicShredder.AddValue(primitiveValue, levels)

let private shredList (listShredder: ListShredder) (parentLevels: Levels) listValue =
    let listInfo = listShredder.ListInfo
    let listMaxLevels = listShredder.MaxLevels
    let elementShredder = listShredder.ElementShredder
    let elementMaxLevels = elementShredder.MaxLevels
    // TODO: It's a bit weird that we have to extract the list from the value
    // here and for the atomic, but not for the record. Are records just
    // different or should we always have a '(Try)GetValue'-like function for all
    // that unwraps the nullable/option cases.
    let elementList = listInfo.GetValues listValue
    let listLevels =
        if listInfo.IsOptional
            && not (isNull elementList)
        then Levels.create parentLevels.Repetition listMaxLevels.Definition
        else parentLevels
    // If the list is null or empty, leave the levels as they are and pass a
    // null value down to the element shredder. This ensures we write out levels
    // for any child values in the schema.
    if isNull elementList
        || elementList.Count = 0 then
        shredValue elementShredder listLevels null
    else
        // The first element inherits its repetition level from the parent list,
        // whereas subsequent elements use the max repetition level of the
        // element value. We also need to increment the definition level since
        // we're now inside a non-null repeated field.
        let definitionLevel = listLevels.Definition + 1
        let firstElementLevels = Levels.create listLevels.Repetition definitionLevel
        let otherElementLevels = Levels.create elementMaxLevels.Repetition definitionLevel
        shredValue elementShredder firstElementLevels elementList[0]
        for index in [ 1 .. elementList.Count - 1 ] do
            shredValue elementShredder otherElementLevels elementList[index]

let private shredRecord (recordShredder: RecordShredder) (parentLevels: Levels) recordValue =
    let recordInfo = recordShredder.RecordInfo
    let maxLevels = recordShredder.MaxLevels
    let levels =
        if recordInfo.IsOptional
            && not (isNull recordValue)
        then Levels.create parentLevels.Repetition maxLevels.Definition
        else parentLevels
    // Shred all fields regardless of whether the record is null. This ensures
    // we write out levels for any child values in the schema.
    for fieldShredder in recordShredder.FieldShredders do
        let fieldInfo = fieldShredder.FieldInfo
        let fieldValue = fieldInfo.GetValue recordValue
        shredValue fieldShredder.ValueShredder levels fieldValue

let private shredValue valueShredder parentLevels value =
    match valueShredder with
    | ValueShredder.Atomic atomicShredder -> shredAtomic atomicShredder parentLevels value
    | ValueShredder.List listShredder -> shredList listShredder parentLevels value
    | ValueShredder.Record recordShredder -> shredRecord recordShredder parentLevels value

let shred (records: 'Record[]) =
    let recordInfo = RecordInfo.ofRecord typeof<'Record>
    let maxLevels = Levels.Default
    let recordShredder = RecordShredder.forRecord recordInfo maxLevels
    for record in records do
        let levels = Levels.Default
        let recordValue = box record
        shredRecord recordShredder levels recordValue
    recordShredder
    |> RecordShredder.buildColumns
    |> Array.ofSeq
