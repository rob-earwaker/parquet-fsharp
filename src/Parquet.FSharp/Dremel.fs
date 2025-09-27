module rec Parquet.FSharp.Dremel

open System
open System.Collections.Generic

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
    // Levels are required if the max level is greater than zero. Since repeated
    // values result in an increment to both the repetition and definition
    // levels, we can determine whether both levels are required by just
    // checking whether the definition level is greater than zero.
    let levelsRequired = maxLevels.Definition > 0

    let mutable valueCount = 0
    let primitiveValues = ResizeArray()
    let repetitionLevels = ResizeArray()
    let definitionLevels = ResizeArray()

    member this.AtomicInfo = atomicInfo
    member this.MaxLevels = maxLevels

    member this.AddPrimitiveValue(primitiveValue: obj, levels: Levels) =
        valueCount <- valueCount + 1
        if not (isNull primitiveValue) then
            primitiveValues.Add(primitiveValue)
        if levelsRequired then
            repetitionLevels.Add(levels.Repetition)
            definitionLevels.Add(levels.Definition)

    member this.BuildColumn() =
        let columnValues =
            match atomicInfo.PrimitiveType with
            | PrimitiveType.Bool ->
                let values = primitiveValues |> Seq.cast<bool> |> Array.ofSeq
                ColumnValues.Bool values
            | PrimitiveType.Int32 ->
                let values = primitiveValues |> Seq.cast<int> |> Array.ofSeq
                ColumnValues.Int32 values
            | PrimitiveType.Int64 ->
                let values = primitiveValues |> Seq.cast<int64> |> Array.ofSeq
                ColumnValues.Int64 values
            | PrimitiveType.Float64 ->
                let values = primitiveValues |> Seq.cast<float> |> Array.ofSeq
                ColumnValues.Float64 values
            | PrimitiveType.ByteArray ->
                let values = primitiveValues |> Seq.cast<byte[]> |> Array.ofSeq
                ColumnValues.ByteArray values
        let repetitionLevels =
            if levelsRequired
            then Option.Some (Array.ofSeq repetitionLevels)
            else Option.None
        let definitionLevels =
            if levelsRequired
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

let private shredAtomic (atomicShredder: AtomicShredder) (parentLevels: Levels) value =
    let atomicInfo = atomicShredder.AtomicInfo
    let maxLevels = atomicShredder.MaxLevels
    let primitiveValue =
        if isNull value
        then null
        else atomicInfo.ResolvePrimitiveValue value
    let levels =
        if atomicInfo.IsOptional && not (isNull primitiveValue)
        then Levels.create parentLevels.Repetition maxLevels.Definition
        else parentLevels
    atomicShredder.AddPrimitiveValue(primitiveValue, levels)

let private shredList (listShredder: ListShredder) (parentLevels: Levels) list =
    let listInfo = listShredder.ListInfo
    let listMaxLevels = listShredder.MaxLevels
    let elementShredder = listShredder.ElementShredder
    let elementMaxLevels = elementShredder.MaxLevels
    let valueList =
        if isNull list
        then null
        else listInfo.ResolveValues list
    let listLevels =
        if listInfo.IsOptional && not (isNull valueList)
        then Levels.create parentLevels.Repetition listMaxLevels.Definition
        else parentLevels
    // If the list is null or empty, leave the levels as they are and pass a
    // null value down to the element shredder. This ensures we write out levels
    // for any child values in the schema.
    if isNull valueList || valueList.Count = 0 then
        shredValue elementShredder listLevels null
    else
        // The first element inherits its repetition level from the parent list,
        // whereas subsequent elements use the max repetition level of the
        // element value. We also need to increment the definition level since
        // we're now inside a non-null repeated field.
        let definitionLevel = listLevels.Definition + 1
        let firstElementLevels = Levels.create listLevels.Repetition definitionLevel
        let otherElementLevels = Levels.create elementMaxLevels.Repetition definitionLevel
        shredValue elementShredder firstElementLevels valueList[0]
        for index in [ 1 .. valueList.Count - 1 ] do
            shredValue elementShredder otherElementLevels valueList[index]

let private shredRecord (recordShredder: RecordShredder) (parentLevels: Levels) record =
    let recordInfo = recordShredder.RecordInfo
    let maxLevels = recordShredder.MaxLevels
    let fieldValues =
        if isNull record
        then null
        else recordInfo.ResolveFieldValues record
    let levels =
        if recordInfo.IsOptional && not (isNull fieldValues)
        then Levels.create parentLevels.Repetition maxLevels.Definition
        else parentLevels
    // Shred all fields regardless of whether the record is null. This ensures
    // we write out levels for any child values in the schema.
    for fieldShredder in recordShredder.FieldShredders do
        let fieldInfo = fieldShredder.FieldInfo
        let fieldValue =
            if isNull fieldValues
            then null
            else fieldValues[fieldInfo.Index]
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

type private AtomicAssembler(atomicInfo: AtomicInfo, maxLevels: Levels, column: Column) =
    // Levels are required if the max level is greater than zero. Since repeated
    // values result in an increment to both the repetition and definition
    // levels, we can determine whether both levels are required by just
    // checking whether the definition level is greater than zero.
    let levelsRequired = maxLevels.Definition > 0

    do if levelsRequired && column.RepetitionLevels.IsNone then
        failwith "repetition levels are required but none exist in column data"

    do if levelsRequired && column.DefinitionLevels.IsNone then
        failwith "definition levels are required but none exist in column data"

    let primitiveValues =
        match column.Values with
        | ColumnValues.Bool values -> values :> Array
        | ColumnValues.Int32 values -> values :> Array
        | ColumnValues.Int64 values -> values :> Array
        | ColumnValues.Float64 values -> values :> Array
        | ColumnValues.ByteArray values -> values :> Array

    let mutable nextLevelsIndex = 0
    let mutable nextValueIndex = 0

    member this.AtomicInfo = atomicInfo
    member this.MaxLevels = maxLevels

    member this.SkipUndefinedValue() =
        if levelsRequired
        then nextLevelsIndex <- nextLevelsIndex + 1
        else failwith "value is required and should not be UNDEFINED"

    member this.ReadNextValue() =
        let levels =
            if levelsRequired
            then
                let repetitionLevel = column.RepetitionLevels.Value[nextLevelsIndex]
                let definitionLevel = column.DefinitionLevels.Value[nextLevelsIndex]
                nextLevelsIndex <- nextLevelsIndex + 1
                Levels.create repetitionLevel definitionLevel
            else
                Levels.Default
        let value =
            // NULL values are not written to the values array as they are
            // implied by a definition level that is lower than the maximum. A
            // NULL value means that either this value is NULL or this value is
            // UNDEFINED (due to one of its ancestors being NULL).
            if levels.Definition < maxLevels.Definition
            then
                // If this value is optional and the definition level is one
                // less than the maximum definition level then this value is
                // DEFINED, but NULL.
                if atomicInfo.IsOptional
                    && levels.Definition = maxLevels.Definition - 1
                then Option.Some (atomicInfo.CreateNullValue ())
                // If the value is not optional, or if it is optional and the
                // definition level is more than one less than the maximum
                // definition level then this value is UNDEFINED.
                else Option.None
            else
                // If the definition level equals the maximum definition level
                // then this value is DEFINED and NOTNULL. Get the next
                // primitive value and convert it to the correct type.
                let primitiveValue = primitiveValues.GetValue(nextValueIndex)
                nextValueIndex <- nextValueIndex + 1
                Option.Some (atomicInfo.FromPrimitiveValue primitiveValue)
        levels, value

type private ListAssembler = {
    ListInfo: ListInfo
    MaxLevels: Levels
    ElementAssembler: ValueAssembler }

type private FieldAssembler = {
    FieldInfo: FieldInfo
    ValueAssembler: ValueAssembler }

type private RecordAssembler = {
    RecordInfo: RecordInfo
    MaxLevels: Levels
    FieldAssemblers: FieldAssembler[] }

type private ValueAssembler =
    | Atomic of AtomicAssembler
    | List of ListAssembler
    | Record of RecordAssembler
    with
    member this.MaxLevels =
        match this with
        | ValueAssembler.Atomic atomicAssembler -> atomicAssembler.MaxLevels
        | ValueAssembler.List listAssembler -> listAssembler.MaxLevels
        | ValueAssembler.Record recordAssembler -> recordAssembler.MaxLevels

module private AtomicAssembler =
    let forAtomic (atomicInfo: AtomicInfo) parentMaxLevels (columns: Queue<Column>) =
        let maxLevels =
            if atomicInfo.IsOptional
            then Levels.incrementForOptional parentMaxLevels
            else parentMaxLevels
        let column =
            if columns.Count = 0
            then failwith "no column found for atomic value"
            else columns.Dequeue()
        AtomicAssembler(atomicInfo, maxLevels, column)

module private ListAssembler =
    let forList (listInfo: ListInfo) parentMaxLevels columns =
        let listMaxLevels =
            if listInfo.IsOptional
            then Levels.incrementForOptional parentMaxLevels
            else parentMaxLevels
        let elementMaxLevels = Levels.incrementForRepeated listMaxLevels
        let elementAssembler =
            ValueAssembler.forValue listInfo.ElementInfo elementMaxLevels columns
        { ListAssembler.ListInfo = listInfo
          ListAssembler.MaxLevels = listMaxLevels
          ListAssembler.ElementAssembler = elementAssembler }

module private RecordAssembler =
    let forRecord (recordInfo: RecordInfo) parentMaxLevels columns =
        let maxLevels =
            if recordInfo.IsOptional
            then Levels.incrementForOptional parentMaxLevels
            else parentMaxLevels
        let fieldAssemblers =
            recordInfo.Fields
            |> Array.map (fun fieldInfo ->
                let valueAssembler = ValueAssembler.forValue fieldInfo.ValueInfo maxLevels
                { FieldAssembler.FieldInfo = fieldInfo
                  FieldAssembler.ValueAssembler = valueAssembler columns })
        { RecordAssembler.RecordInfo = recordInfo
          RecordAssembler.MaxLevels = maxLevels
          RecordAssembler.FieldAssemblers = fieldAssemblers }

module private ValueAssembler =
    let forValue (valueInfo: ValueInfo) parentMaxLevels columns =
        match valueInfo with
        | ValueInfo.Atomic atomicInfo ->
            AtomicAssembler.forAtomic atomicInfo parentMaxLevels columns
            |> ValueAssembler.Atomic
        | ValueInfo.List listInfo ->
            ListAssembler.forList listInfo parentMaxLevels columns
            |> ValueAssembler.List
        | ValueInfo.Record recordInfo ->
            RecordAssembler.forRecord recordInfo parentMaxLevels columns
            |> ValueAssembler.Record

let private assembleNextRecord (recordAssembler: RecordAssembler) =
    let recordInfo = recordAssembler.RecordInfo
    let recordMaxLevels = recordAssembler.MaxLevels
    let fieldValues = Array.zeroCreate<obj> recordAssembler.FieldAssemblers.Length
    // If the record is NULL or UNDEFINED (due to one of its ancestors being
    // NULL) then all field values will be UNDEFINED. Determine whether this is
    // the case from the first field value.
    let firstFieldAssembler = recordAssembler.FieldAssemblers[0]
    let firstFieldValueAssembler = firstFieldAssembler.ValueAssembler
    let firstFieldLevels, firstFieldValue = assembleNextValue firstFieldValueAssembler
    match firstFieldValue with
    // If the first field value is UNDEFINED then either the record is NULL or
    // the record is UNDEFINED (due to one of its ancestors being NULL). In
    // either case, the definition level of the first field should be less than
    // the maximum definition level of the record.
    | Option.None ->
        let record =
            // If the record is optional and the definition level of the first
            // field value is one less than the maximum definition level of the
            // record then the record is DEFINED, but NULL.
            if recordInfo.IsOptional
                && firstFieldLevels.Definition = recordMaxLevels.Definition - 1
            then Option.Some (recordInfo.CreateNullValue ())
            // If the record is not optional, or if it is optional and the
            // definition level of the first field value is more than one less
            // than the maximum definition level of the record then the record
            // is UNDEFINED.
            else Option.None
        // If the first field is UNDEFINED then any remaining fields in the
        // record should also be UNDEFINED. It is important to skip these
        // UNDEFINED values, otherwise they will be considered part of the next
        // record.
        for fieldAssembler in recordAssembler.FieldAssemblers[1..] do
            skipUndefinedValue fieldAssembler.ValueAssembler
        // Return the NULL or UNDEFINED record along with the levels of the
        // first field. It's important to pass the first field levels back up
        // the chain so that ancestor values can be resolved correctly. In the
        // case where the record is UNDEFINED, the definition level of the first
        // field determines whether ancestors are NULL or UNDEFINED.
        firstFieldLevels, record
    // If the first field value is DEFINED then the record must also be DEFINED.
    | Option.Some firstFieldValue ->
        // Add the first field value to the array of field values.
        fieldValues[0] <- firstFieldValue
        // If the first field is DEFINED then any remaining fields in the record
        // should also be DEFINED. Assemble their values and add to the array of
        // field values.
        for fieldAssembler in recordAssembler.FieldAssemblers[1..] do
            let fieldInfo = fieldAssembler.FieldInfo
            let _, fieldValue = assembleNextValue fieldAssembler.ValueAssembler
            // Assume the field value is DEFINED.
            fieldValues[fieldInfo.Index] <- fieldValue.Value
        // Construct the record from the field values.
        let record = recordInfo.CreateValue fieldValues
        // Return the record along with the levels of the first field. It's
        // important to pass the first field levels back up the chain so that
        // ancestor values can be resolved correctly. In the case where this
        // record is a repeated value, the repetition level of the first field
        // indicates the repetition level of the record.
        firstFieldLevels, Option.Some record

let private assembleNextValue (valueAssembler: ValueAssembler) =
    match valueAssembler with
    | ValueAssembler.Atomic atomicAssembler -> atomicAssembler.ReadNextValue()
    | ValueAssembler.List listAssembler -> failwith "unsupported"
    | ValueAssembler.Record recordAssembler -> failwith "unsupported"

let private skipUndefinedValue (valueAssembler: ValueAssembler) =
    match valueAssembler with
    | ValueAssembler.Atomic atomicAssembler -> atomicAssembler.SkipUndefinedValue()
    | ValueAssembler.List listAssembler -> failwith "unsupported"
    | ValueAssembler.Record recordAssembler -> failwith "unsupported"

let assemble<'Record> (columns: Column[]) =
    let recordInfo = RecordInfo.ofRecord typeof<'Record>
    let maxLevels = Levels.Default
    let columns = Queue(columns)
    let recordAssembler = RecordAssembler.forRecord recordInfo maxLevels columns
    let records = ResizeArray()
    for _ in [ 1 .. 20 ] do
        let _, record = assembleNextRecord recordAssembler
        records.Add(record.Value :?> 'Record)
    Array.ofSeq records
