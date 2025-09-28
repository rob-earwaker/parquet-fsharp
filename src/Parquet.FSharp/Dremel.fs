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
    let repetitionLevelsRequired = maxLevels.Repetition > 0
    let definitionLevelsRequired = maxLevels.Definition > 0

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
        if repetitionLevelsRequired then
            repetitionLevels.Add(levels.Repetition)
        if definitionLevelsRequired then
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

type private AssembledValue = {
    Levels: Levels
    Value: obj option }

module private AssembledValue =
    let create levels value =
        { AssembledValue.Levels = levels
          AssembledValue.Value = value }

type private AtomicAssembler(atomicInfo: AtomicInfo, maxLevels: Levels, column: Column) =
    let repetitionLevelsRequired = maxLevels.Repetition > 0
    let definitionLevelsRequired = maxLevels.Definition > 0

    do if repetitionLevelsRequired && column.RepetitionLevels.IsNone then
        failwith "repetition levels are required but none exist in column data"

    do if definitionLevelsRequired && column.DefinitionLevels.IsNone then
        failwith "definition levels are required but none exist in column data"

    let primitiveValues =
        match column.Values with
        | ColumnValues.Bool values -> values :> Array
        | ColumnValues.Int32 values -> values :> Array
        | ColumnValues.Int64 values -> values :> Array
        | ColumnValues.Float64 values -> values :> Array
        | ColumnValues.ByteArray values -> values :> Array

    // NULL values are not written to the array, so keep track of the array
    // index and the value index separately. For optional values, these will be
    // different if there are any NULL values in the column.
    let mutable nextValueIndex = 0
    let mutable nextArrayValueIndex = 0

    let mutable peekedValue = Option.None

    member this.MaxLevels = maxLevels

    member this.SkipUndefinedValue() =
        nextValueIndex <- nextValueIndex + 1

    member this.PeekNextValue() =
        let peekedValue' =
            // Determine whether we've reached the end of our column data based on
            // the value index. It's important not to use the array value index
            // because for optional values reaching the end of the values array does
            // not imply we've reached the end of the column as there could be more
            // NULL values in the column.
            if nextValueIndex = column.ValueCount
            then Option.None
            else
                let repetitionLevel =
                    if repetitionLevelsRequired
                    then column.RepetitionLevels.Value[nextValueIndex]
                    else 0
                let definitionLevel =
                    if definitionLevelsRequired
                    then column.DefinitionLevels.Value[nextValueIndex]
                    else 0
                let levels = Levels.create repetitionLevel definitionLevel
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
                        let primitiveValue = primitiveValues.GetValue(nextArrayValueIndex)
                        nextArrayValueIndex <- nextArrayValueIndex + 1
                        Option.Some (atomicInfo.FromPrimitiveValue primitiveValue)
                nextValueIndex <- nextValueIndex + 1
                let assembledValue = AssembledValue.create levels value
                Option.Some assembledValue
        peekedValue <- Option.Some peekedValue'
        peekedValue'

    member this.SkipPeekedValue() =
        peekedValue <- Option.None

    member this.AssembleNextValue() =
        let peekedValue =
            match peekedValue with
            | Option.Some peekedValue -> peekedValue
            | Option.None -> this.PeekNextValue()
        this.SkipPeekedValue()
        peekedValue

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

let private assembleNextList (listAssembler: ListAssembler) =
    let listInfo = listAssembler.ListInfo
    let listMaxLevels = listAssembler.MaxLevels
    let elementAssembler = listAssembler.ElementAssembler
    let elementMaxLevels = elementAssembler.MaxLevels
    let elementValues = ResizeArray()
    // If the list is NULL or UNDEFINED (due to one of its ancestors being NULL)
    // then there will be a single UNDEFINED element to indicate it. Determine
    // whether this is the case by assembling the next element value.
    match assembleNextValue elementAssembler with
    | Option.None -> Option.None
    | Option.Some nextElement ->
        let list =
            match nextElement.Value with
            // If the next element value is UNDEFINED then either the list is
            // empty, the list is NULL or the list is UNDEFINED (due to one of
            // its ancestors being NULL).
            | Option.None ->
                // If the definition level of the next element is equal to the
                // maximum definition level of the list then the list is empty.
                if nextElement.Levels.Definition = listMaxLevels.Definition
                then Option.Some (listInfo.CreateValue [||])
                // If the list is optional and the definition level of the next
                // element is one less than the maximum definition level of the
                // list then the list is DEFINED, but NULL.
                elif listInfo.IsOptional
                    && nextElement.Levels.Definition = listMaxLevels.Definition - 1
                then Option.Some (listInfo.CreateNullValue ())
                // If the list is not optional, or if it is optional and the
                // definition level of the next element value is more than one
                // less than the maximum definition level of the list then the
                // list is UNDEFINED.
                else Option.None
            // If the next element value is DEFINED then the list must also be
            // DEFINED.
            | Option.Some nextElementValue ->
                // Add the element to the list of element values.
                elementValues.Add(nextElementValue)
                // The end of the list is indicated by a repetition level less
                // than the max repetition level of the element values, so keep
                // reading element values until we find an element where this is
                // the case. This final element will form part of another list
                // so we don't want to include it in the current list. For this
                // reason we iterate by peeking the next element and only skip
                // it if the repetition level is equal to the max repetition
                // level of the element values.
                let mutable allElementsFound = false
                while not allElementsFound do
                    let nextElementValue =
                        match peekNextValue elementAssembler with
                        | Option.None -> Option.None
                        | Option.Some nextElement ->
                            if nextElement.Levels.Repetition < elementMaxLevels.Repetition
                            then Option.None
                            else
                                skipPeekedValue elementAssembler
                                Option.Some nextElement.Value.Value
                    match nextElementValue with
                    | Option.None -> allElementsFound <- true
                    | Option.Some nextElementValue -> elementValues.Add(nextElementValue)
                // Construct the list from the element values.
                Option.Some (listInfo.CreateValue (Array.ofSeq elementValues))
        // Return the list along with the levels of the first element. It's
        // important to pass the first element levels back up the chain so that
        // ancestor values can be resolved correctly. In the case where the
        // list is UNDEFINED, the definition level of the first element
        // determines whether ancestors are NULL or UNDEFINED. In the case where
        // the list itself is repeated, the repetition level of the first
        // element indicates the repetition level of the list.
        let assembledList = AssembledValue.create nextElement.Levels list
        Option.Some assembledList

let private assembleNextRecord (recordAssembler: RecordAssembler) =
    let recordInfo = recordAssembler.RecordInfo
    let recordMaxLevels = recordAssembler.MaxLevels
    let fieldValues = Array.zeroCreate<obj> recordAssembler.FieldAssemblers.Length
    // If the record is NULL or UNDEFINED (due to one of its ancestors being
    // NULL) then all field values will be UNDEFINED. Determine whether this is
    // the case from the first field value.
    let firstFieldAssembler = recordAssembler.FieldAssemblers[0]
    match assembleNextValue firstFieldAssembler.ValueAssembler with
    | Option.None -> Option.None
    | Option.Some firstField ->
        let record =
            match firstField.Value with
            // If the first field value is UNDEFINED then either the record is NULL
            // or the record is UNDEFINED (due to one of its ancestors being NULL).
            // In either case, the definition level of the first field will be
            // less than the maximum definition level of the record.
            | Option.None ->
                // If the first field is UNDEFINED then any remaining fields in the
                // record should also be UNDEFINED. It is important to skip these
                // UNDEFINED values, otherwise they will be considered part of the next
                // record.
                for fieldAssembler in recordAssembler.FieldAssemblers[1..] do
                    skipUndefinedValue fieldAssembler.ValueAssembler
                // If the record is optional and the definition level of the first
                // field value is one less than the maximum definition level of the
                // record then the record is DEFINED, but NULL.
                if recordInfo.IsOptional
                    && firstField.Levels.Definition = recordMaxLevels.Definition - 1
                then Option.Some (recordInfo.CreateNullValue ())
                // If the record is not optional, or if it is optional and the
                // definition level of the first field value is more than one less
                // than the maximum definition level of the record then the record
                // is UNDEFINED.
                else Option.None
            // If the first field value is DEFINED then the record must also be DEFINED.
            | Option.Some firstFieldValue ->
                // Add the first field value to the array of field values.
                fieldValues[0] <- firstFieldValue
                // If the first field is DEFINED then any remaining fields in the record
                // should also be DEFINED. Assemble their values and add to the array of
                // field values.
                for fieldAssembler in recordAssembler.FieldAssemblers[1..] do
                    let fieldInfo = fieldAssembler.FieldInfo
                    let fieldValue = assembleNextValue fieldAssembler.ValueAssembler
                    // Assume there is a next field value and that it is DEFINED.
                    fieldValues[fieldInfo.Index] <- fieldValue.Value.Value.Value
                // Construct the record from the field values.
                Option.Some (recordInfo.CreateValue fieldValues)
        // Return the record along with the levels of the first field. It's
        // important to pass the first field levels back up the chain so that
        // ancestor values can be resolved correctly. In the case where the
        // record is UNDEFINED, the definition level of the first field
        // determines whether ancestors are NULL or UNDEFINED. In the case where
        // the record is a repeated value, the repetition level of the first
        // field indicates the repetition level of the record.
        let assembledRecord = AssembledValue.create firstField.Levels record
        Option.Some assembledRecord

let private skipUndefinedRecord (recordAssembler: RecordAssembler) =
    for fieldAssembler in recordAssembler.FieldAssemblers do
        skipUndefinedValue fieldAssembler.ValueAssembler

let private assembleNextValue (valueAssembler: ValueAssembler) =
    match valueAssembler with
    | ValueAssembler.Atomic atomicAssembler -> atomicAssembler.AssembleNextValue()
    | ValueAssembler.List listAssembler -> assembleNextList listAssembler
    | ValueAssembler.Record recordAssembler -> assembleNextRecord recordAssembler

let private skipUndefinedValue (valueAssembler: ValueAssembler) =
    match valueAssembler with
    | ValueAssembler.Atomic atomicAssembler -> atomicAssembler.SkipUndefinedValue()
    | ValueAssembler.List listAssembler -> failwith "unsupported"
    | ValueAssembler.Record recordAssembler -> skipUndefinedRecord recordAssembler

let private peekNextValue (valueAssembler: ValueAssembler) =
    match valueAssembler with
    | ValueAssembler.Atomic atomicAssembler -> atomicAssembler.PeekNextValue()
    | ValueAssembler.List listAssembler -> failwith "unsupported"
    | ValueAssembler.Record recordAssembler -> failwith "unsupported"

let private skipPeekedValue (valueAssembler: ValueAssembler) =
    match valueAssembler with
    | ValueAssembler.Atomic atomicAssembler -> atomicAssembler.SkipPeekedValue()
    | ValueAssembler.List listAssembler -> failwith "unsupported"
    | ValueAssembler.Record recordAssembler -> failwith "unsupported"

let assemble<'Record> (columns: Column[]) =
    let recordInfo = RecordInfo.ofRecord typeof<'Record>
    let maxLevels = Levels.Default
    let columns = Queue(columns)
    let recordAssembler = RecordAssembler.forRecord recordInfo maxLevels columns
    let records = ResizeArray()
    let mutable allRecordsAssembled = false
    while not allRecordsAssembled do
        match assembleNextRecord recordAssembler with
        | Option.None -> allRecordsAssembled <- true
        | Option.Some record ->
            match record.Value with
            | Option.None -> failwith "root record should always be defined"
            | Option.Some record -> records.Add(record :?> 'Record)
    Array.ofSeq records
