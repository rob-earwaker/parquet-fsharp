module rec Parquet.FSharp.Dremel

open Parquet.Data
open Parquet.Schema
open System
open System.Collections.Generic

module private Schema =
    let private getValueSchema fieldName valueInfo =
        match valueInfo with
        | ValueInfo.Atomic atomicInfo ->
            // TODO: Should we use some of the custom DataField types here, e.g. DecimalDataField?
            let dataType = atomicInfo.DataDotnetType
            let isNullable = atomicInfo.IsOptional
            DataField(fieldName, dataType, isNullable) :> Field
        | ValueInfo.List listInfo ->
            let element = getValueSchema ListField.ElementName listInfo.ElementInfo
            ListField(fieldName, element) :> Field
        | ValueInfo.Record recordInfo ->
            let fields = getRecordFields recordInfo
            StructField(fieldName, fields) :> Field

    let private getRecordFields (recordInfo: RecordInfo) =
        recordInfo.Fields
        |> Array.map (fun field -> getValueSchema field.Name field.ValueInfo)

    let ofRecordInfo recordInfo =
        ParquetSchema(getRecordFields recordInfo)

// TODO: Maybe more efficient to just get rid of Levels type and split into separate fields?
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

[<AbstractClass>]
type private ValueShredder(maxLevels: Levels) =
    member this.MaxLevels = maxLevels
    abstract member AddNull : levels:Levels -> unit
    abstract member ShredValue : parentLevels:Levels * value:obj -> unit
    abstract member BuildColumns : unit -> DataColumn seq

type private AtomicShredder(atomicInfo: AtomicInfo, maxLevels, field: DataField) =
    inherit ValueShredder(maxLevels)

    let repetitionLevelsRequired = maxLevels.Repetition > 0
    let definitionLevelsRequired = maxLevels.Definition > 0

    let mutable valueCount = 0
    let dataValues = ResizeArray()
    let repetitionLevels = ResizeArray()
    let definitionLevels = ResizeArray()

    let incrementValueCount () =
        valueCount <- valueCount + 1

    let addLevels (levels: Levels) =
        if repetitionLevelsRequired then
            repetitionLevels.Add(levels.Repetition)
        if definitionLevelsRequired then
            definitionLevels.Add(levels.Definition)

    let addValue levels value =
        let dataValue = atomicInfo.GetDataValue value
        addLevels levels
        dataValues.Add(dataValue)
        incrementValueCount ()

    override this.AddNull(levels) =
        addLevels levels
        incrementValueCount ()

    override this.ShredValue(parentLevels, value) =
        if atomicInfo.IsOptional
        then
            if atomicInfo.IsNull value
            then
                // Add a NULL value using the parent levels to indicate the last
                // definition level at which a value was present.
                this.AddNull(parentLevels)
            else
                // Add a value at the max definition level for this value. In
                // practice this should be one more than the parent definition
                // level.
                let levels = Levels.create parentLevels.Repetition maxLevels.Definition
                addValue levels value
        else
            // If the value is REQUIRED then the definition level will be the
            // same as the parent, so just add the value using the parent
            // levels.
            addValue parentLevels value

    override this.BuildColumns() =
        let columnData = Array.CreateInstance(atomicInfo.DataDotnetType, dataValues.Count)
        for index in [ 0 .. columnData.Length - 1 ] do
            columnData.SetValue(dataValues[index], index)
        let repetitionLevels =
            if repetitionLevelsRequired
            then Array.ofSeq repetitionLevels
            else null
        let definitionLevels =
            if definitionLevelsRequired
            then Array.ofSeq definitionLevels
            else null
        let column = DataColumn(field, columnData, definitionLevels, repetitionLevels)
        Seq.singleton column

type private ListShredder(listInfo: ListInfo, maxLevels, elementShredder: ValueShredder) =
    inherit ValueShredder(maxLevels)

    let shredElements listLevels list =
        let elementValues = listInfo.GetElementValues list
        // If there are no element values then add a NULL element using the list
        // levels. This indicates that the list is NOTNULL but there are no
        // elements.
        if elementValues.Count = 0
        then elementShredder.AddNull(listLevels)
        else
            // The first element inherits its repetition level from the list,
            // whereas subsequent elements use the max repetition level of the
            // element value, which in practice should be one more than the
            // repetition level of the list. We also need to increment the
            // definition level since we're now inside a REPEATED field that is
            // NOTNULL.
            let elementMaxLevels = elementShredder.MaxLevels
            let definitionLevel = listLevels.Definition + 1
            let firstElementLevels = Levels.create listLevels.Repetition definitionLevel
            let otherElementLevels = Levels.create elementMaxLevels.Repetition definitionLevel
            elementShredder.ShredValue(firstElementLevels, elementValues[0])
            for index in [ 1 .. elementValues.Count - 1 ] do
                elementShredder.ShredValue(otherElementLevels, elementValues[index])

    override this.AddNull(levels) =
        elementShredder.AddNull(levels)

    override this.ShredValue(parentLevels, list) =
        if listInfo.IsOptional
        then
            if listInfo.IsNull list
            then this.AddNull(parentLevels)
            else
                // If the list is OPTIONAL and NOTNULL then update the
                // definition level to the max definition level of the list
                // before shredding the elements.
                let listLevels = Levels.create parentLevels.Repetition maxLevels.Definition
                shredElements listLevels list
        else
            // If the list is REQUIRED then the definition level will be the
            // same as the parent, so use the parent levels when shredding the
            // elements.
            shredElements parentLevels list

    override this.BuildColumns() =
        elementShredder.BuildColumns()

type private RecordShredder(recordInfo: RecordInfo, maxLevels, fieldShredders: ValueShredder[]) =
    inherit ValueShredder(maxLevels)

    let shredFields recordLevels record =
        let fieldValues = recordInfo.GetFieldValues record
        for fieldShredder, fieldValue in Array.zip fieldShredders fieldValues do
            fieldShredder.ShredValue(recordLevels, fieldValue)

    override this.AddNull(levels) =
        for fieldShredder in fieldShredders do
            fieldShredder.AddNull(levels)

    override this.ShredValue(parentLevels, record) =
        // TODO: Potential performance optimisation to have a different shredder
        // implementation for optional and non-optional records/lists/atomics
        // to reduce branching required with if statements.
        if recordInfo.IsOptional
        then
            if recordInfo.IsNull record
            then this.AddNull(parentLevels)
            else
                // If the record is OPTIONAL and NOTNULL then update the
                // definition level to the max definition level of the record
                // before shredding the fields.
                let recordLevels = Levels.create parentLevels.Repetition maxLevels.Definition
                shredFields recordLevels record
        else
            // If the record is REQUIRED then the definition level will be the
            // same as the parent, so use the parent levels when shredding the
            // fields.
            shredFields parentLevels record

    override this.BuildColumns() =
        fieldShredders
        |> Seq.collect _.BuildColumns()

module private ValueShredder =
    let forAtomic (atomicInfo: AtomicInfo) parentMaxLevels dataField =
        let maxLevels =
            if atomicInfo.IsOptional
            then Levels.incrementForOptional parentMaxLevels
            else parentMaxLevels
        AtomicShredder(atomicInfo, maxLevels, dataField)
        :> ValueShredder

    let forList (listInfo: ListInfo) parentMaxLevels (listFIeld: ListField) =
        let listMaxLevels =
            if listInfo.IsOptional
            then Levels.incrementForOptional parentMaxLevels
            else parentMaxLevels
        let elementMaxLevels = Levels.incrementForRepeated listMaxLevels
        let elementShredder = ValueShredder.forValue listInfo.ElementInfo elementMaxLevels listFIeld.Item
        ListShredder(listInfo, listMaxLevels, elementShredder)
        :> ValueShredder

    let forRecord (recordInfo: RecordInfo) parentMaxLevels (fields: Field seq) =
        let maxLevels =
            if recordInfo.IsOptional
            then Levels.incrementForOptional parentMaxLevels
            else parentMaxLevels
        let fieldShredders =
            Seq.zip recordInfo.Fields fields
            |> Seq.map (fun (fieldInfo, field) ->
                ValueShredder.forValue fieldInfo.ValueInfo maxLevels field)
            |> Array.ofSeq
        RecordShredder(recordInfo, maxLevels, fieldShredders)
        :> ValueShredder

    let forValue (valueInfo: ValueInfo) parentMaxLevels (field: Field) =
        match valueInfo with
        | ValueInfo.Atomic atomicInfo ->
            let dataField = field :?> DataField
            ValueShredder.forAtomic atomicInfo parentMaxLevels dataField
        | ValueInfo.List listInfo ->
            let listField = field :?> ListField
            ValueShredder.forList listInfo parentMaxLevels listField
        | ValueInfo.Record recordInfo ->
            let structField = field :?> StructField
            ValueShredder.forRecord recordInfo parentMaxLevels structField.Fields

type Shredder<'Record>() =
    // TODO: Currently only supports F# records but we probably want it to
    // support other type as well, e.g. classes, structs, C# records.
    let recordInfo = RecordInfo.ofRecord typeof<'Record>
    // TODO: The root record is never optional, so update the record info
    // in case this is a nullable record type.
    let recordInfo = { recordInfo with IsOptional = false }
    let schema = Schema.ofRecordInfo recordInfo

    member this.Schema = schema

    member this.Shred(records: 'Record seq) =
        let maxLevels = Levels.Default
        let recordShredder = ValueShredder.forRecord recordInfo maxLevels schema.Fields
        for record in records do
            let parentLevels = Levels.Default
            recordShredder.ShredValue(parentLevels, record)
        recordShredder.BuildColumns()
        |> Array.ofSeq

type private AssembledValue = {
    Levels: Levels
    Value: obj option }

module private AssembledValue =
    let create levels value =
        { AssembledValue.Levels = levels
          AssembledValue.Value = value }

[<AbstractClass>]
type private ValueAssembler(maxLevels: Levels) =
    let mutable peekedValue = Option.None

    member this.MaxLevels = maxLevels

    abstract member TryAssembleNextValue : unit -> AssembledValue option

    member this.TryReadNextValue() =
        let value =
            match peekedValue with
            | Option.Some peekedValue -> peekedValue
            | Option.None -> this.TryPeekNextValue()
        this.SkipPeekedValue()
        value

    member this.TryPeekNextValue() =
        peekedValue <- Option.Some (this.TryAssembleNextValue())
        peekedValue.Value

    member this.SkipPeekedValue() =
        peekedValue <- Option.None

    member this.SkipUndefinedValue() =
        // TODO: This might be inefficient as we may be attempting to assemble
        // values when we know they will be undefined, but it updates the
        // assembler states in a more predictable way.
        this.TryReadNextValue() |> ignore

type private AtomicAssembler(atomicInfo: AtomicInfo, maxLevels, column: DataColumn) =
    inherit ValueAssembler(maxLevels)

    let repetitionLevelsRequired = maxLevels.Repetition > 0
    let definitionLevelsRequired = maxLevels.Definition > 0

    // NULL values are not written to the data array, so keep track of the data
    // index and the value index separately. For optional values, these will be
    // different if there are any NULL values in the column.
    let mutable nextValueIndex = 0
    let mutable nextDataValueIndex = 0

    override this.TryAssembleNextValue() =
        // Determine whether we've reached the end of our column data based on
        // the value index. It's important not to use the data value index
        // because for optional values reaching the end of the data array does
        // not imply we've reached the end of the column as there could be more
        // NULL values in the column.
        if nextValueIndex = column.NumValues
        then Option.None
        else
            let repetitionLevel =
                if repetitionLevelsRequired
                then column.RepetitionLevels[nextValueIndex]
                else 0
            let definitionLevel =
                if definitionLevelsRequired
                then column.DefinitionLevels[nextValueIndex]
                else 0
            let levels = Levels.create repetitionLevel definitionLevel
            let value =
                // NULL values are not written to the data array as they are
                // implied by a definition level that is lower than the maximum.
                // A NULL value means that either this value is NULL or this
                // value is UNDEFINED (due to one of its ancestors being NULL).
                if levels.Definition < maxLevels.Definition
                then
                    // If this value is optional and the definition level is one
                    // less than the maximum definition level then this value is
                    // DEFINED, but NULL.
                    if atomicInfo.IsOptional
                        && levels.Definition = maxLevels.Definition - 1
                    then Option.Some (atomicInfo.CreateNull ())
                    // If the value is not optional, or if it is optional and
                    // the definition level is more than one less than the
                    // maximum definition level then this value is UNDEFINED.
                    else Option.None
                else
                    // If the definition level equals the maximum definition
                    // level then this value is DEFINED and NOTNULL. Get the
                    // next data value and convert it to the correct type.
                    let dataValue = column.DefinedData.GetValue(nextDataValueIndex)
                    nextDataValueIndex <- nextDataValueIndex + 1
                    Option.Some (atomicInfo.CreateFromDataValue dataValue)
            nextValueIndex <- nextValueIndex + 1
            let assembledValue = AssembledValue.create levels value
            Option.Some assembledValue

type private ListAssembler(listInfo: ListInfo, maxLevels, elementAssembler: ValueAssembler) =
    inherit ValueAssembler(maxLevels)

    override this.TryAssembleNextValue() =
        let elementMaxLevels = elementAssembler.MaxLevels
        let elementValues = ResizeArray()
        // If the list is empty, NULL or UNDEFINED (due to one of its ancestors
        // being NULL) then there will be a single UNDEFINED element to indicate
        // it. Determine whether this is the case by reading the next element
        // value.
        match elementAssembler.TryReadNextValue() with
        | Option.None -> Option.None
        | Option.Some nextElement ->
            let list =
                match nextElement.Value with
                // If the next element value is UNDEFINED then either the list
                // is empty, the list is NULL or the list is UNDEFINED (due to
                // one of its ancestors being NULL).
                | Option.None ->
                    // If the definition level of the next element is equal to
                    // the maximum definition level of the list then the list is
                    // DEFINED and empty.
                    if nextElement.Levels.Definition = maxLevels.Definition
                    then Option.Some (listInfo.CreateFromElementValues [||])
                    // If the list is optional and the definition level of the
                    // next element is one less than the maximum definition
                    // level of the list then the list is DEFINED, but NULL.
                    elif listInfo.IsOptional
                        && nextElement.Levels.Definition = maxLevels.Definition - 1
                    then Option.Some (listInfo.CreateNull ())
                    // If the list is not optional, or if it is optional and the
                    // definition level of the next element value is more than
                    // one less than the maximum definition level of the list
                    // then the list is UNDEFINED.
                    else Option.None
                // If the next element value is DEFINED then the list must also
                // be DEFINED.
                | Option.Some nextElementValue ->
                    // Add the element to the list of element values.
                    elementValues.Add(nextElementValue)
                    // The end of the list is indicated by a repetition level
                    // less than the max repetition level of the element values,
                    // so keep reading element values until we find an element
                    // where this is the case. This final element will form part
                    // of another list so we don't want to include it in the
                    // current list. For this reason we iterate by peeking the
                    // next element and only skip it if the repetition level is
                    // equal to the max repetition level of the element values.
                    let mutable allElementsFound = false
                    while not allElementsFound do
                        let nextElementValue =
                            match elementAssembler.TryPeekNextValue() with
                            | Option.None -> Option.None
                            | Option.Some nextElement ->
                                if nextElement.Levels.Repetition < elementMaxLevels.Repetition
                                then Option.None
                                else
                                    elementAssembler.SkipPeekedValue()
                                    Option.Some nextElement.Value.Value
                        match nextElementValue with
                        | Option.None -> allElementsFound <- true
                        | Option.Some nextElementValue -> elementValues.Add(nextElementValue)
                    // Construct the list from the element values.
                    Option.Some (listInfo.CreateFromElementValues (Array.ofSeq elementValues))
            // Return the list along with the levels of the first element. It's
            // important to pass the first element levels back up the chain so
            // that ancestor values can be resolved correctly. In the case where
            // the list is UNDEFINED, the definition level of the first element
            // determines whether ancestors are NULL or UNDEFINED. In the case
            // where the list itself is repeated, the repetition level of the
            // first element indicates the repetition level of the list.
            let assembledList = AssembledValue.create nextElement.Levels list
            Option.Some assembledList

type private RecordAssembler(recordInfo: RecordInfo, maxLevels, fieldAssemblers: ValueAssembler[]) =
    inherit ValueAssembler(maxLevels)

    override this.TryAssembleNextValue() =
        let fieldValues = Array.zeroCreate<obj> fieldAssemblers.Length
        // If the record is NULL or UNDEFINED (due to one of its ancestors being
        // NULL) then all field values will be UNDEFINED. Determine whether this
        // is the case from the first field value.
        match fieldAssemblers[0].TryReadNextValue() with
        | Option.None -> Option.None
        | Option.Some firstField ->
            let record =
                match firstField.Value with
                // If the first field value is UNDEFINED then either the record
                // is NULL or the record is UNDEFINED (due to one of its
                // ancestors being NULL). In either case, the definition level
                // of the first field will be less than the maximum definition
                // level of the record.
                | Option.None ->
                    // If the first field is UNDEFINED then any remaining fields
                    // in the record should also be UNDEFINED. It is important
                    // to skip these UNDEFINED values, otherwise they will be
                    // considered part of the next record.
                    for fieldAssembler in fieldAssemblers[1..] do
                        fieldAssembler.SkipUndefinedValue()
                    // If the record is optional and the definition level of the
                    // first field value is one less than the maximum definition
                    // level of the record then the record is DEFINED, but NULL.
                    if recordInfo.IsOptional
                        && firstField.Levels.Definition = maxLevels.Definition - 1
                    then Option.Some (recordInfo.CreateNull ())
                    // If the record is not optional, or if it is optional and
                    // the definition level of the first field value is more
                    // than one less than the maximum definition level of the
                    // record then the record is UNDEFINED.
                    else Option.None
                // If the first field value is DEFINED then the record must also
                // be DEFINED.
                | Option.Some firstFieldValue ->
                    // Add the first field value to the array of field values.
                    fieldValues[0] <- firstFieldValue
                    // If the first field is DEFINED then any remaining fields
                    // in the record should also be DEFINED. Read their values
                    // and add to the array of field values.
                    for fieldIndex in [ 1 .. fieldAssemblers.Length - 1 ] do
                        let fieldValue = fieldAssemblers[fieldIndex].TryReadNextValue()
                        // Assume there is a next field value and that it is
                        // DEFINED.
                        fieldValues[fieldIndex] <- fieldValue.Value.Value.Value
                    // Construct the record from the field values.
                    Option.Some (recordInfo.CreateFromFieldValues fieldValues)
            // Return the record along with the levels of the first field. It's
            // important to pass the first field levels back up the chain so
            // that ancestor values can be resolved correctly. In the case where
            // the record is UNDEFINED, the definition level of the first field
            // determines whether ancestors are NULL or UNDEFINED. In the case
            // where the record is a repeated value, the repetition level of the
            // first field indicates the repetition level of the record.
            let assembledRecord = AssembledValue.create firstField.Levels record
            Option.Some assembledRecord

module private ValueAssembler =
    let forAtomic (atomicInfo: AtomicInfo) parentMaxLevels (columns: Queue<DataColumn>) =
        let maxLevels =
            if atomicInfo.IsOptional
            then Levels.incrementForOptional parentMaxLevels
            else parentMaxLevels
        let column =
            if columns.Count = 0
            then failwith "no column found for atomic value"
            else columns.Dequeue()
        AtomicAssembler(atomicInfo, maxLevels, column)
        :> ValueAssembler

    let forList (listInfo: ListInfo) parentMaxLevels columns =
        let listMaxLevels =
            if listInfo.IsOptional
            then Levels.incrementForOptional parentMaxLevels
            else parentMaxLevels
        let elementMaxLevels = Levels.incrementForRepeated listMaxLevels
        let elementAssembler =
            ValueAssembler.forValue listInfo.ElementInfo elementMaxLevels columns
        ListAssembler(listInfo, listMaxLevels, elementAssembler)
        :> ValueAssembler

    let forRecord (recordInfo: RecordInfo) parentMaxLevels columns =
        let maxLevels =
            if recordInfo.IsOptional
            then Levels.incrementForOptional parentMaxLevels
            else parentMaxLevels
        let fieldAssemblers =
            recordInfo.Fields
            |> Array.map (fun fieldInfo ->
                ValueAssembler.forValue fieldInfo.ValueInfo maxLevels columns)
        RecordAssembler(recordInfo, maxLevels, fieldAssemblers)
        :> ValueAssembler

    let forValue (valueInfo: ValueInfo) parentMaxLevels columns =
        match valueInfo with
        | ValueInfo.Atomic atomicInfo -> ValueAssembler.forAtomic atomicInfo parentMaxLevels columns
        | ValueInfo.List listInfo -> ValueAssembler.forList listInfo parentMaxLevels columns
        | ValueInfo.Record recordInfo -> ValueAssembler.forRecord recordInfo parentMaxLevels columns

type Assembler<'Record>() =
    // TODO: Currently only supports F# records but we probably want it to
    // support other type as well, e.g. classes, structs, C# records.
    let recordInfo = RecordInfo.ofRecord typeof<'Record>
    // TODO: The root record is never optional, so update the record info
    // in case this is a nullable record type.
    let recordInfo = { recordInfo with IsOptional = false }
    let schema = Schema.ofRecordInfo recordInfo

    member this.Schema = schema
    
    member this.Assemble(columns: DataColumn[]) =
        let maxLevels = Levels.Default
        // TODO: Would this be better as a Dictionary of columns? Relies on being able
        // to look up the correct data field, which may require the reflected schema
        // to match the schema read from the file.
        let columns = Queue(columns)
        let recordAssembler = ValueAssembler.forRecord recordInfo maxLevels columns
        let records = ResizeArray()
        let mutable allRecordsAssembled = false
        while not allRecordsAssembled do
            match recordAssembler.TryReadNextValue() with
            | Option.None -> allRecordsAssembled <- true
            | Option.Some record ->
                match record.Value with
                | Option.None -> failwith "root record should always be defined"
                | Option.Some record -> records.Add(record :?> 'Record)
        Array.ofSeq records
