module rec Parquet.FSharp.DremelExpr

open Parquet.Data
open Parquet.Schema
open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions

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

type private ColumnBuilder<'DataValue>(maxRepLevel, maxDefLevel, field) =
    let repLevelsRequired = maxRepLevel > 0
    let defLevelsRequired = maxDefLevel > 0

    let mutable valueCount = 0
    let dataValues = ResizeArray<'DataValue>()
    let repLevels = ResizeArray<int>()
    let defLevels = ResizeArray<int>()

    let incrementValueCount () =
        valueCount <- valueCount + 1

    let addLevels repLevel defLevel =
        if repLevelsRequired then
            repLevels.Add(repLevel)
        if defLevelsRequired then
            defLevels.Add(defLevel)

    member this.AddNull(repLevel, defLevel) =
        addLevels repLevel defLevel
        incrementValueCount ()

    member this.AddDataValue(repLevel, defLevel, dataValue) =
        addLevels repLevel defLevel
        dataValues.Add(dataValue) |> ignore
        incrementValueCount ()

    member this.Build() =
        let columnData = dataValues.ToArray()
        let repLevels =
            if repLevelsRequired
            then Array.ofSeq repLevels
            else null
        let defLevels =
            if defLevelsRequired
            then Array.ofSeq defLevels
            else null
        DataColumn(field, columnData, defLevels, repLevels)

[<AbstractClass>]
type private ValueShredder() =
    // TODO: Do we even need these or can they be calculated/inferred while shredding?
    // e.g. for an optional record field that is NUTNULL we know we need to increment defLevel.
    abstract member MaxRepLevel : int
    abstract member MaxDefLevel : int
    abstract member CollectColumnBuilderParams : unit -> ParameterExpression seq
    abstract member CollectColumnBuilderInitializers : unit -> Expression seq
    abstract member AddNull
        : repLevel:Expression * defLevel:Expression
        -> Expression
    abstract member ShredValue
        : parentRepLevel:Expression * parentDefLevel:Expression * value:Expression
        -> Expression

type private AtomicShredder(atomicInfo: AtomicInfo, maxRepLevel, maxDefLevel, field: DataField) =
    inherit ValueShredder()

    let columnBuilderType =
        typedefof<ColumnBuilder<_>>.MakeGenericType(atomicInfo.DataDotnetType)

    let columnBuilder =
        Expression.Parameter(
            columnBuilderType,
            $"columnBuilder_{field.Path.ToString().Replace('/', '_')}")

    let initializeColumnBuilder =
        Expression.Assign(
            columnBuilder,
            Expression.New(
                columnBuilderType.GetConstructor([| typeof<int>; typeof<int>; typeof<DataField> |]),
                Expression.Constant(maxRepLevel),
                Expression.Constant(maxDefLevel),
                Expression.Constant(field)))

    let addValue(repLevel, defLevel, value) =
        let dataValue = atomicInfo.GetDataValueExpr value
        Expression.Call(columnBuilder, "AddDataValue", [||], repLevel, defLevel, dataValue)

    override this.MaxRepLevel = maxRepLevel
    override this.MaxDefLevel = maxDefLevel

    override this.CollectColumnBuilderParams() =
        Seq.singleton columnBuilder

    override this.CollectColumnBuilderInitializers() =
        Seq.singleton initializeColumnBuilder

    override this.AddNull(repLevel, defLevel) =
        Expression.Call(columnBuilder, "AddNull", [||], repLevel, defLevel)

    override this.ShredValue(parentRepLevel, parentDefLevel, value) =
        if atomicInfo.IsOptional
        then
            Expression.IfThenElse(
                // The value is OPTIONAL, so check for NULL.
                atomicInfo.IsNullExpr value,
                // If the value is NULL, add a NULL value using the parent
                // levels to indicate the last definition level at which a value
                // was present.
                this.AddNull(parentRepLevel, parentDefLevel),
                // If the value is NOTNULL, add the value at the max definition
                // level. In practice this should be one more than the parent
                // definition level.
                addValue(parentRepLevel, Expression.Constant(maxDefLevel), value))
        else
            // The value is REQUIRED. The definition level will be the same as
            // the parent, so just add the value using the parent levels.
            addValue(parentRepLevel, parentDefLevel, value)

type private ListShredder(listInfo: ListInfo, maxRepLevel, maxDefLevel, elementShredder: ValueShredder) =
    inherit ValueShredder()

    let shredElements(listRepLevel, listDefLevel, list) =
        let elementCount = Expression.Variable(typeof<int>, "elementCount")
        let firstElementRepLevel = Expression.Variable(typeof<int>, "firstElementRepLevel")
        let otherElementRepLevel = Expression.Variable(typeof<int>, "otherElementRepLevel")
        let elementDefLevel = Expression.Variable(typeof<int>, "elementDefLevel")
        let elementIndex = Expression.Variable(typeof<int>, "elementIndex")
        let loopBreakLabel = Expression.Label("loopBreak")
        Expression.Block(
            [ elementCount ],
            // Get the element count.
            Expression.Assign(elementCount, listInfo.GetLengthExpr list),
            Expression.IfThenElse(
                // Check if the list is empty.
                Expression.Equal(elementCount, Expression.Constant(0)),
                // The list is empty. Add a NULL element using the list levels.
                // This indicates that the list is NOTNULL but there are no
                // elements.
                elementShredder.AddNull(listRepLevel, listDefLevel),
                // The list contains at least one element. The first element
                // inherits its repetition level from the list, whereas
                // subsequent elements use the max repetition level of the
                // element value, which in practice should be one more than the
                // repetition level of the list. We also need to increment the
                // definition level since we're now inside a REPEATED field that
                // is NOTNULL.
                Expression.Block(
                    [ firstElementRepLevel; otherElementRepLevel; elementDefLevel; elementIndex ],
                    Expression.Assign(firstElementRepLevel, listRepLevel),
                    Expression.Assign(otherElementRepLevel, Expression.Constant(elementShredder.MaxRepLevel)),
                    Expression.Assign(elementDefLevel, Expression.Increment(listDefLevel)),
                    elementShredder.ShredValue(
                        firstElementRepLevel,
                        elementDefLevel,
                        listInfo.GetElementExpr(list, Expression.Constant(0))),
                    Expression.Assign(elementIndex, Expression.Constant(1)),
                    Expression.Loop(
                        // while True do
                        Expression.IfThenElse(
                            // if elementIndex = elementCount
                            Expression.Equal(elementIndex, elementCount),
                            // then break
                            Expression.Break(loopBreakLabel),
                            // else
                            //     elementShredder.ShredValue(...)
                            //     elementIndex <- elementIndex + 1
                            Expression.Block(
                                elementShredder.ShredValue(
                                    otherElementRepLevel,
                                    elementDefLevel,
                                    listInfo.GetElementExpr(list, elementIndex)),
                                Expression.AddAssign(elementIndex, Expression.Constant(1)))),
                        loopBreakLabel))))

    override this.MaxRepLevel = maxRepLevel
    override this.MaxDefLevel = maxDefLevel

    override this.CollectColumnBuilderParams() =
        elementShredder.CollectColumnBuilderParams()

    override this.CollectColumnBuilderInitializers() =
        elementShredder.CollectColumnBuilderInitializers()

    override this.AddNull(repLevel, defLevel) =
        elementShredder.AddNull(repLevel, defLevel)

    override this.ShredValue(parentRepLevel, parentDefLevel, list) =
        if listInfo.IsOptional
        then
            Expression.IfThenElse(
                // The list is OPTIONAL, so check for NULL.
                listInfo.IsNullExpr list,
                // If the list is NULL, add a NULL value using the parent levels
                // to indicate the last definition level at which a value was
                // present.
                this.AddNull(parentRepLevel, parentDefLevel),
                // If the record is NOTNULL, update the definition level to the
                // max definition level of the list before shredding the
                // elements.
                shredElements(parentRepLevel, Expression.Constant(maxDefLevel), list))
        else
            // The list is REQUIRED. The definition level will be the same as
            // the parent, so use the parent levels when shredding the elements.
            shredElements(parentRepLevel, parentDefLevel, list)

type private RecordShredder(recordInfo: RecordInfo, maxRepLevel, maxDefLevel, fieldShredders: ValueShredder[]) =
    inherit ValueShredder()

    let shredFields(recordRepLevel, recordDefLevel, record) =
        let recordValue = Expression.Variable(recordInfo.ValueDotnetType, "recordValue")
        let fieldValues =
            recordInfo.Fields
            |> Array.map (fun fieldInfo ->
                Expression.Variable(fieldInfo.DotnetType, $"fieldValue_{fieldInfo.Name}"))
        Expression.Block(
            seq {
                yield recordValue
                yield! fieldValues
            },
            seq {
                yield Expression.Assign(recordValue, recordInfo.GetValueExpr record) :> Expression
                yield! Array.zip3 recordInfo.Fields fieldShredders fieldValues
                    |> Array.collect (fun (fieldInfo, fieldShredder, fieldValue) ->
                        [| Expression.Assign(fieldValue, fieldInfo.GetValueExpr recordValue) :> Expression
                           fieldShredder.ShredValue(recordRepLevel, recordDefLevel, fieldValue) |])
            })

    override this.MaxRepLevel = maxRepLevel
    override this.MaxDefLevel = maxDefLevel

    override this.CollectColumnBuilderParams() =
        fieldShredders
        |> Seq.collect _.CollectColumnBuilderParams()

    override this.CollectColumnBuilderInitializers() =
        fieldShredders
        |> Seq.collect _.CollectColumnBuilderInitializers()

    override this.AddNull(repLevel, defLevel) =
        // TODO: Slight improvement to debug readability of expression by only
        // using a block if necessary. There must be a better way of combining
        // blocks up the tree!
        match fieldShredders with
        | [| fieldShredder |] ->
            fieldShredder.AddNull(repLevel, defLevel)
        | _ ->
            Expression.Block(
                fieldShredders
                |> Array.map _.AddNull(repLevel, defLevel))

    override this.ShredValue(parentRepLevel, parentDefLevel, record) =
        if recordInfo.IsOptional
        then
            Expression.IfThenElse(
                // The record is OPTIONAL, so check for NULL.
                recordInfo.IsNullExpr record,
                // If the record is NULL, add a NULL value using the parent
                // levels to indicate the last definition level at which a value
                // was present.
                this.AddNull(parentRepLevel, parentDefLevel),
                // If the record is NOTNULL, update the definition level to the
                // max definition level of the record before shredding the
                // fields.
                shredFields(parentRepLevel, Expression.Constant(maxDefLevel), record))
        else
            // The record is REQUIRED. The definition level will be the same as
            // the parent, so use the parent levels when shredding the fields.
            shredFields(parentRepLevel, parentDefLevel, record)

module private ValueShredder =
    let forAtomic (atomicInfo: AtomicInfo) parentMaxRepLevel parentMaxDefLevel dataField =
        let maxRepLevel = parentMaxRepLevel
        let maxDefLevel =
            if atomicInfo.IsOptional
            then parentMaxDefLevel + 1
            else parentMaxDefLevel
        AtomicShredder(atomicInfo, maxRepLevel, maxDefLevel, dataField)
        :> ValueShredder

    let forList (listInfo: ListInfo) parentMaxRepLevel parentMaxDefLevel (listField: ListField) =
        let listMaxRepLevel = parentMaxRepLevel
        let listMaxDefLevel =
            if listInfo.IsOptional
            then parentMaxDefLevel + 1
            else parentMaxDefLevel
        let elementMaxRepLevel = listMaxRepLevel + 1
        let elementMaxDefLevel = listMaxDefLevel + 1
        let elementShredder =
            ValueShredder.forValue
                listInfo.ElementInfo
                elementMaxRepLevel
                elementMaxDefLevel
                listField.Item
        ListShredder(listInfo, listMaxRepLevel, listMaxDefLevel, elementShredder)
        :> ValueShredder

    let forRecord (recordInfo: RecordInfo) parentMaxRepLevel parentMaxDefLevel (fields: Field seq) =
        let maxRepLevel = parentMaxRepLevel
        let maxDefLevel =
            if recordInfo.IsOptional
            then parentMaxDefLevel + 1
            else parentMaxDefLevel
        let fieldShredders =
            Seq.zip recordInfo.Fields fields
            |> Seq.map (fun (fieldInfo, field) ->
                ValueShredder.forValue fieldInfo.ValueInfo maxRepLevel maxDefLevel field)
            |> Array.ofSeq
        RecordShredder(recordInfo, maxRepLevel, maxDefLevel, fieldShredders)
        :> ValueShredder

    let forValue (valueInfo: ValueInfo) parentMaxRepLevel parentMaxDefLevel (field: Field) =
        match valueInfo with
        | ValueInfo.Atomic atomicInfo ->
            let dataField = field :?> DataField
            ValueShredder.forAtomic atomicInfo parentMaxRepLevel parentMaxDefLevel dataField
        | ValueInfo.List listInfo ->
            let listField = field :?> ListField
            ValueShredder.forList listInfo parentMaxRepLevel parentMaxDefLevel listField
        | ValueInfo.Record recordInfo ->
            let structField = field :?> StructField
            ValueShredder.forRecord recordInfo parentMaxRepLevel parentMaxDefLevel structField.Fields


// TODO: Make this private or internal.
type Shredder<'Record>() =
    // TODO: Currently only supports F# records but we probably want it to
    // support other type as well, e.g. classes, structs, C# records.
    let recordInfo =
        match ValueInfo.of'<'Record> with
        | ValueInfo.Record recordInfo ->
            // TODO: The root record is never optional, so update the record info
            // in case this is a nullable record type.
            { recordInfo with IsOptional = false }
        | _ ->
            failwith $"type {typeof<'Record>.FullName} is not a record"

    let schema = Schema.ofRecordInfo recordInfo

    let recordShredder =
        let maxRepLevel = 0
        let maxDefLevel = 0
        ValueShredder.forRecord
            recordInfo maxRepLevel maxDefLevel schema.Fields

    let shredExpr =
        let records = Expression.Parameter(typeof<'Record seq>, "records")
        let columnBuilderParams = recordShredder.CollectColumnBuilderParams()
        let recordEnumerator = Expression.Variable(typeof<IEnumerator<'Record>>, "recordEnumerator")
        let record = Expression.Variable(typeof<'Record>, "record")
        let enumeratorMoveNextMethod = typeof<IEnumerator>.GetMethod("MoveNext")
        let loopBreakLabel = Expression.Label("loopBreak")
        // fun (records: 'Record seq) ->
        Expression.Lambda<Func<'Record seq, DataColumn[]>>(
            Expression.Block(
                seq {
                    yield! columnBuilderParams
                    yield recordEnumerator
                },
                seq {
                    // let columnBuilder1 = ColumnBuilder<'DataValue1>()
                    // let columnBuilder2 = ColumnBuilder<'DataValue2>()
                    // ...
                    yield! recordShredder.CollectColumnBuilderInitializers()
                    // let recordEnumerator = records.GetEnumerator()
                    yield Expression.Assign(
                        recordEnumerator,
                        Expression.Call(records, "GetEnumerator", [||], [||]))
                    yield Expression.Loop(
                        // while True do
                        Expression.IfThenElse(
                            // if not (recordEnumerator.MoveNext())
                            Expression.Not(
                                Expression.Call(recordEnumerator, enumeratorMoveNextMethod)),
                            // then break
                            Expression.Break(loopBreakLabel),
                            // else
                            Expression.Block(
                                [ record ],
                                // let record = recordEnumerator.Current
                                Expression.Assign(
                                    record,
                                    Expression.Property(recordEnumerator, "Current")),
                                // recordShredder.ShredValue(0, 0, record)
                                recordShredder.ShredValue(
                                    parentRepLevel = Expression.Constant(0),
                                    parentDefLevel = Expression.Constant(0),
                                    value = record))),
                        loopBreakLabel)
                    // return [|
                    //     columnBuilder1.Build()
                    //     columnBuilder2.Build()
                    //     ...
                    // |]
                    yield Expression.NewArrayInit(
                        typeof<DataColumn>,
                        columnBuilderParams
                        |> Seq.map (fun columnBuilder ->
                            Expression.Call(columnBuilder, "Build", [||])
                            :> Expression))
                }),
            records)

    let shred = shredExpr.Compile()

    member this.Schema = schema

    member this.Shred(records: 'Record seq) =
        shred.Invoke(records)
