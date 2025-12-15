namespace Parquet.FSharp

open Parquet.Data
open Parquet.Schema
open System
open System.Collections
open System.Collections.Generic
open System.Linq.Expressions

type private ColumnBuilder<'DataValue>(maxRepLevel, maxDefLevel, field) =
    let repLevelsRequired = maxRepLevel > 0
    let defLevelsRequired = maxDefLevel > 0

    let mutable valueCount = 0
    let dataValues = ResizeArray<'DataValue>()
    let repLevels = ResizeArray<int>()
    let defLevels = ResizeArray<int>()

    let incrementValueCount () =
        valueCount <- valueCount + 1

    let addLevels =
        if repLevelsRequired && defLevelsRequired
        then
            fun repLevel defLevel ->
                repLevels.Add(repLevel)
                defLevels.Add(defLevel)
        elif repLevelsRequired && not defLevelsRequired
        then
            fun repLevel defLevel ->
                repLevels.Add(repLevel)
        elif not repLevelsRequired && defLevelsRequired
        then
            fun repLevel defLevel ->
                defLevels.Add(defLevel)
        else
            fun repLevel defLevel -> ()

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
    abstract member CollectColumnBuilderVariables : unit -> ParameterExpression seq
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

    let columnBuilder = Expression.Variable(columnBuilderType, "columnBuilder")

    let initializeColumnBuilder =
        Expression.Assign(
            columnBuilder,
            Expression.New(
                columnBuilderType.GetConstructor([| typeof<int>; typeof<int>; typeof<DataField> |]),
                Expression.Constant(maxRepLevel),
                Expression.Constant(maxDefLevel),
                Expression.Constant(field)))

    let addValue(repLevel, defLevel, value) =
        let dataValue = Expression.Variable(atomicInfo.DataDotnetType, "dataValue")
        Expression.Block(
            [ dataValue ],
            Expression.Assign(dataValue, atomicInfo.GetDataValue value),
            Expression.Call(columnBuilder, "AddDataValue", [||], repLevel, defLevel, dataValue))

    override this.CollectColumnBuilderVariables() =
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
                atomicInfo.IsNull value,
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

type private ListShredder(listInfo: ListInfo, maxDefLevel, elementMaxRepLevel, elementShredder: ValueShredder) =
    inherit ValueShredder()

    let shredElement =
        let repLevel = Expression.Parameter(typeof<int>, "repLevel")
        let defLevel = Expression.Parameter(typeof<int>, "defLevel")
        let element = Expression.Parameter(listInfo.ElementInfo.DotnetType, "element")
        Expression.Lambda(
            elementShredder.ShredValue(repLevel, defLevel, element),
            "shredElement",
            [ repLevel; defLevel; element ])

    let shredElements(listRepLevel, listDefLevel, list) =
        let elementCount = Expression.Variable(typeof<int>, "elementCount")
        let firstElementRepLevel = Expression.Variable(typeof<int>, "firstElementRepLevel")
        let otherElementRepLevel = Expression.Variable(typeof<int>, "otherElementRepLevel")
        let elementDefLevel = Expression.Variable(typeof<int>, "elementDefLevel")
        let elementIndex = Expression.Variable(typeof<int>, "elementIndex")
        let loopBreakLabel = Expression.Label("loopBreak")
        // TODO: Should probably do this using IEnumerator instead to allow
        // support for sequences and IEnumerables
        Expression.Block(
            [ elementCount ],
            // Get the element count.
            Expression.Assign(elementCount, listInfo.GetLength list),
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
                    Expression.Assign(otherElementRepLevel, Expression.Constant(elementMaxRepLevel)),
                    Expression.Assign(elementDefLevel, Expression.Increment(listDefLevel)),
                    Expression.Invoke(
                        shredElement,
                        firstElementRepLevel,
                        elementDefLevel,
                        listInfo.GetElement(list, Expression.Constant(0))),
                    Expression.Assign(elementIndex, Expression.Constant(1)),
                    Expression.Loop(
                        // while True do
                        Expression.IfThenElse(
                            // if elementIndex = elementCount
                            Expression.Equal(elementIndex, elementCount),
                            // then break
                            Expression.Break(loopBreakLabel),
                            // else
                            //     shredValue(...)
                            //     elementIndex <- elementIndex + 1
                            Expression.Block(
                                Expression.Invoke(
                                    shredElement,
                                    otherElementRepLevel,
                                    elementDefLevel,
                                    listInfo.GetElement(list, elementIndex)),
                                Expression.AddAssign(elementIndex, Expression.Constant(1)))),
                        loopBreakLabel))))

    override this.CollectColumnBuilderVariables() =
        elementShredder.CollectColumnBuilderVariables()

    override this.CollectColumnBuilderInitializers() =
        elementShredder.CollectColumnBuilderInitializers()

    override this.AddNull(repLevel, defLevel) =
        elementShredder.AddNull(repLevel, defLevel)

    override this.ShredValue(parentRepLevel, parentDefLevel, list) =
        if listInfo.IsOptional
        then
            Expression.IfThenElse(
                // The list is OPTIONAL, so check for NULL.
                listInfo.IsNull list,
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

type private RecordShredder(recordInfo: RecordInfo, maxDefLevel, fieldShredders: ValueShredder[]) =
    inherit ValueShredder()

    let shredFieldLambdas =
        Array.zip recordInfo.Fields fieldShredders
        |> Array.map (fun (fieldInfo, fieldShredder) ->
            let recordRepLevel = Expression.Parameter(typeof<int>, "recordRepLevel")
            let recordDefLevel = Expression.Parameter(typeof<int>, "recordDefLevel")
            let recordValue = Expression.Parameter(recordInfo.ValueDotnetType, "recordValue")
            let fieldValue = Expression.Variable(fieldInfo.ValueInfo.DotnetType, "fieldValue")
            Expression.Lambda(
                Expression.Block(
                    [ fieldValue ],
                    Expression.Assign(fieldValue, fieldInfo.GetValue recordValue),
                    fieldShredder.ShredValue(recordRepLevel, recordDefLevel, fieldValue)),
                "shredField",
                [ recordRepLevel; recordDefLevel; recordValue ]))

    let shredFields(recordRepLevel, recordDefLevel, record) =
        let recordValue = Expression.Variable(recordInfo.ValueDotnetType, "recordValue")
        Expression.Block(
            [ recordValue ],
            seq {
                yield Expression.Assign(recordValue, recordInfo.GetValue record) :> Expression
                yield! shredFieldLambdas
                    |> Array.map (fun shredField ->
                        Expression.Invoke(shredField, recordRepLevel, recordDefLevel, recordValue)
                        :> Expression)
            })

    override this.CollectColumnBuilderVariables() =
        fieldShredders
        |> Seq.collect _.CollectColumnBuilderVariables()

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
                recordInfo.IsNull record,
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

module private rec ValueShredder =
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
                listInfo.ElementInfo elementMaxRepLevel elementMaxDefLevel listField.Item
        ListShredder(listInfo, listMaxDefLevel, elementMaxRepLevel, elementShredder)
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
        RecordShredder(recordInfo, maxDefLevel, fieldShredders)
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

type private Shredder<'Record>() =
    // TODO: Currently only supports F# records but we probably want it to
    // support other type as well, e.g. classes, structs, C# records.
    let recordInfo =
        match ValueInfo.of'<'Record> with
        | ValueInfo.Record recordInfo ->
            // Update the root record info to be non-optional. This ensures the
            // definition levels for the schema are set correctly when
            // generating the schema and when initializing the shredder.
            // TODO: If this is an optional record we should check for null before
            // shredding each record and raise an exception if a null record is encountered.
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
        let columnBuilderVariables = recordShredder.CollectColumnBuilderVariables()
        let recordEnumerator = Expression.Variable(typeof<IEnumerator<'Record>>, "recordEnumerator")
        let record = Expression.Variable(typeof<'Record>, "record")
        let enumeratorMoveNextMethod = typeof<IEnumerator>.GetMethod("MoveNext")
        let loopBreakLabel = Expression.Label("loopBreak")
        // fun (records: 'Record seq) ->
        Expression.Lambda<Func<'Record seq, DataColumn[]>>(
            Expression.Block(
                seq {
                    yield! columnBuilderVariables
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
                        columnBuilderVariables
                        |> Seq.map (fun columnBuilder ->
                            Expression.Call(columnBuilder, "Build", [||])
                            :> Expression))
                }),
            records)

    let shred = shredExpr.Compile()

    member this.Schema = schema

    member this.Shred(records) =
        shred.Invoke(records)

module private Shredder =
    let private Cache = Dictionary<Type, obj>()

    let private tryGetCached<'Record> =
        lock Cache (fun () ->
            match Cache.TryGetValue(typeof<'Record>) with
            | false, _ -> Option.None
            | true, shredder ->
                Option.Some (shredder :?> Shredder<'Record>))

    let private addToCache<'Record> (shredder: Shredder<'Record>) =
        lock Cache (fun () ->
            Cache[typeof<'Record>] <- shredder)

    let of'<'Record> =
        match tryGetCached<'Record> with
        | Option.Some shredder -> shredder
        | Option.None ->
            let shredder = Shredder<'Record>()
            addToCache shredder
            shredder
