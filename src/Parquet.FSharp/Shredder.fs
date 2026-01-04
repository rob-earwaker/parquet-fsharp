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
    // TODO: Would it be clearer if these were lambda expressions?
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

    override this.CollectColumnBuilderVariables() =
        Seq.singleton columnBuilder

    override this.CollectColumnBuilderInitializers() =
        Seq.singleton initializeColumnBuilder

    override this.AddNull(repLevel, defLevel) =
        Expression.Call(columnBuilder, "AddNull", [ repLevel; defLevel ])

    override this.ShredValue(parentRepLevel, parentDefLevel, value) =
        // The value is REQUIRED. The definition level will be the same as
        // the parent, so just add the value using the parent levels.
        let dataValue = Expression.Variable(atomicInfo.DataDotnetType, "dataValue")
        Expression.Block(
            [ dataValue ],
            Expression.Assign(dataValue, atomicInfo.GetDataValue(value)),
            Expression.Call(
                columnBuilder, "AddDataValue",
                [ parentRepLevel; parentDefLevel; dataValue ]))

type private ListShredder(listInfo: ListInfo, elementMaxRepLevel, elementShredder: ValueShredder) =
    inherit ValueShredder()

    let shredElement =
        let repLevel = Expression.Parameter(typeof<int>, "repLevel")
        let defLevel = Expression.Parameter(typeof<int>, "defLevel")
        let element = Expression.Parameter(listInfo.ElementInfo.DotnetType, "element")
        Expression.Lambda(
            elementShredder.ShredValue(repLevel, defLevel, element),
            "shredElement",
            [ repLevel; defLevel; element ])

    override this.CollectColumnBuilderVariables() =
        elementShredder.CollectColumnBuilderVariables()

    override this.CollectColumnBuilderInitializers() =
        elementShredder.CollectColumnBuilderInitializers()

    override this.AddNull(repLevel, defLevel) =
        elementShredder.AddNull(repLevel, defLevel)

    override this.ShredValue(parentRepLevel, parentDefLevel, list) =
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
            Expression.Assign(elementCount, listInfo.GetLength(list)),
            Expression.IfThenElse(
                // Check if the list is empty.
                Expression.Equal(elementCount, Expression.Constant(0)),
                // The list is empty. Add a NULL element using the list levels.
                // This indicates that the list is NOTNULL but there are no
                // elements.
                elementShredder.AddNull(parentRepLevel, parentDefLevel),
                // The list contains at least one element. The first element
                // inherits its repetition level from the list, whereas
                // subsequent elements use the max repetition level of the
                // element value, which in practice should be one more than the
                // repetition level of the list. We also need to increment the
                // definition level since we're now inside a REPEATED field that
                // is NOTNULL.
                Expression.Block(
                    [ firstElementRepLevel; otherElementRepLevel; elementDefLevel; elementIndex ],
                    Expression.Assign(firstElementRepLevel, parentRepLevel),
                    Expression.Assign(otherElementRepLevel, Expression.Constant(elementMaxRepLevel)),
                    Expression.Assign(elementDefLevel, Expression.Increment(parentDefLevel)),
                    Expression.Invoke(
                        shredElement,
                        firstElementRepLevel,
                        elementDefLevel,
                        listInfo.GetElementValue(list, Expression.Constant(0))),
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
                                    listInfo.GetElementValue(list, elementIndex)),
                                Expression.AddAssign(elementIndex, Expression.Constant(1)))),
                        loopBreakLabel))))

type private RecordShredder(recordInfo: RecordInfo, fieldShredders: ValueShredder[]) =
    inherit ValueShredder()

    let shredFieldLambdas =
        Array.zip recordInfo.Fields fieldShredders
        |> Array.map (fun (fieldInfo, fieldShredder) ->
            let recordRepLevel = Expression.Parameter(typeof<int>, "recordRepLevel")
            let recordDefLevel = Expression.Parameter(typeof<int>, "recordDefLevel")
            let record = Expression.Parameter(recordInfo.DotnetType, "record")
            let fieldValue = Expression.Variable(fieldInfo.ValueInfo.DotnetType, "fieldValue")
            Expression.Lambda(
                Expression.Block(
                    [ fieldValue ],
                    Expression.Assign(fieldValue, fieldInfo.GetValue record),
                    fieldShredder.ShredValue(recordRepLevel, recordDefLevel, fieldValue)),
                "shredField",
                [ recordRepLevel; recordDefLevel; record ]))

    override this.CollectColumnBuilderVariables() =
        fieldShredders
        |> Seq.collect _.CollectColumnBuilderVariables()

    override this.CollectColumnBuilderInitializers() =
        fieldShredders
        |> Seq.collect _.CollectColumnBuilderInitializers()

    override this.AddNull(repLevel, defLevel) =
        Expression.Block(
            fieldShredders
            |> Array.map _.AddNull(repLevel, defLevel))

    override this.ShredValue(parentRepLevel, parentDefLevel, record) =
        // The record is REQUIRED. The definition level will be the same as
        // the parent, so use the parent levels when shredding the fields.
        Expression.Block(
            shredFieldLambdas
            |> Array.map (fun shredField ->
                Expression.Invoke(shredField, parentRepLevel, parentDefLevel, record)
                :> Expression))

type private OptionalShredder(optionalInfo: OptionalInfo, maxDefLevel, valueShredder: ValueShredder) =
    inherit ValueShredder()

    override this.CollectColumnBuilderVariables() =
        valueShredder.CollectColumnBuilderVariables()

    override this.CollectColumnBuilderInitializers() =
        valueShredder.CollectColumnBuilderInitializers()

    override this.AddNull(repLevel, defLevel) =
        valueShredder.AddNull(repLevel, defLevel)

    override this.ShredValue(parentRepLevel, parentDefLevel, optionalValue) =
        let value = Expression.Variable(optionalInfo.ValueInfo.DotnetType, "value")
        Expression.IfThenElse(
            // The value is OPTIONAL, so check for NULL.
            optionalInfo.IsNull(optionalValue),
            // If the value is NULL, add a NULL value using the parent levels to
            // indicate the last definition level at which a value was present.
            this.AddNull(parentRepLevel, parentDefLevel),
            // If the value is NOTNULL, get the value and pass it through to the
            // value shredder at the max definition level. In practice this
            // should be one more than the parent definition level.
            Expression.Block(
                [ value ],
                Expression.Assign(value, optionalInfo.GetValue(optionalValue)),
                valueShredder.ShredValue(
                    parentRepLevel,
                    Expression.Constant(maxDefLevel),
                    value)))

module private rec ValueShredder =
    let forAtomic (atomicInfo: AtomicInfo) parentMaxRepLevel parentMaxDefLevel dataField =
        let maxRepLevel = parentMaxRepLevel
        let maxDefLevel = parentMaxDefLevel
        AtomicShredder(atomicInfo, maxRepLevel, maxDefLevel, dataField)
        :> ValueShredder

    let forList (listInfo: ListInfo) parentMaxRepLevel parentMaxDefLevel (listField: ListField) =
        let listMaxRepLevel = parentMaxRepLevel
        let listMaxDefLevel = parentMaxDefLevel
        let elementMaxRepLevel = listMaxRepLevel + 1
        let elementMaxDefLevel = listMaxDefLevel + 1
        let elementShredder =
            ValueShredder.forValue
                listInfo.ElementInfo elementMaxRepLevel elementMaxDefLevel listField.Item
        ListShredder(listInfo, elementMaxRepLevel, elementShredder)
        :> ValueShredder

    let forRecord (recordInfo: RecordInfo) parentMaxRepLevel parentMaxDefLevel (fields: Field seq) =
        let maxRepLevel = parentMaxRepLevel
        let maxDefLevel = parentMaxDefLevel
        let fieldShredders =
            Seq.zip recordInfo.Fields fields
            |> Seq.map (fun (fieldInfo, field) ->
                ValueShredder.forValue fieldInfo.ValueInfo maxRepLevel maxDefLevel field)
            |> Array.ofSeq
        RecordShredder(recordInfo, fieldShredders)
        :> ValueShredder

    let forOptional (optionalInfo: OptionalInfo) parentMaxRepLevel parentMaxDefLevel field =
        let maxRepLevel = parentMaxRepLevel
        let maxDefLevel = parentMaxDefLevel + 1
        let valueShredder =
            ValueShredder.forValue optionalInfo.ValueInfo maxRepLevel maxDefLevel field
        OptionalShredder(optionalInfo, maxDefLevel, valueShredder)
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
        | ValueInfo.Optional optionalInfo ->
            ValueShredder.forOptional optionalInfo parentMaxRepLevel parentMaxDefLevel field

type private Shredder<'Record>() =
    let recordInfo =
        match ValueInfo.of'<'Record> with
        | ValueInfo.Record recordInfo -> recordInfo
        // TODO: F# records are currently treated as optional for compatability
        // with Parquet.Net, but the root record should never be optional.
        // Unwrap the record info to remove this optionality.
        | ValueInfo.Optional optionalInfo ->
            match optionalInfo.ValueInfo with
            | ValueInfo.Record recordInfo -> recordInfo
            | _ -> failwith $"type {typeof<'Record>.FullName} is not a record"
        | _ -> failwith $"type {typeof<'Record>.FullName} is not a record"

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
                        Expression.Call(records, "GetEnumerator", []))
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
                            Expression.Call(columnBuilder, "Build", [])))
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
