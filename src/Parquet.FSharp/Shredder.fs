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

type private AtomicShredder(atomicSerializer: AtomicSerializer, maxRepLevel, maxDefLevel, field: DataField) =
    inherit ValueShredder()

    let columnBuilderType =
        typedefof<ColumnBuilder<_>>.MakeGenericType(atomicSerializer.DataDotnetType)

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
        let dataValue = Expression.Variable(atomicSerializer.DataDotnetType, "dataValue")
        Expression.Block(
            [ dataValue ],
            Expression.Assign(dataValue, atomicSerializer.GetDataValue(value)),
            Expression.Call(
                columnBuilder, "AddDataValue",
                [ parentRepLevel; parentDefLevel; dataValue ]))

type private ListShredder(listSerializer: ListSerializer, elementMaxRepLevel, elementShredder: ValueShredder) =
    inherit ValueShredder()

    let shredElement =
        let repLevel = Expression.Parameter(typeof<int>, "repLevel")
        let defLevel = Expression.Parameter(typeof<int>, "defLevel")
        let element = Expression.Parameter(listSerializer.ElementSerializer.DotnetType, "element")
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
        let enumeratorType =
            typedefof<IEnumerator<_>>.MakeGenericType(
                listSerializer.ElementSerializer.DotnetType)
        let enumerator = Expression.Variable(enumeratorType, "enumerator")
        let enumeratorMoveNextMethod = typeof<IEnumerator>.GetMethod("MoveNext")
        let firstElementRepLevel = Expression.Variable(typeof<int>, "firstElementRepLevel")
        let otherElementRepLevel = Expression.Variable(typeof<int>, "otherElementRepLevel")
        let elementDefLevel = Expression.Variable(typeof<int>, "elementDefLevel")
        let loopBreakLabel = Expression.Label("loopBreak")
        Expression.Block(
            [ enumerator ],
            // Get an enumerator for the list.
            Expression.Assign(enumerator, listSerializer.GetEnumerator(list)),
            Expression.IfThenElse(
                // Move to the next (first) value of the enumerator. If this
                // returns false then the list is empty, otherwise the first
                // element will be stored as the enumerator's current value.
                Expression.Not(Expression.Call(enumerator, enumeratorMoveNextMethod)),
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
                    [ firstElementRepLevel; otherElementRepLevel; elementDefLevel ],
                    // let firstElementRepLevel = parentRepLevel
                    // let otherElementRepLevel = elementMaxRepLevel
                    // let elementDefLevel = parentDefLevel + 1
                    Expression.Assign(firstElementRepLevel, parentRepLevel),
                    Expression.Assign(otherElementRepLevel, Expression.Constant(elementMaxRepLevel)),
                    Expression.Assign(elementDefLevel, Expression.Increment(parentDefLevel)),
                    // Shred the first element before continuing to enumerate
                    // subsequent elements.
                    // ---
                    // shredElement(...)
                    Expression.Invoke(
                        shredElement,
                        firstElementRepLevel,
                        elementDefLevel,
                        Expression.Property(enumerator, "Current")),
                    Expression.Loop(
                        // while True do
                        Expression.IfThenElse(
                            // if not enumerator.MoveNext()
                            Expression.Not(Expression.Call(enumerator, enumeratorMoveNextMethod)),
                            // then break
                            Expression.Break(loopBreakLabel),
                            // else shredElement(...)
                            Expression.Invoke(
                                shredElement,
                                otherElementRepLevel,
                                elementDefLevel,
                                Expression.Property(enumerator, "Current"))),
                        loopBreakLabel))))

type private RecordShredder(recordSerializer: RecordSerializer, fieldShredders: ValueShredder[]) =
    inherit ValueShredder()

    let shredFieldLambdas =
        Array.zip recordSerializer.FieldSerializers fieldShredders
        |> Array.map (fun (fieldSerializer, fieldShredder) ->
            let recordRepLevel = Expression.Parameter(typeof<int>, "recordRepLevel")
            let recordDefLevel = Expression.Parameter(typeof<int>, "recordDefLevel")
            let record = Expression.Parameter(recordSerializer.DotnetType, "record")
            let fieldValue = Expression.Variable(fieldSerializer.ValueSerializer.DotnetType, "fieldValue")
            Expression.Lambda(
                Expression.Block(
                    [ fieldValue ],
                    Expression.Assign(fieldValue, fieldSerializer.GetValue record),
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

type private OptionalShredder(optionalSerializer: OptionalSerializer, maxDefLevel, valueShredder: ValueShredder) =
    inherit ValueShredder()

    override this.CollectColumnBuilderVariables() =
        valueShredder.CollectColumnBuilderVariables()

    override this.CollectColumnBuilderInitializers() =
        valueShredder.CollectColumnBuilderInitializers()

    override this.AddNull(repLevel, defLevel) =
        valueShredder.AddNull(repLevel, defLevel)

    override this.ShredValue(parentRepLevel, parentDefLevel, optionalValue) =
        let value = Expression.Variable(optionalSerializer.ValueSerializer.DotnetType, "value")
        Expression.IfThenElse(
            // The value is OPTIONAL, so check for NULL.
            optionalSerializer.IsNull(optionalValue),
            // If the value is NULL, add a NULL value using the parent levels to
            // indicate the last definition level at which a value was present.
            this.AddNull(parentRepLevel, parentDefLevel),
            // If the value is NOTNULL, get the value and pass it through to the
            // value shredder at the max definition level. In practice this
            // should be one more than the parent definition level.
            Expression.Block(
                [ value ],
                Expression.Assign(value, optionalSerializer.GetValue(optionalValue)),
                valueShredder.ShredValue(
                    parentRepLevel,
                    Expression.Constant(maxDefLevel),
                    value)))

module private rec ValueShredder =
    let forAtomic (atomicSerializer: AtomicSerializer) parentMaxRepLevel parentMaxDefLevel dataField =
        let maxRepLevel = parentMaxRepLevel
        let maxDefLevel = parentMaxDefLevel
        AtomicShredder(atomicSerializer, maxRepLevel, maxDefLevel, dataField)
        :> ValueShredder

    let forList (listSerializer: ListSerializer) parentMaxRepLevel parentMaxDefLevel (listField: ListField) =
        let listMaxRepLevel = parentMaxRepLevel
        let listMaxDefLevel = parentMaxDefLevel
        let elementMaxRepLevel = listMaxRepLevel + 1
        let elementMaxDefLevel = listMaxDefLevel + 1
        let elementShredder =
            ValueShredder.forValue
                listSerializer.ElementSerializer elementMaxRepLevel elementMaxDefLevel listField.Item
        ListShredder(listSerializer, elementMaxRepLevel, elementShredder)
        :> ValueShredder

    let forRecord (recordSerializer: RecordSerializer) parentMaxRepLevel parentMaxDefLevel (fields: Field seq) =
        let maxRepLevel = parentMaxRepLevel
        let maxDefLevel = parentMaxDefLevel
        let fieldShredders =
            Seq.zip recordSerializer.FieldSerializers fields
            |> Seq.map (fun (fieldSerializer, field) ->
                ValueShredder.forValue fieldSerializer.ValueSerializer maxRepLevel maxDefLevel field)
            |> Array.ofSeq
        RecordShredder(recordSerializer, fieldShredders)
        :> ValueShredder

    let forOptional (optionalSerializer: OptionalSerializer) parentMaxRepLevel parentMaxDefLevel field =
        let maxRepLevel = parentMaxRepLevel
        let maxDefLevel = parentMaxDefLevel + 1
        let valueShredder =
            ValueShredder.forValue optionalSerializer.ValueSerializer maxRepLevel maxDefLevel field
        OptionalShredder(optionalSerializer, maxDefLevel, valueShredder)
        :> ValueShredder

    let forValue (serializer: Serializer) parentMaxRepLevel parentMaxDefLevel (field: Field) =
        match serializer with
        | Serializer.Atomic atomicSerializer ->
            let dataField = field :?> DataField
            ValueShredder.forAtomic atomicSerializer parentMaxRepLevel parentMaxDefLevel dataField
        | Serializer.List listSerializer ->
            let listField = field :?> ListField
            ValueShredder.forList listSerializer parentMaxRepLevel parentMaxDefLevel listField
        | Serializer.Record recordSerializer ->
            let structField = field :?> StructField
            ValueShredder.forRecord recordSerializer parentMaxRepLevel parentMaxDefLevel structField.Fields
        | Serializer.Optional optionalSerializer ->
            ValueShredder.forOptional optionalSerializer parentMaxRepLevel parentMaxDefLevel field

type private Shredder<'Record>(settings) =
    let recordSerializer =
        match Serializer.resolve typeof<'Record> settings with
        | Serializer.Record recordSerializer -> recordSerializer
        // The root record should never be optional. If it is an optional
        // record, unwrap it to remove this optionality. This ensures that the
        // root defnition level is always zero.
        | Serializer.Optional optionalSerializer ->
            match optionalSerializer.ValueSerializer with
            | Serializer.Record recordSerializer -> recordSerializer
            | _ -> failwith $"type {typeof<'Record>.FullName} is not a record"
        | _ -> failwith $"type {typeof<'Record>.FullName} is not a record"

    let schema = RootSchema.ofValueSchema recordSerializer.Schema
    let parquetNetSchema = RootSchema.toParquetNet schema

    let recordShredder =
        let maxRepLevel = 0
        let maxDefLevel = 0
        ValueShredder.forRecord
            recordSerializer maxRepLevel maxDefLevel parquetNetSchema.Fields

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
    // TODO: We probably won't be able to cache at this level eventually when
    // we introduce settings that control deserialization, since the behaviour
    // will depend on the settings, e.g. which serializers are registered.
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

    let createFor<'Record> settings =
        match tryGetCached<'Record> with
        | Option.Some shredder -> shredder
        | Option.None ->
            let shredder = Shredder<'Record>(settings)
            addToCache shredder
            shredder
