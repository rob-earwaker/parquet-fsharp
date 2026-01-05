namespace Parquet.FSharp

open Parquet.Data
open System
open System.Collections.Generic
open System.Linq.Expressions

type private ColumnEnumerator<'DataValue>(maxRepLevel, maxDefLevel, dataColumn: DataColumn) =
    let repLevelsRequired = maxRepLevel > 0
    let defLevelsRequired = maxDefLevel > 0

    let valueCount = dataColumn.NumValues
    let dataValues = dataColumn.DefinedData :?> 'DataValue[]
    let repLevels = dataColumn.RepetitionLevels
    let defLevels = dataColumn.DefinitionLevels

    // NULL values are not written to the data array, so keep track of the data
    // index and the value index separately. For optional values, these will be
    // different if there are any NULL values in the column.
    let mutable nextValueIndex = 0
    let mutable nextDataValueIndex = 0

    let mutable currentRepLevel = 0
    let mutable currentDefLevel = 0
    let mutable currentDataValue = Unchecked.defaultof<'DataValue>

    let updateCurrentRepLevel =
        if repLevelsRequired
        then fun () -> currentRepLevel <- repLevels[nextValueIndex]
        else fun () -> ()

    let updateCurrentDefLevel =
        if defLevelsRequired
        then fun () -> currentDefLevel <- defLevels[nextValueIndex]
        else fun () -> ()

    let moveNextValue =
        if defLevelsRequired
        then
            fun () ->
                updateCurrentRepLevel ()
                updateCurrentDefLevel ()
                if currentDefLevel >= maxDefLevel then
                    currentDataValue <- dataValues[nextDataValueIndex]
                    nextDataValueIndex <- nextDataValueIndex + 1
                nextValueIndex <- nextValueIndex + 1
        else
            // If the definition levels are not required it means they are
            // always zero, which means the data values are always defined. We
            // can optimize for this case by only using the next value index and
            // never setting the level fields.
            fun () ->
                currentDataValue <- dataValues[nextValueIndex]
                nextValueIndex <- nextValueIndex + 1

    member this.CurrentRepLevel = currentRepLevel
    member this.CurrentDefLevel = currentDefLevel
    member this.CurrentDataValue = currentDataValue

    member this.MoveNext() =
        if nextValueIndex >= valueCount
        then false
        else
            moveNextValue ()
            true

type private AssembledValue<'Value>() =
    member val Used = true with get, private set
    member val RepLevel = 0 with get, private set
    member val DefLevel = 0 with get, private set
    member val Value = Unchecked.defaultof<'Value> with get, private set

    member this.MarkUsed() =
        this.Used <- true

    member this.SetValue(repLevel, defLevel, value) =
        this.Used <- false
        this.RepLevel <- repLevel
        this.DefLevel <- defLevel
        this.Value <- value

    member this.SetUndefined(repLevel, defLevel) =
        this.Used <- false
        this.RepLevel <- repLevel
        this.DefLevel <- defLevel

[<AbstractClass>]
type private ValueAssembler(dotnetType: Type) =
    let currentValue =
        Expression.Variable(
            typedefof<AssembledValue<_>>.MakeGenericType(dotnetType),
            "currentValue")

    let currentValueProperty (propertyName: string) =
        Expression.Property(currentValue, propertyName)
        :> Expression

    member this.CurrentValueRepLevel = currentValueProperty "RepLevel"
    member this.CurrentValueDefLevel = currentValueProperty "DefLevel"
    member this.CurrentValue = currentValueProperty "Value"

    member private this.MarkCurrentValueUsed =
        Expression.Call(currentValue, "MarkUsed", [])

    member this.SetCurrentValueUndefined(repLevel, defLevel) =
        Expression.Call(currentValue, "SetUndefined", [ repLevel; defLevel ])

    member this.SetCurrentValue(repLevel, defLevel, value) =
        Expression.Call(currentValue, "SetValue", [ repLevel; defLevel; value ])

    abstract member CollectColumnEnumeratorVariables : unit -> ParameterExpression seq
    abstract member CollectColumnEnumeratorInitializers : unit -> (Expression -> Expression) seq
    abstract member CollectChildCurrentValueVariables : unit -> ParameterExpression seq

    member this.CollectCurrentValueVariables() =
        seq {
            yield! this.CollectChildCurrentValueVariables()
            yield currentValue
        }

    abstract member TryAssembleNextValue : LambdaExpression

    member this.TryReadNextValue =
        let valueAvailable = Expression.Variable(typeof<bool>, "valueAvailable")
        Expression.Lambda(
            Expression.Block(
                [ valueAvailable ],
                // let valueAvailable = this.TryPeekNextValue()
                Expression.Assign(
                    valueAvailable,
                    Expression.Invoke(this.TryPeekNextValue)),
                // this.CurrentValue.MarkUsed()
                this.MarkCurrentValueUsed,
                // return valueAvailable
                valueAvailable),
            "tryReadNextValue",
            [||])

    member this.TryPeekNextValue =
        Expression.Lambda(
            Expression.OrElse(
                Expression.Not(Expression.Property(currentValue, "Used")),
                Expression.Invoke(this.TryAssembleNextValue)),
            "tryPeekNextValue",
            [||])
        :> Expression

    member this.SkipPeekedValue =
        this.MarkCurrentValueUsed

type private AtomicAssembler(atomicInfo: AtomicInfo, maxRepLevel, maxDefLevel) =
    inherit ValueAssembler(atomicInfo.DotnetType)

    let columnEnumeratorType =
        typedefof<ColumnEnumerator<_>>.MakeGenericType(atomicInfo.DataDotnetType)

    let columnEnumerator = Expression.Variable(columnEnumeratorType, "valueEnumerator")

    let initializeColumnEnumerator (dataColumn: Expression) =
        Expression.Assign(
            columnEnumerator,
            Expression.New(
                columnEnumeratorType.GetConstructor([| typeof<int>; typeof<int>; typeof<DataColumn> |]),
                Expression.Constant(maxRepLevel),
                Expression.Constant(maxDefLevel),
                dataColumn))
        :> Expression

    let columnEnumeratorProperty (propertyName: string) =
        Expression.Property(columnEnumerator, propertyName)
        :> Expression

    let columnEnumeratorMoveNext =
        Expression.Call(columnEnumerator, "MoveNext", [])

    override this.CollectColumnEnumeratorVariables() =
        Seq.singleton columnEnumerator

    override this.CollectColumnEnumeratorInitializers() =
        Seq.singleton initializeColumnEnumerator

    override this.CollectChildCurrentValueVariables() =
        Seq.empty

    override this.TryAssembleNextValue =
        let repLevel = Expression.Variable(typeof<int>, "repLevel")
        let defLevel = Expression.Variable(typeof<int>, "defLevel")
        let dataValue = Expression.Variable(atomicInfo.DataDotnetType, "dataValue")
        let value = Expression.Variable(atomicInfo.DotnetType, "value")
        let returnValue = Expression.Label(typeof<bool>, "valueAvailable")
        // fun () ->
        Expression.Lambda(
            Expression.Block(
                [ repLevel; defLevel; dataValue; value ],
                // if not columnEnumerator.MoveNext() then
                //     return false
                Expression.IfThen(
                    Expression.Not(columnEnumeratorMoveNext),
                    Expression.Return(returnValue, Expression.False)),
                // let repLevel = columnEnumerator.CurrentRepLevel
                // let defLevel = columnEnumerator.CurrentDefLevel
                Expression.Assign(repLevel, columnEnumeratorProperty "CurrentRepLevel"),
                Expression.Assign(defLevel, columnEnumeratorProperty "CurrentDefLevel"),
                Expression.IfThenElse(
                    // if defLevel < maxDefLevel
                    Expression.LessThan(defLevel, Expression.Constant(maxDefLevel)),
                    // then this.CurrentValue.SetUndefined(repLevel, defLevel)
                    this.SetCurrentValueUndefined(repLevel, defLevel),
                    // else
                    Expression.Block(
                        // let dataValue = columnEnumerator.CurrentDataValue
                        Expression.Assign(dataValue, columnEnumeratorProperty "CurrentDataValue"),
                        // let value = atomicInfo.CreateFromDataValue dataValue
                        Expression.Assign(value, atomicInfo.CreateFromDataValue(dataValue)),
                        // this.CurrentValue.SetValue(repLevel, defLevel, value)
                        this.SetCurrentValue(repLevel, defLevel, value))),
                // return true
                Expression.Label(returnValue, Expression.True)),
            "tryAssembleNextValue",
            [||])

type private ListAssembler(listInfo: ListInfo, maxDefLevel, elementMaxRepLevel, elementAssembler: ValueAssembler) =
    inherit ValueAssembler(listInfo.DotnetType)

    override this.CollectColumnEnumeratorVariables() =
        elementAssembler.CollectColumnEnumeratorVariables()

    override this.CollectColumnEnumeratorInitializers() =
        elementAssembler.CollectColumnEnumeratorInitializers()

    override this.CollectChildCurrentValueVariables() =
        elementAssembler.CollectCurrentValueVariables()

    override this.TryAssembleNextValue =
        let firstElementRepLevel = Expression.Variable(typeof<int>, "firstElementRepLevel")
        let firstElementDefLevel = Expression.Variable(typeof<int>, "firstElementDefLevel")
        let elementValues =
            Expression.Variable(
                typedefof<ResizeArray<_>>.MakeGenericType(listInfo.ElementInfo.DotnetType),
                "elementValues")
        let list = Expression.Variable(listInfo.DotnetType, "list")
        let loopBreakLabel = Expression.Label("loopBreak")
        let returnValue = Expression.Label(typeof<bool>, "valueAvailable")
        // fun () ->
        Expression.Lambda(
            Expression.Block(
                [ firstElementRepLevel; firstElementDefLevel; elementValues; list ],
                seq<Expression> {
                    // Assuming we've not reached the end of the data set, there will always be at
                    // least one element value for any given list value. If the list is empty, NULL
                    // or UNDEFINED (due to one of its ancestors being NULL) then there will be a
                    // single UNDEFINED element to indicate it. We therefore start by attempting to
                    // read a single element value. If there is no first element value available
                    // then we've reached the end of the data set and the list itself is not
                    // available, so indicate this by returning false. Otherwise, we determine
                    // whether or not to read more elements based on the definition level of this
                    // first element value.
                    // ---
                    // if not elementAssembler.TryReadNextValue() then
                    //    return false
                    yield Expression.IfThen(
                        Expression.Not(Expression.Invoke(elementAssembler.TryReadNextValue)),
                        Expression.Return(returnValue, Expression.False))
                    // Keep hold of the levels of the first element. We use the definition level of
                    // the first element to determine whether the list is empty, NULL or UNDEFINED.
                    // It's also important to use the first element levels as the list levels so
                    // that ancestor values can be resolved correctly. In the case where the list is
                    // UNDEFINED, the definition level of the first element determines whether
                    // ancestors are NULL or UNDEFINED. In the case where the list itself is
                    // repeated, the repetition level of the first element indicates the repetition
                    // level of the list.
                    // ---
                    // let firstElementRepLevel = elementAssembler.CurrentValue.RepLevel
                    // let firstElementDefLevel = elementAssembler.CurrentValue.DefLevel
                    yield Expression.Assign(firstElementRepLevel, elementAssembler.CurrentValueRepLevel)
                    yield Expression.Assign(firstElementDefLevel, elementAssembler.CurrentValueDefLevel)
                    yield Expression.IfThenElse(
                        // If the definition level of the first element is less than the max
                        // definition level of the list then the list is UNDEFINED.
                        // ---
                        // if firstElementDefLevel < maxDefLevel
                        Expression.LessThan(firstElementDefLevel, Expression.Constant(maxDefLevel)),
                        // then
                        //     this.CurrentValue.SetUndefined(
                        //         firstElementRepLevel,
                        //         firstElementDefLevel)
                        this.SetCurrentValueUndefined(firstElementRepLevel, firstElementDefLevel),
                        // else
                        Expression.IfThenElse(
                            // If the definition level of the first element is equal to the maximum
                            // definition level of the list then the list is DEFINED and empty.
                            // ---
                            // if firstElementDefLevel = maxDefLevel
                            Expression.Equal(firstElementDefLevel, Expression.Constant(maxDefLevel)),
                            // then
                            //     this.CurrentValue.SetValue(
                            //         firstElementRepLevel,
                            //         firstElementDefLevel,
                            //         <empty>)
                            this.SetCurrentValue(
                                firstElementRepLevel,
                                firstElementDefLevel,
                                listInfo.CreateEmpty),
                            // else
                            Expression.Block(
                                // If the list is not UNDEFINED or empty then it has one or more
                                // DEFINED elements, so we need to construct it. Create a new
                                // collection to hold the element values and add the first element.
                                // ---
                                // let elementValues = ResizeArray<'Element>()
                                // elementValues.Add(elementAssembler.CurrentValue.Value)
                                Expression.Assign(elementValues, Expression.New(elementValues.Type)),
                                Expression.Call(elementValues, "Add", [ elementAssembler.CurrentValue ]),
                                // Enumerate subsequent elements until we either reach the end of
                                // the data set or find an element with a repetition level less than
                                // the element maximum repetition level. In the latter case, this
                                // final element will form part of another list so we don't want to
                                // include it in the current list. For this reason we iterate by
                                // peeking the next element and only add it to the collection of
                                // element values if it is part of the current list.
                                Expression.Loop(
                                    // while True do
                                    Expression.IfThenElse(
                                        // if not elementAssembler.TryPeekNextValue()
                                        //     || elementAssembler.CurrentValue.RepLevel < elementMaxRepLevel
                                        Expression.OrElse(
                                            Expression.Not(Expression.Invoke(elementAssembler.TryPeekNextValue)),
                                            Expression.LessThan(
                                                elementAssembler.CurrentValueRepLevel,
                                                Expression.Constant(elementMaxRepLevel))),
                                        // then break
                                        Expression.Break(loopBreakLabel),
                                        // else
                                        //     elementValues.Add(elementAssembler.CurrentValue.Value)
                                        //     elementAssembler.SkipPeekedValue()
                                        Expression.Block(
                                            Expression.Call(
                                                elementValues, "Add",
                                                [ elementAssembler.CurrentValue ]),
                                            elementAssembler.SkipPeekedValue)),
                                    loopBreakLabel),
                                Expression.Assign(list, listInfo.CreateFromElementValues(elementValues)),
                                this.SetCurrentValue(
                                    firstElementRepLevel,
                                    firstElementDefLevel,
                                    list))))
                    // return true
                    yield Expression.Label(returnValue, Expression.True)
                }),
            "tryAssembleNextList",
            [||])

type private RecordAssembler(recordInfo: RecordInfo, maxDefLevel, fieldAssemblers: ValueAssembler[]) =
    inherit ValueAssembler(recordInfo.DotnetType)

    override this.CollectColumnEnumeratorVariables() =
        fieldAssemblers
        |> Seq.collect _.CollectColumnEnumeratorVariables()

    override this.CollectColumnEnumeratorInitializers() =
        fieldAssemblers
        |> Seq.collect _.CollectColumnEnumeratorInitializers()

    override this.CollectChildCurrentValueVariables() =
        fieldAssemblers
        |> Seq.collect _.CollectCurrentValueVariables()

    override this.TryAssembleNextValue =
        let repLevel = Expression.Variable(typeof<int>, "repLevel")
        let defLevel = Expression.Variable(typeof<int>, "defLevel")
        let record = Expression.Variable(recordInfo.DotnetType, "record")
        let returnValue = Expression.Label(typeof<bool>, "valueAvailable")
        // fun () ->
        Expression.Lambda(
            Expression.Block(
                [ repLevel; defLevel; record ],
                seq<Expression> {
                    // if not fieldAssembler1.TryReadNextValue()
                    //     || not fieldAssembler2.TryReadNextValue()
                    //     || ... then
                    //     return false
                    yield Expression.IfThen(
                        fieldAssemblers
                        |> Array.map (fun fieldAssembler ->
                            Expression.Not(Expression.Invoke(fieldAssembler.TryReadNextValue))
                            :> Expression)
                        |> Expression.OrElse,
                        Expression.Return(returnValue, Expression.False))
                    // let repLevel = fieldAssembler1.CurrentValue.RepLevel
                    // let defLevel = fieldAssembler1.CurrentValue.DefLevel
                    yield Expression.Assign(repLevel, fieldAssemblers[0].CurrentValueRepLevel)
                    yield Expression.Assign(defLevel, fieldAssemblers[0].CurrentValueDefLevel)
                    yield Expression.IfThenElse(
                        // if defLevel < maxDefLevel
                        Expression.LessThan(defLevel, Expression.Constant(maxDefLevel)),
                        // then this.CurrentValue.SetUndefined(repLevel, defLevel)
                        this.SetCurrentValueUndefined(repLevel, defLevel),
                        // else
                        Expression.Block(
                            // let record =
                            //     recordInfo.CreateFromFieldValues(
                            //         fieldAssembler1.CurrentValue.Value,
                            //         fieldAssembler2.CurrentValue.Value,
                            //         ...)
                            Expression.Assign(
                                record,
                                fieldAssemblers
                                |> Array.map (fun fieldAssembler -> fieldAssembler.CurrentValue)
                                |> recordInfo.CreateFromFieldValues),
                            // this.CurrentValue.SetValue(repLevel, defLevel, value)
                            this.SetCurrentValue(repLevel, defLevel, record)))
                    // return true
                    yield Expression.Label(returnValue, Expression.True)
                }),
            "tryAssembleNextRecord",
            [||])

type private OptionalAssembler(optionalInfo: OptionalInfo, maxDefLevel, valueAssembler: ValueAssembler) =
    inherit ValueAssembler(optionalInfo.DotnetType)

    override this.CollectColumnEnumeratorVariables() =
        valueAssembler.CollectColumnEnumeratorVariables()

    override this.CollectColumnEnumeratorInitializers() =
        valueAssembler.CollectColumnEnumeratorInitializers()

    override this.CollectChildCurrentValueVariables() =
        valueAssembler.CollectCurrentValueVariables()

    override this.TryAssembleNextValue =
        let repLevel = Expression.Variable(typeof<int>, "repLevel")
        let defLevel = Expression.Variable(typeof<int>, "defLevel")
        let value = Expression.Variable(optionalInfo.ValueInfo.DotnetType, "value")
        let optionalValue = Expression.Variable(optionalInfo.DotnetType, "optionalValue")
        let returnValue = Expression.Label(typeof<bool>, "valueAvailable")
        // fun () ->
        Expression.Lambda(
            Expression.Block(
                [ repLevel; defLevel; value; optionalValue ],
                // if not valueAssembler.TryReadNextValue() then
                //     return false
                Expression.IfThen(
                    Expression.Not(Expression.Invoke(valueAssembler.TryReadNextValue)),
                    Expression.Return(returnValue, Expression.False)),
                // let repLevel = valueAssembler.CurrentValue.RepLevel
                // let defLevel = valueAssembler.CurrentValue.DefLevel
                Expression.Assign(repLevel, valueAssembler.CurrentValueRepLevel),
                Expression.Assign(defLevel, valueAssembler.CurrentValueDefLevel),
                Expression.IfThenElse(
                    // if defLevel < maxDefLevel
                    Expression.LessThan(defLevel, Expression.Constant(maxDefLevel)),
                    // then
                    Expression.IfThenElse(
                        // if defLevel = maxDefLevel - 1
                        Expression.Equal(defLevel, Expression.Constant(maxDefLevel - 1)),
                        // then this.CurrentValue.SetValue(repLevel, defLevel, <null>)
                        this.SetCurrentValue(repLevel, defLevel, optionalInfo.CreateNull),
                        // else this.CurrentValue.SetUndefined(repLevel, defLevel)
                        this.SetCurrentValueUndefined(repLevel, defLevel)),
                    // else
                    Expression.Block(
                        // let value = valueAssembler.CurrentValue.Value
                        Expression.Assign(value, valueAssembler.CurrentValue),
                        // let optionalValue = optionalInfo.CreateFromValue(value)
                        Expression.Assign(optionalValue, optionalInfo.CreateFromValue(value)),
                        // this.CurrentValue.SetValue(repLevel, defLevel, value)
                        this.SetCurrentValue(repLevel, defLevel, optionalValue))),
                // return true
                Expression.Label(returnValue, Expression.True)),
            "tryAssembleNextOptional",
            [||])

module private rec ValueAssembler =
    let forAtomic (atomicInfo: AtomicInfo) parentMaxRepLevel parentMaxDefLevel =
        let maxRepLevel = parentMaxRepLevel
        let maxDefLevel = parentMaxDefLevel
        AtomicAssembler(atomicInfo, maxRepLevel, maxDefLevel)
        :> ValueAssembler

    let forList (listInfo: ListInfo) parentMaxRepLevel parentMaxDefLevel =
        let listMaxRepLevel = parentMaxRepLevel
        let listMaxDefLevel = parentMaxDefLevel
        let elementMaxRepLevel = listMaxRepLevel + 1
        let elementMaxDefLevel = listMaxDefLevel + 1
        let elementAssembler =
            ValueAssembler.forValue
                listInfo.ElementInfo elementMaxRepLevel elementMaxDefLevel
        ListAssembler(listInfo, listMaxDefLevel, elementMaxRepLevel, elementAssembler)
        :> ValueAssembler

    let forRecord (recordInfo: RecordInfo) parentMaxRepLevel parentMaxDefLevel =
        let maxRepLevel = parentMaxRepLevel
        let maxDefLevel = parentMaxDefLevel
        let fieldAssemblers =
            recordInfo.Fields
            |> Array.map (fun fieldInfo ->
                ValueAssembler.forValue fieldInfo.ValueInfo maxRepLevel maxDefLevel)
        RecordAssembler(recordInfo, maxDefLevel, fieldAssemblers)
        :> ValueAssembler

    let forOptional (optionalInfo: OptionalInfo) parentMaxRepLevel parentMaxDefLevel =
        let maxRepLevel = parentMaxRepLevel
        let maxDefLevel = parentMaxDefLevel + 1
        let valueAssembler = ValueAssembler.forValue optionalInfo.ValueInfo maxRepLevel maxDefLevel
        OptionalAssembler(optionalInfo, maxDefLevel, valueAssembler)
        :> ValueAssembler

    let forValue (valueInfo: ValueInfo) parentMaxRepLevel parentMaxDefLevel =
        match valueInfo with
        | ValueInfo.Atomic atomicInfo ->
            ValueAssembler.forAtomic atomicInfo parentMaxRepLevel parentMaxDefLevel
        | ValueInfo.List listInfo ->
            ValueAssembler.forList listInfo parentMaxRepLevel parentMaxDefLevel
        | ValueInfo.Record recordInfo ->
            ValueAssembler.forRecord recordInfo parentMaxRepLevel parentMaxDefLevel
        | ValueInfo.Optional optionalInfo ->
            ValueAssembler.forOptional optionalInfo parentMaxRepLevel parentMaxDefLevel

type internal Assembler<'Record>() =
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

    let recordAssembler =
        let maxRepLevel = 0
        let maxDefLevel = 0
        ValueAssembler.forRecord recordInfo maxRepLevel maxDefLevel

    let assembleExpr =
        let dataColumns = Expression.Parameter(typeof<DataColumn[]>, "dataColumns")
        let columnEnumeratorVariables = recordAssembler.CollectColumnEnumeratorVariables()
        let currentValueVariables = recordAssembler.CollectCurrentValueVariables() |> Array.ofSeq
        let records = Expression.Variable(typeof<ResizeArray<'Record>>, "records")
        let loopBreakLabel = Expression.Label("loopBreak")
        // fun (dataColumns: DataColumn[]) ->
        Expression.Lambda<Func<DataColumn[], 'Record[]>>(
            Expression.Block(
                seq {
                    yield! columnEnumeratorVariables
                    yield! currentValueVariables
                    yield records
                },
                seq {
                    // let columnEnumerator1 = ColumnEnumerator<'DataValue1>(dataColumns[0])
                    // let columnEnumerator2 = ColumnEnumerator<'DataValue2>(dataColumns[1])
                    // ...
                    yield! recordAssembler.CollectColumnEnumeratorInitializers()
                        |> Seq.mapi (fun index initializeColumnEnumerator ->
                            let dataColumn =
                                Expression.ArrayAccess(
                                    dataColumns,
                                    Expression.Constant(index))
                            initializeColumnEnumerator dataColumn)
                    // let currentValue1 = CurrentValue<'Value1>()
                    // let currentValue2 = CurrentValue<'Value2>()
                    // ...
                    yield! currentValueVariables
                        |> Seq.map (fun currentValue ->
                            Expression.Assign(currentValue, Expression.New(currentValue.Type))
                            :> Expression)
                    // let records = ResizeArray<'Record>()
                    yield Expression.Assign(records, Expression.New(records.Type))
                    // while True do
                    yield Expression.Loop(
                            // if recordAssembler.TryReadNextValue()
                            // then records.Add(recordAssembler.CurrentValue)
                            // else break
                            Expression.IfThenElse(
                                Expression.Invoke(recordAssembler.TryReadNextValue),
                                Expression.Call(records, "Add", [ recordAssembler.CurrentValue ]),
                                Expression.Break(loopBreakLabel)),
                        loopBreakLabel)
                    // return records.ToArray()
                    yield Expression.Call(records, "ToArray", [])
                }),
            dataColumns)

    let assemble = assembleExpr.Compile()

    member this.Schema = schema
    
    member this.Assemble(columns) =
        assemble.Invoke(columns)

module private Assembler =
    let private Cache = Dictionary<Type, obj>()

    let private tryGetCached<'Record> =
        lock Cache (fun () ->
            match Cache.TryGetValue(typeof<'Record>) with
            | false, _ -> Option.None
            | true, assembler ->
                Option.Some (assembler :?> Assembler<'Record>))

    let private addToCache<'Record> (assembler: Assembler<'Record>) =
        lock Cache (fun () ->
            Cache[typeof<'Record>] <- assembler)

    let of'<'Record> =
        match tryGetCached<'Record> with
        | Option.Some assembler -> assembler
        | Option.None ->
            let assembler = Assembler<'Record>()
            addToCache assembler
            assembler
