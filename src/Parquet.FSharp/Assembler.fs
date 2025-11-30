namespace Parquet.FSharp

open Parquet.Data
open System
open System.Collections.Generic
open System.Linq.Expressions

type ColumnEnumerator<'DataValue>(maxRepLevel, maxDefLevel, dataColumn: DataColumn) =
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

    member val CurrentRepLevel = 0 with get, private set
    member val CurrentDefLevel = 0 with get, private set
    member val CurrentDataValue = Unchecked.defaultof<'DataValue> with get, private set

    member private this.UpdateCurrentRepLevel =
        if repLevelsRequired
        then fun () -> this.CurrentRepLevel <- repLevels[nextValueIndex]
        else fun () -> ()

    member private this.UpdateCurrentDefLevel =
        if defLevelsRequired
        then fun () -> this.CurrentDefLevel <- defLevels[nextValueIndex]
        else fun () -> ()

    member this.MoveNextValue =
        if defLevelsRequired
        then
            fun () ->
                this.UpdateCurrentRepLevel()
                this.UpdateCurrentDefLevel()
                if this.CurrentDefLevel >= maxDefLevel then
                    this.CurrentDataValue <- dataValues[nextDataValueIndex]
                    nextDataValueIndex <- nextDataValueIndex + 1
                nextValueIndex <- nextValueIndex + 1
        else
            // If the definition levels are not required it means they are
            // always zero, which means the data values are always defined. We
            // can optimize for this case by only using the next value index and
            // never setting the level fields.
            fun () ->
                this.CurrentDataValue <- dataValues[nextValueIndex]
                nextValueIndex <- nextValueIndex + 1

    member this.MoveNext() =
        if nextValueIndex >= valueCount
        then false
        else
            this.MoveNextValue()
            true

type AssembledValue<'Value>() =
    member val Used = true with get, private set
    member val RepLevel = 0 with get, private set
    member val DefLevel = 0 with get, private set
    member val IsDefined = true with get, private set
    member val Value = Unchecked.defaultof<'Value> with get, private set

    member this.MarkUsed() =
        this.Used <- true

    // TODO: Let's just consolidate this back to always set levels and value.
    member this.SetValue(value) =
        this.Used <- false
        this.IsDefined <- true
        this.Value <- value

    member this.SetLevelsAndValue(repLevel, defLevel, value) =
        this.Used <- false
        this.RepLevel <- repLevel
        this.DefLevel <- defLevel
        this.IsDefined <- true
        this.Value <- value

    member this.SetUndefined(repLevel, defLevel) =
        this.Used <- false
        this.RepLevel <- repLevel
        this.DefLevel <- defLevel
        this.IsDefined <- false

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
    member this.CurrentValueIsDefined = currentValueProperty "IsDefined"
    member this.CurrentValue = currentValueProperty "Value"

    member this.MarkCurrentValueUsed =
        Expression.Call(currentValue, "MarkUsed", [||], [||])
        :> Expression

    member this.SetCurrentValueUndefined(repLevel, defLevel) =
        Expression.Call(currentValue, "SetUndefined", [||], repLevel, defLevel)
        :> Expression

    member this.SetCurrentValue(value: Expression) =
        Expression.Call(currentValue, "SetValue", [||], value)
        :> Expression

    member this.SetCurrentLevelsAndValue(repLevel, defLevel, value) =
        Expression.Call(currentValue, "SetLevelsAndValue", [||], repLevel, defLevel, value)
        :> Expression

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
        Expression.Call(columnEnumerator, "MoveNext", [||], [||])

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
                    // then
                    Expression.IfThenElse(
                        // TODO: Use >>> before pseudo code so we can also add normal comments
                        // if atomicInfo.IsOptional
                        //     && defLevel = maxDefLevel - 1
                        Expression.And(
                            Expression.Constant(atomicInfo.IsOptional),
                            Expression.Equal(defLevel, Expression.Constant(maxDefLevel - 1))),
                        // then this.CurrentValue.SetValue(repLevel, defLevel, <null>)
                        this.SetCurrentLevelsAndValue(repLevel, defLevel, atomicInfo.NullExpr),
                        // else this.CurrentValue.SetUndefined(repLevel, defLevel)
                        this.SetCurrentValueUndefined(repLevel, defLevel)),
                    // else
                    Expression.Block(
                        // let dataValue = columnEnumerator.CurrentDataValue
                        Expression.Assign(dataValue, columnEnumeratorProperty "CurrentDataValue"),
                        // let value = atomicInfo.CreateFromDataValue dataValue
                        Expression.Assign(value, atomicInfo.CreateFromDataValueExpr(dataValue)),
                        // this.CurrentValue.SetValue(repLevel, defLevel, value)
                        this.SetCurrentLevelsAndValue(repLevel, defLevel, value))),
                // return true
                Expression.Label(returnValue, Expression.True)),
            "tryAssembleNextValue",
            [||])

type private ListAssembler(listInfo: ListInfo, maxRepLevel, maxDefLevel, elementAssembler: ValueAssembler) =
    inherit ValueAssembler(listInfo.DotnetType)

    override this.CollectColumnEnumeratorVariables() =
        elementAssembler.CollectColumnEnumeratorVariables()

    override this.CollectColumnEnumeratorInitializers() =
        elementAssembler.CollectColumnEnumeratorInitializers()

    override this.CollectChildCurrentValueVariables() =
        elementAssembler.CollectCurrentValueVariables()

    override this.TryAssembleNextValue =
        failwith "not implemented!"

type private RecordAssembler(recordInfo: RecordInfo, maxRepLevel, maxDefLevel, fieldAssemblers: ValueAssembler[]) =
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
                        // then
                        Expression.IfThenElse(
                            // if recordInfo.IsOptional
                            //     && defLevel = maxDefLevel - 1
                            Expression.And(
                                Expression.Constant(recordInfo.IsOptional),
                                Expression.Equal(defLevel, Expression.Constant(maxDefLevel - 1))),
                            // then this.CurrentValue.SetValue(repLevel, defLevel, <null>)
                            this.SetCurrentLevelsAndValue(repLevel, defLevel, recordInfo.NullExpr),
                            // else this.CurrentValue.SetUndefined(repLevel, defLevel)
                            this.SetCurrentValueUndefined(repLevel, defLevel)),
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
                                |> recordInfo.CreateFromFieldValuesExpr),
                            // this.CurrentValue.SetValue(repLevel, defLevel, value)
                            this.SetCurrentLevelsAndValue(repLevel, defLevel, record)))
                    // return true
                    yield Expression.Label(returnValue, Expression.True)
                }),
            "tryAssembleNextRecord",
            [||])

module private rec ValueAssembler =
    let forAtomic (atomicInfo: AtomicInfo) parentMaxRepLevel parentMaxDefLevel =
        let maxRepLevel = parentMaxRepLevel
        let maxDefLevel =
            if atomicInfo.IsOptional
            then parentMaxDefLevel + 1
            else parentMaxDefLevel
        AtomicAssembler(atomicInfo, maxRepLevel, maxDefLevel)
        :> ValueAssembler

    let forList (listInfo: ListInfo) parentMaxRepLevel parentMaxDefLevel =
        let listMaxRepLevel = parentMaxRepLevel
        let listMaxDefLevel =
            if listInfo.IsOptional
            then parentMaxDefLevel + 1
            else parentMaxDefLevel
        let elementMaxRepLevel = listMaxRepLevel + 1
        let elementMaxDefLevel = listMaxDefLevel + 1
        let elementAssembler =
            ValueAssembler.forValue
                listInfo.ElementInfo elementMaxRepLevel elementMaxDefLevel
        ListAssembler(listInfo, listMaxRepLevel, listMaxDefLevel, elementAssembler)
        :> ValueAssembler

    let forRecord (recordInfo: RecordInfo) parentMaxRepLevel parentMaxDefLevel =
        let maxRepLevel = parentMaxRepLevel
        let maxDefLevel =
            if recordInfo.IsOptional
            then parentMaxDefLevel + 1
            else parentMaxDefLevel
        let fieldAssemblers =
            recordInfo.Fields
            |> Array.map (fun fieldInfo ->
                ValueAssembler.forValue fieldInfo.ValueInfo maxRepLevel maxDefLevel)
        RecordAssembler(recordInfo, maxRepLevel, maxDefLevel, fieldAssemblers)
        :> ValueAssembler

    let forValue (valueInfo: ValueInfo) parentMaxRepLevel parentMaxDefLevel =
        match valueInfo with
        | ValueInfo.Atomic atomicInfo ->
            ValueAssembler.forAtomic atomicInfo parentMaxRepLevel parentMaxDefLevel
        | ValueInfo.List listInfo ->
            ValueAssembler.forList listInfo parentMaxRepLevel parentMaxDefLevel
        | ValueInfo.Record recordInfo ->
            ValueAssembler.forRecord recordInfo parentMaxRepLevel parentMaxDefLevel

type internal Assembler<'Record>() =
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
                                Expression.Call(records, "Add", [||], recordAssembler.CurrentValue),
                                Expression.Break(loopBreakLabel)),
                        loopBreakLabel)
                    // return records.ToArray()
                    yield Expression.Call(records, "ToArray", [||], [||])
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
