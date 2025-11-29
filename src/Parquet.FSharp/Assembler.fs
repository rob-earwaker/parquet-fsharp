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

    let mutable currentRepLevel = 0
    let mutable currentDefLevel = 0
    let mutable currentValueIsDefined = true
    let mutable currentDataValue = Unchecked.defaultof<'DataValue>

    let updateCurrentRepLevel =
        if repLevelsRequired
        then fun () -> currentRepLevel <- repLevels[nextValueIndex]
        else fun () -> ()

    let moveNextValue =
        if defLevelsRequired
        then
            fun () ->
                updateCurrentRepLevel ()
                currentDefLevel <- defLevels[nextValueIndex]
                if currentDefLevel < maxDefLevel
                then
                    currentValueIsDefined <- false
                else
                    currentValueIsDefined <- true
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

    member val CurrentRepLevel = currentRepLevel
    member val CurrentDefLevel = currentDefLevel
    member val CurrentValueIsDefined = currentValueIsDefined
    member val CurrentDataValue = currentDataValue

    member this.MoveNext() =
        if nextValueIndex >= valueCount
        then false
        else
            moveNextValue ()
            true

type AssembledValue<'Value>() =
    member val Used = true with get, private set
    member val RepLevel = 0 with get, private set
    member val DefLevel = 0 with get, private set
    member val IsDefined = true with get, private set
    member val Value = Unchecked.defaultof<'Value> with get, private set

    member this.MarkUsed() =
        this.Used <- true

    member this.SetValue(repLevel, defLevel, value) =
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

    member this.MarkCurrentValueUsed =
        Expression.Call(currentValue, "MarkUsed", [||], [||])
        :> Expression

    member this.SetCurrentValueUndefined(repLevel, defLevel) =
        Expression.Call(currentValue, "SetUndefined", [||], repLevel, defLevel)
        :> Expression

    member this.SetCurrentValue(repLevel, defLevel, value) =
        Expression.Call(currentValue, "SetValue", [||], repLevel, defLevel, value)
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
        Expression.Block(
            [ valueAvailable ],
            // let valueAvailable =
            //     not currentValue.Used
            //     || tryAssembleNextValue ()
            Expression.Assign(
                valueAvailable,
                Expression.OrElse(
                    Expression.Not(Expression.Property(currentValue, "Used")),
                    Expression.Invoke(this.TryAssembleNextValue))),
            this.MarkCurrentValueUsed,
            valueAvailable)
        :> Expression

    member this.TryPeekNextValue =
        // There's no need to check whether the current value is used before
        // assembling the next value. The only situation in which a value is
        // peeked is when dealing with repeated elements of a list after the
        // first element. Since the first element must always have a value,
        // regardless of whether that value is DEFINED or UNDEFINED, we must
        // always _read_ the first element of the list. This guarantees that
        // when we peek the second element the current value (first element)
        // must always have been used. Each subsequent element of the list will
        // also be peeked, but only if the previous value was part of the list
        // and therefore already marked as used. The peeked value that indicates
        // we've reached the end of the list, i.e. that is not part of the
        // current list, will be the first element of the next list and will
        // therefore also be _read_ as per above.
        Expression.Invoke(this.TryAssembleNextValue)

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

    override this.CollectColumnEnumeratorVariables() =
        Seq.singleton columnEnumerator

    override this.CollectColumnEnumeratorInitializers() =
        Seq.singleton initializeColumnEnumerator

    override this.CollectChildCurrentValueVariables() =
        Seq.empty

    override this.TryAssembleNextValue =
        failwith "not implemented!"

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
        failwith "not implemented!"

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
        let record = Expression.Variable(typeof<'Record>, "record")
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
                    yield Expression.Loop(
                        // while True do
                        Expression.Block(
                            [ record ],
                            // TODO!!!
                            Expression.Break(loopBreakLabel),
                            // records.Add(record)
                            Expression.Call(records, "Add", [||], record)),
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
