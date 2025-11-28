namespace Parquet.FSharp

open Parquet.Data
open System
open System.Collections.Generic
open System.Linq.Expressions

[<Struct>]
type AssembledValue<'Value>(exists: bool, repLevel: int, defLevel: int, isDefined: bool, value: 'Value) =
    member this.Exists = exists
    member this.RepLevel = repLevel
    member this.DefLevel = defLevel
    member this.IsDefined = isDefined
    member this.Value = value

[<AbstractClass>]
type private ValueAssembler(dotnetType: Type, maxRepLevel, maxDefLevel) =
    // TODO: Do these need to be members?
    member this.RepLevelsRequired = maxRepLevel > 0
    member this.DefLevelsRequired = maxDefLevel > 0

    member this.NextValueExists = Expression.Variable(typeof<bool>, "nextValueExists")
    member this.NextValueRepLevel = Expression.Variable(typeof<int>, "nextValueRepLevel")
    member this.NextValueDefLevel = Expression.Variable(typeof<int>, "nextValueDefLevel")
    member this.NextValueDefined = Expression.Variable(typeof<bool>, "nextValueDefined")
    member this.NextValue = Expression.Variable(dotnetType, "nextValue")

    abstract member CollectDataColumnVariables : unit -> ParameterExpression seq
    abstract member CollectChildNextValueVariables : unit -> ParameterExpression seq

    member this.CollectNextValueVariables() =
        seq {
            yield! this.CollectChildNextValueVariables()
            yield this.NextValueExists
            if this.RepLevelsRequired then
                yield this.NextValueRepLevel
            if this.DefLevelsRequired then
                yield this.NextValueDefLevel
                yield this.NextValueDefined
            yield this.NextValue
        }

    abstract member AssembleNextValue : LambdaExpression

type private AtomicAssembler(atomicInfo: AtomicInfo, maxRepLevel, maxDefLevel) =
    inherit ValueAssembler(atomicInfo.DotnetType, maxRepLevel, maxDefLevel)

    let dataColumn = Expression.Variable(typeof<DataColumn>, "dataColumn")

    override this.CollectDataColumnVariables() =
        Seq.singleton dataColumn

    override this.CollectChildNextValueVariables() =
        Seq.empty

    override this.AssembleNextValue =
        failwith "not implemented!"

type private ListAssembler(listInfo: ListInfo, maxRepLevel, maxDefLevel, elementAssembler: ValueAssembler) =
    inherit ValueAssembler(listInfo.DotnetType, maxRepLevel, maxDefLevel)

    override this.CollectDataColumnVariables() =
        elementAssembler.CollectDataColumnVariables()

    override this.CollectChildNextValueVariables() =
        elementAssembler.CollectNextValueVariables()

    override this.AssembleNextValue =
        failwith "not implemented!"

type private RecordAssembler(recordInfo: RecordInfo, maxRepLevel, maxDefLevel, fieldAssemblers: ValueAssembler[]) =
    inherit ValueAssembler(recordInfo.DotnetType, maxRepLevel, maxDefLevel)

    override this.CollectDataColumnVariables() =
        fieldAssemblers
        |> Seq.collect _.CollectDataColumnVariables()

    override this.CollectChildNextValueVariables() =
        fieldAssemblers
        |> Seq.collect _.CollectNextValueVariables()

    override this.AssembleNextValue =
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
        let dataColumnVariables = recordAssembler.CollectDataColumnVariables() |> Array.ofSeq
        let nextValueVariables = recordAssembler.CollectNextValueVariables() |> Array.ofSeq
        let records = Expression.Variable(typeof<ResizeArray<'Record>>, "records")
        let record = Expression.Variable(typeof<'Record>, "record")
        let loopBreakLabel = Expression.Label("loopBreak")
        // fun (dataColumns: DataColumn[]) ->
        Expression.Lambda<Func<DataColumn[], 'Record[]>>(
            Expression.Block(
                seq {
                    yield! dataColumnVariables
                    yield! nextValueVariables
                    yield records
                },
                seq {
                    // let dataColumn1 = dataColumns[0]
                    // let dataColumn2 = dataColumns[1]
                    // ...
                    yield! dataColumnVariables
                        |> Array.mapi (fun index dataColumnVariable ->
                            Expression.Assign(
                                dataColumnVariable,
                                Expression.ArrayAccess(dataColumns, Expression.Constant(index)))
                            :> Expression)
                    yield Expression.Assign(records, Expression.New(records.Type))
                    yield Expression.Loop(
                        Expression.Block(
                            [ record ],
                            Expression.Break(loopBreakLabel),
                            Expression.Call(records, "Add", [||], record)),
                        loopBreakLabel)
                    // TODO

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
