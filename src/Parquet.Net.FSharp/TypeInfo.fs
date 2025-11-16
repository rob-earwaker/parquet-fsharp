namespace rec Parquet.FSharp

open FSharp.Reflection
open System
open System.Collections
open System.Collections.Generic
open System.Reflection

type ValueInfo =
    | Atomic of AtomicInfo
    | List of ListInfo
    | Record of RecordInfo

// TODO: Types supported by Parquet.Net:
//   - bool
//   - int8, int16, int32, int64
//   - uint8, uint16, uint32, uint64
//   - BigInteger
//   - float32, float64
//   - decimal
//   - byte[]
//   - DateTime, DateOnly, TimeOnly
//   - TimeSpan, Interval
//   - string
//   - Guid

type AtomicInfo = {
    DotnetType: Type
    IsOptional: bool
    DataDotnetType: Type
    IsNull: obj -> bool
    GetDataValue: obj -> obj
    CreateFromDataValue: obj -> obj
    CreateNull: unit -> obj }

type ListInfo = {
    DotnetType: Type
    IsOptional: bool
    ElementInfo: ValueInfo
    IsNull: obj -> bool
    GetElementValues: obj -> IList
    CreateFromElementValues: IList -> obj
    CreateNull: unit -> obj }

type FieldInfo = {
    Index: int
    Name: string
    ValueInfo: ValueInfo }

// TODO: Rename Record to Stuct? I think this is the more common Parquet terminology.
type RecordInfo = {
    DotnetType: Type
    IsOptional: bool
    Fields: FieldInfo[]
    IsNull: obj -> bool
    GetFieldValues: obj -> obj[]
    CreateFromFieldValues: obj[] -> obj
    CreateNull: unit -> obj }

module private Nullable =
    let preComputeGetValue (dotnetType: Type) =
        let getValueMethod = dotnetType.GetProperty("Value").GetGetMethod()
        fun (nullableObj: obj) -> getValueMethod.Invoke(nullableObj, [||])

    let preComputeCreateValue (dotnetType: Type) =
        let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
        let constructor = dotnetType.GetConstructor([| valueDotnetType |])
        fun (valueObj: obj) -> constructor.Invoke([| valueObj |])

    let isNull' = isNull

    let createNull = fun () -> null :> obj

module private Option =
    let private getSomeCase dotnetType =
        let unionCases = FSharpType.GetUnionCases(dotnetType)
        unionCases |> Array.find _.Name.Equals("Some")
        
    let private getNoneCase dotnetType =
        let unionCases = FSharpType.GetUnionCases(dotnetType)
        unionCases |> Array.find _.Name.Equals("None")

    let preComputeIsNone dotnetType =
        let noneCase = getNoneCase dotnetType
        let getOptionTag = FSharpValue.PreComputeUnionTagReader(dotnetType)
        fun (optionObj: obj) -> getOptionTag optionObj = noneCase.Tag

    let preComputeGetValue dotnetType =
        let someCase = getSomeCase dotnetType
        let getSomeFields = FSharpValue.PreComputeUnionReader(someCase)
        fun (optionObj: obj) -> Array.head (getSomeFields optionObj)

    let preComputeCreateSome dotnetType =
        let someCase = getSomeCase dotnetType
        let createSome = FSharpValue.PreComputeUnionConstructor(someCase)
        fun (valueObj: obj) -> createSome [| valueObj |]

    let preComputeCreateNone dotnetType =
        let noneCase = getNoneCase dotnetType
        let createNone = FSharpValue.PreComputeUnionConstructor(noneCase)
        fun () -> createNone [||]

module private DotnetType =
    let (|Bool|_|) dotnetType =
        if dotnetType = typeof<bool>
        then Option.Some ()
        else Option.None

    let (|Int32|_|) dotnetType =
        if dotnetType = typeof<int>
        then Option.Some ()
        else Option.None

    let (|Int64|_|) dotnetType =
        if dotnetType = typeof<int64>
        then Option.Some ()
        else Option.None

    let (|UInt32|_|) dotnetType =
        if dotnetType = typeof<uint>
        then Option.Some ()
        else Option.None

    let (|UInt64|_|) dotnetType =
        if dotnetType = typeof<uint64>
        then Option.Some ()
        else Option.None

    let (|Float32|_|) dotnetType =
        if dotnetType = typeof<float32>
        then Option.Some ()
        else Option.None

    let (|Float64|_|) dotnetType =
        if dotnetType = typeof<float>
        then Option.Some ()
        else Option.None

    let (|Decimal|_|) dotnetType =
        if dotnetType = typeof<decimal>
        then Option.Some ()
        else Option.None

    let (|String|_|) dotnetType =
        if dotnetType = typeof<string>
        then Option.Some ()
        else Option.None

    let (|Guid|_|) dotnetType =
        if dotnetType = typeof<Guid>
        then Option.Some ()
        else Option.None

    let (|DateTime|_|) dotnetType =
        if dotnetType = typeof<DateTime>
        then Option.Some ()
        else Option.None

    let (|DateTimeOffset|_|) dotnetType =
        if dotnetType = typeof<DateTimeOffset>
        then Option.Some ()
        else Option.None

    let (|Array1d|_|) (dotnetType: Type) =
        if dotnetType.IsArray
            && dotnetType.GetArrayRank() = 1
        then Option.Some ()
        else Option.None

    let (|List|_|) (dotnetType: Type) =
        if dotnetType.IsGenericType
            && dotnetType.GetGenericTypeDefinition() = typedefof<list<_>>
        then Option.Some ()
        else Option.None

    let (|Record|_|) dotnetType =
        if FSharpType.IsRecord(dotnetType)
        then Option.Some ()
        else Option.None

    let (|Nullable|_|) (dotnetType: Type) =
        if dotnetType.IsGenericType
            && dotnetType.GetGenericTypeDefinition() = typedefof<Nullable<_>>
        then Option.Some ()
        else Option.None

    let (|Option|_|) (dotnetType: Type) =
        if dotnetType.IsGenericType
            && dotnetType.GetGenericTypeDefinition() = typedefof<Option<_>>
        then Option.Some ()
        else Option.None

module ValueInfo =
    let private Cache = Dictionary<Type, ValueInfo>()

    let private tryGetCached dotnetType =
        lock Cache (fun () ->
            match Cache.TryGetValue(dotnetType) with
            | false, _ -> Option.None
            | true, valueInfo -> Option.Some valueInfo)

    let private addToCache dotnetType valueInfo =
        lock Cache (fun () ->
            Cache[dotnetType] <- valueInfo)

    let private ofNullable (dotnetType: Type) =
        let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
        match ValueInfo.ofType valueDotnetType with
        | ValueInfo.Atomic atomicInfo ->
            let nullableAtomicInfo = AtomicInfo.ofNullable dotnetType atomicInfo
            ValueInfo.Atomic nullableAtomicInfo
        | ValueInfo.List listInfo ->
            let nullableListInfo = ListInfo.ofNullable dotnetType listInfo
            ValueInfo.List nullableListInfo
        | ValueInfo.Record recordInfo ->
            let nullableRecordInfo = RecordInfo.ofNullable dotnetType recordInfo
            ValueInfo.Record nullableRecordInfo

    let private ofOption (dotnetType: Type) =
        let valueDotnetType = dotnetType.GetGenericArguments()[0]
        match ValueInfo.ofType valueDotnetType with
        | ValueInfo.Atomic atomicInfo ->
            let atomicOptionInfo = AtomicInfo.ofOption dotnetType atomicInfo
            ValueInfo.Atomic atomicOptionInfo
        | ValueInfo.List listInfo ->
            let listOptionInfo = ListInfo.ofOption dotnetType listInfo
            ValueInfo.List listOptionInfo
        | ValueInfo.Record recordInfo ->
            let recordOptionInfo = RecordInfo.ofOption dotnetType recordInfo
            ValueInfo.Record recordOptionInfo

    let ofType (dotnetType: Type) =
        match tryGetCached dotnetType with
        | Option.Some valueInfo -> valueInfo
        | Option.None ->
            let valueInfo =
                match dotnetType with
                | DotnetType.Bool -> ValueInfo.Atomic AtomicInfo.Bool
                | DotnetType.Int32 -> ValueInfo.Atomic AtomicInfo.Int32
                | DotnetType.Int64 -> ValueInfo.Atomic AtomicInfo.Int64
                | DotnetType.UInt32 -> ValueInfo.Atomic AtomicInfo.UInt32
                | DotnetType.UInt64 -> ValueInfo.Atomic AtomicInfo.UInt64
                | DotnetType.Float32 -> ValueInfo.Atomic AtomicInfo.Float32
                | DotnetType.Float64 -> ValueInfo.Atomic AtomicInfo.Float64
                | DotnetType.Decimal -> ValueInfo.Atomic AtomicInfo.Decimal
                | DotnetType.DateTime -> ValueInfo.Atomic AtomicInfo.DateTime
                | DotnetType.DateTimeOffset -> ValueInfo.Atomic AtomicInfo.DateTimeOffset
                | DotnetType.String -> ValueInfo.Atomic AtomicInfo.String
                | DotnetType.Guid -> ValueInfo.Atomic AtomicInfo.Guid
                | DotnetType.Array1d -> ValueInfo.List (ListInfo.ofArray1d dotnetType)
                | DotnetType.List -> ValueInfo.List (ListInfo.ofList dotnetType)
                | DotnetType.Record -> ValueInfo.Record (RecordInfo.ofRecord dotnetType)
                | DotnetType.Nullable -> ofNullable dotnetType
                | DotnetType.Option -> ofOption dotnetType
                | _ -> failwith $"unsupported type '{dotnetType.FullName}'"
            addToCache dotnetType valueInfo
            valueInfo

    let of'<'Value> =
        ofType typeof<'Value>

module private AtomicInfo =
    let private create
        dotnetType isOptional dataDotnetType
        isNull getDataValue createFromDataValue createNull =
        { AtomicInfo.DotnetType = dotnetType
          AtomicInfo.IsOptional = isOptional
          AtomicInfo.DataDotnetType = dataDotnetType
          AtomicInfo.IsNull = isNull
          AtomicInfo.GetDataValue = getDataValue
          AtomicInfo.CreateFromDataValue = createFromDataValue
          AtomicInfo.CreateNull = createNull }

    let private ofDataType<'Value> =
        let dotnetType = typeof<'Value>
        let isOptional = false
        let dataDotnetType = dotnetType
        let isNull = fun _ -> false
        let getDataValue = id
        let createFromDataValue = id
        let createNull () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional dataDotnetType
            isNull getDataValue createFromDataValue createNull

    let Bool = ofDataType<bool>
    let Int32 = ofDataType<int>
    let Int64 = ofDataType<int64>
    let UInt32 = ofDataType<uint32>
    let UInt64 = ofDataType<uint64>
    let Float32 = ofDataType<float32>
    let Float64 = ofDataType<float>
    let Decimal = ofDataType<decimal>
    let DateTime = ofDataType<DateTime>
    let Guid = ofDataType<Guid>

    let DateTimeOffset =
        let dotnetType = typeof<DateTimeOffset>
        let isOptional = false
        let dataDotnetType = typeof<DateTime>
        let isNull = fun _ -> false
        let getDataValue (valueObj: obj) =
            let dateTimeOffset = valueObj :?> DateTimeOffset
            dateTimeOffset.UtcDateTime :> obj
        let createFromDataValue (dataValueObj: obj) =
            let dateTime = dataValueObj :?> DateTime
            System.DateTimeOffset(dateTime.ToUniversalTime()) :> obj
        let createNull () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional dataDotnetType
            isNull getDataValue createFromDataValue createNull

    let String =
        let dotnetType = typeof<string>
        let isOptional = true
        let dataDotnetType = dotnetType
        let isNull = isNull
        let getDataValue = id
        let createFromDataValue = id
        let createNull = fun () -> null
        create dotnetType isOptional dataDotnetType
            isNull getDataValue createFromDataValue createNull

    let ofNullable (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let dataDotnetType = valueInfo.DataDotnetType
        let isNull = Nullable.isNull'
        let getDataValue =
            let getValue = Nullable.preComputeGetValue dotnetType
            fun nullableValueObj ->
                let valueObj = getValue nullableValueObj
                valueInfo.GetDataValue valueObj
        let createFromDataValue =
            let createValue = Nullable.preComputeCreateValue dotnetType
            fun dataValueObj ->
                let valueObj = valueInfo.CreateFromDataValue dataValueObj
                createValue valueObj
        let createNull = Nullable.createNull
        create dotnetType isOptional dataDotnetType
            isNull getDataValue createFromDataValue createNull

    let ofOption (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let dataDotnetType = valueInfo.DataDotnetType
        let isNull = Option.preComputeIsNone dotnetType
        let getDataValue =
            let getValue = Option.preComputeGetValue dotnetType
            fun optionValueObj ->
                let valueObj = getValue optionValueObj
                valueInfo.GetDataValue valueObj
        let createFromDataValue =
            let createSome = Option.preComputeCreateSome dotnetType
            fun dataValueObj ->
                let valueObj = valueInfo.CreateFromDataValue dataValueObj
                createSome valueObj
        let createNull = Option.preComputeCreateNone dotnetType
        create dotnetType isOptional dataDotnetType
            isNull getDataValue createFromDataValue createNull

module private ListInfo =
    let private create
        dotnetType isOptional elementInfo
        isNull getElementValues createFromElementValues createNull =
        { ListInfo.DotnetType = dotnetType
          ListInfo.IsOptional = isOptional
          ListInfo.ElementInfo = elementInfo
          ListInfo.IsNull = isNull
          ListInfo.GetElementValues = getElementValues
          ListInfo.CreateFromElementValues = createFromElementValues
          ListInfo.CreateNull = createNull }

    let ofArray1d (dotnetType: Type) =
        let isOptional = true
        let elementDotnetType = dotnetType.GetElementType()
        let elementInfo = ValueInfo.ofType elementDotnetType
        let isNull = isNull
        let getElementValues (arrayObj: obj) =
            arrayObj :?> IList
        let createFromElementValues (elementValues: IList) =
            let array = Array.CreateInstance(elementDotnetType, elementValues.Count)
            elementValues.CopyTo(array, 0)
            array :> obj
        let createNull = fun () -> null
        create dotnetType isOptional elementInfo
            isNull getElementValues createFromElementValues createNull

    let ofList (dotnetType: Type) =
        // TODO: F# lists are not nullable by default, however Parquet.Net
        // does not support list fields that aren't nullable and a nullable
        // list field class can't easily be created since some of the relevant
        // properties are internal. For now, treat all lists as optional, even
        // though in practice they will never be null.
        let isOptional = true
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementInfo = ValueInfo.ofType elementDotnetType
        let isNull = fun _ -> false
        let getElementValues (listObj: obj) =
            let arrayList = ArrayList()
            for element in listObj :?> IEnumerable do
                arrayList.Add(element) |> ignore
            arrayList :> IList
        let createFromElementValues =
            let Empty = dotnetType.GetProperty("Empty").GetValue(null)
            let cons = dotnetType.GetMethod("Cons")
            fun (elementValues: IList) ->
                let mutable list = Empty
                for index in [ elementValues.Count - 1 .. -1 .. 0 ] do
                    list <- cons.Invoke(null, [| elementValues[index]; list |])
                list
        let createNull () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional elementInfo
            isNull getElementValues createFromElementValues createNull

    let ofNullable (dotnetType: Type) (listInfo: ListInfo) =
        let isOptional = true
        let elementInfo = listInfo.ElementInfo
        let isNull = Nullable.isNull'
        let getElementValues =
            let getValue = Nullable.preComputeGetValue dotnetType
            fun nullableObj ->
                let valueObj = getValue nullableObj
                listInfo.GetElementValues valueObj
        let createFromElementValues =
            let createValue = Nullable.preComputeCreateValue dotnetType
            fun elementValues ->
                let listObj = listInfo.CreateFromElementValues elementValues
                createValue listObj
        let createNull = Nullable.createNull
        create dotnetType isOptional elementInfo
            isNull getElementValues createFromElementValues createNull

    let ofOption (dotnetType: Type) (listInfo: ListInfo) =
        let isOptional = true
        let elementInfo = listInfo.ElementInfo
        let isNull = Option.preComputeIsNone dotnetType
        let getElementValues =
            let getValue = Option.preComputeGetValue dotnetType
            fun optionObj ->
                let valueObj = getValue optionObj
                listInfo.GetElementValues valueObj
        let createFromElementValues =
            let createSome = Option.preComputeCreateSome dotnetType
            fun elementValues ->
                let listObj = listInfo.CreateFromElementValues elementValues
                createSome listObj
        let createNull = Option.preComputeCreateNone dotnetType
        create dotnetType isOptional elementInfo
            isNull getElementValues createFromElementValues createNull

module private FieldInfo =
    let private create index name valueInfo =
        { FieldInfo.Index = index
          FieldInfo.Name = name
          FieldInfo.ValueInfo = valueInfo }

    let ofField index (field: PropertyInfo) =
        let name = field.Name
        let valueInfo = ValueInfo.ofType field.PropertyType
        create index name valueInfo

module private RecordInfo =
    let private create
        dotnetType isOptional fields
        isNull getFieldValues createFromFieldValues createNull =
        { RecordInfo.DotnetType = dotnetType
          RecordInfo.IsOptional = isOptional
          RecordInfo.Fields = fields
          RecordInfo.IsNull = isNull
          RecordInfo.GetFieldValues = getFieldValues
          RecordInfo.CreateFromFieldValues = createFromFieldValues
          RecordInfo.CreateNull = createNull }

    let ofRecord dotnetType =
        // TODO: F# records are not nullable by default, however Parquet.Net
        // does not support struct fields that aren't nullable and a nullable
        // struct field class can't easily be created since some of the relevant
        // properties are internal. For now, treat all records as optional, even
        // though in practice they will never be null.
        let isOptional = true
        let fields =
            FSharpType.GetRecordFields(dotnetType)
            |> Array.mapi FieldInfo.ofField
        let isNull = fun _ -> false
        let getFieldValues = FSharpValue.PreComputeRecordReader(dotnetType)
        let createFromFieldValues = FSharpValue.PreComputeRecordConstructor(dotnetType)
        let createNull () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional fields
            isNull getFieldValues createFromFieldValues createNull

    let ofNullable dotnetType (recordInfo: RecordInfo) =
        let isOptional = true
        let fields = recordInfo.Fields
        let isNull = Nullable.isNull'
        let getFieldValues =
            let getValue = Nullable.preComputeGetValue dotnetType
            fun nullableObj ->
                let valueObj = getValue nullableObj
                recordInfo.GetFieldValues valueObj
        let createFromFieldValues =
            let createValue = Nullable.preComputeCreateValue dotnetType
            fun (fieldValues: obj[]) ->
                let record = recordInfo.CreateFromFieldValues fieldValues
                createValue record
        let createNull = Nullable.createNull
        create dotnetType isOptional fields
            isNull getFieldValues createFromFieldValues createNull

    let ofOption dotnetType (recordInfo: RecordInfo) =
        let isOptional = true
        let fields = recordInfo.Fields
        let isNull = Option.preComputeIsNone dotnetType
        let getFieldValues =
            let getValue = Option.preComputeGetValue dotnetType
            fun optionObj ->
                let valueObj = getValue optionObj
                recordInfo.GetFieldValues valueObj
        let createFromFieldValues =
            let createSome = Option.preComputeCreateSome dotnetType
            fun (fieldValues: obj[]) ->
                let record = recordInfo.CreateFromFieldValues fieldValues
                createSome record
        let createNull = Option.preComputeCreateNone dotnetType
        create dotnetType isOptional fields
            isNull getFieldValues createFromFieldValues createNull
