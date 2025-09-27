namespace rec Parquet.FSharp

open FSharp.Reflection
open System
open System.Collections
open System.Reflection
open System.Text

type ValueInfo =
    | Atomic of AtomicInfo
    | List of ListInfo
    | Record of RecordInfo

type LogicalType =
    | Bool
    | Int32
    | Float64
    | Timestamp of TimestampType
    | String

type TimestampType = {
    IsUtc: bool
    Unit: TimeUnit }

type TimeUnit =
    | Milliseconds
    | Microseconds
    | Nanoseconds

type PrimitiveType =
    | Bool
    | Int32
    | Int64
    | Float64
    | ByteArray

type AtomicInfo = {
    DotnetType: Type
    IsOptional: bool
    LogicalType: LogicalType
    PrimitiveType: PrimitiveType
    ResolvePrimitiveValue: obj -> obj
    FromPrimitiveValue: obj -> obj
    CreateNullValue: unit -> obj }

type ListInfo = {
    DotnetType: Type
    IsOptional: bool
    ElementInfo: ValueInfo
    ResolveValues: obj -> IList }

type FieldInfo = {
    Index: int
    Name: string
    ValueInfo: ValueInfo }

type RecordInfo = {
    DotnetType: Type
    IsOptional: bool
    Fields: FieldInfo[]
    ResolveFieldValues: obj -> obj[]
    CreateValue: obj[] -> obj
    CreateNullValue: unit -> obj }

module private Nullable =
    let preComputeHasValue (dotnetType: Type) =
        let hasValueMethod = dotnetType.GetProperty("HasValue").GetGetMethod()
        fun (nullableObj: obj) -> hasValueMethod.Invoke(nullableObj, [||]) :?> bool

    let preComputeGetValue (dotnetType: Type) =
        let getValueMethod = dotnetType.GetProperty("Value").GetGetMethod()
        fun (nullableObj: obj) -> getValueMethod.Invoke(nullableObj, [||])

    let preComputeCreateValue (dotnetType: Type) =
        let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
        let constructor = dotnetType.GetConstructor([| valueDotnetType |])
        fun (valueObj: obj) -> constructor.Invoke([| valueObj |])

    let createNull = fun () -> null :> obj

module private Option =
    let private getSomeCase dotnetType =
        let unionCases = FSharpType.GetUnionCases(dotnetType)
        unionCases |> Array.find _.Name.Equals("Some")
        
    let private getNoneCase dotnetType =
        let unionCases = FSharpType.GetUnionCases(dotnetType)
        unionCases |> Array.find _.Name.Equals("None")

    let preComputeIsSome dotnetType =
        let someCase = getSomeCase dotnetType
        let getOptionTag = FSharpValue.PreComputeUnionTagReader(dotnetType)
        fun (optionObj: obj) -> getOptionTag optionObj = someCase.Tag

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

    let (|Float64|_|) dotnetType =
        if dotnetType = typeof<float>
        then Option.Some ()
        else Option.None

    let (|String|_|) dotnetType =
        if dotnetType = typeof<string>
        then Option.Some ()
        else Option.None

    let (|DateTimeOffset|_|) dotnetType =
        if dotnetType = typeof<DateTimeOffset>
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

    let (|Record|_|) dotnetType =
        if FSharpType.IsRecord(dotnetType)
        then Option.Some ()
        else Option.None

    let (|Array1d|_|) (dotnetType: Type) =
        if dotnetType.IsArray
            && dotnetType.GetArrayRank() = 1
        then Option.Some ()
        else Option.None

module ValueInfo =
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
        match dotnetType with
        | DotnetType.Bool -> ValueInfo.Atomic AtomicInfo.Bool
        | DotnetType.Int32 -> ValueInfo.Atomic AtomicInfo.Int32
        | DotnetType.Float64 -> ValueInfo.Atomic AtomicInfo.Float64
        | DotnetType.DateTimeOffset -> ValueInfo.Atomic AtomicInfo.DateTimeOffset
        | DotnetType.String -> ValueInfo.Atomic AtomicInfo.String
        | DotnetType.Nullable -> ofNullable dotnetType
        | DotnetType.Option -> ofOption dotnetType
        | DotnetType.Record -> ValueInfo.Record (RecordInfo.ofRecord dotnetType)
        | DotnetType.Array1d -> ValueInfo.List (ListInfo.ofArray1d dotnetType)
        | _ -> failwith $"unsupported type '{dotnetType.FullName}'"

    let of'<'Value> =
        ofType typeof<'Value>

module private AtomicInfo =
    let private create
        dotnetType isOptional logicalType primitiveType
        resolvePrimitiveValue fromPrimitiveValue createNullValue =
        { AtomicInfo.DotnetType = dotnetType
          AtomicInfo.IsOptional = isOptional
          AtomicInfo.LogicalType = logicalType
          AtomicInfo.PrimitiveType = primitiveType
          AtomicInfo.ResolvePrimitiveValue = resolvePrimitiveValue
          AtomicInfo.FromPrimitiveValue = fromPrimitiveValue
          AtomicInfo.CreateNullValue = createNullValue }

    let private ofPrimitive<'Value> logicalType primitiveType =
        let dotnetType = typeof<'Value>
        let isOptional = false
        let resolvePrimitiveValue = id
        let fromPrimitiveValue = id
        let createNullValue () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional logicalType primitiveType
            resolvePrimitiveValue fromPrimitiveValue createNullValue

    let Bool = ofPrimitive<bool> LogicalType.Bool PrimitiveType.Bool
    let Int32 = ofPrimitive<int> LogicalType.Int32 PrimitiveType.Int32
    let Float64 = ofPrimitive<float> LogicalType.Float64 PrimitiveType.Float64

    let DateTimeOffset =
        let dotnetType = typeof<DateTimeOffset>
        let isOptional = false
        let logicalType =
            LogicalType.Timestamp {
                IsUtc = true
                Unit = TimeUnit.Milliseconds }
        let primitiveType = PrimitiveType.Int64
        let resolvePrimitiveValue (valueObj: obj) =
            let value = valueObj :?> DateTimeOffset
            value.ToUnixTimeMilliseconds() :> obj
        let fromPrimitiveValue (primitiveValueObj: obj) =
            let primitiveValue = primitiveValueObj :?> int64
            System.DateTimeOffset.FromUnixTimeMilliseconds(primitiveValue) :> obj
        let createNullValue () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional logicalType primitiveType
            resolvePrimitiveValue fromPrimitiveValue createNullValue

    let String =
        let dotnetType = typeof<string>
        let isOptional = true
        let logicalType = LogicalType.String
        let primitiveType = PrimitiveType.ByteArray
        let resolvePrimitiveValue (valueObj: obj) =
            if isNull valueObj
            then null
            else Encoding.UTF8.GetBytes(valueObj :?> string) :> obj
        let fromPrimitiveValue (primitiveValueObj: obj) =
            Encoding.UTF8.GetString(primitiveValueObj :?> byte[]) :> obj
        let createNullValue = fun () -> null
        create dotnetType isOptional logicalType primitiveType
            resolvePrimitiveValue fromPrimitiveValue createNullValue

    let ofNullable (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let logicalType = valueInfo.LogicalType
        let primitiveType = valueInfo.PrimitiveType
        let resolvePrimitiveValue =
            let hasValue = Nullable.preComputeHasValue dotnetType
            let getValue = Nullable.preComputeGetValue dotnetType
            fun nullableObj ->
                if hasValue nullableObj
                then valueInfo.ResolvePrimitiveValue (getValue nullableObj)
                else null
        let fromPrimitiveValue =
            let createValue = Nullable.preComputeCreateValue dotnetType
            fun primitiveValueObj ->
                let valueObj = valueInfo.FromPrimitiveValue primitiveValueObj
                createValue valueObj
        let createNullValue = Nullable.createNull
        create dotnetType isOptional logicalType primitiveType
            resolvePrimitiveValue fromPrimitiveValue createNullValue

    let ofOption (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let logicalType = valueInfo.LogicalType
        let primitiveType = valueInfo.PrimitiveType
        let resolvePrimitiveValue =
            let isSome = Option.preComputeIsSome dotnetType
            let getValue = Option.preComputeGetValue dotnetType
            fun optionObj ->
                if isSome optionObj
                then valueInfo.ResolvePrimitiveValue (getValue optionObj)
                else null
        let fromPrimitiveValue =
            let createSome = Option.preComputeCreateSome dotnetType
            fun primitiveValueObj ->
                let valueObj = valueInfo.FromPrimitiveValue primitiveValueObj
                createSome valueObj
        let createNullValue = Option.preComputeCreateNone dotnetType
        create dotnetType isOptional logicalType primitiveType
            resolvePrimitiveValue fromPrimitiveValue createNullValue

module private ListInfo =
    let private create dotnetType isOptional elementInfo resolveValues =
        { ListInfo.DotnetType = dotnetType
          ListInfo.IsOptional = isOptional
          ListInfo.ElementInfo = elementInfo
          ListInfo.ResolveValues = resolveValues }

    let ofArray1d (dotnetType: Type) =
        let isOptional = true
        let elementDotnetType = dotnetType.GetElementType()
        let elementInfo = ValueInfo.ofType elementDotnetType
        let resolveValues (arrayObj: obj) =
            // No need to check for null here. The cast will succeed for a null
            // array, we'll just get back a null list, which is what we want.
            arrayObj :?> IList
        create dotnetType isOptional elementInfo resolveValues

    let ofNullable (dotnetType: Type) (listInfo: ListInfo) =
        let isOptional = true
        let elementInfo = listInfo.ElementInfo
        let resolveValues =
            let hasValue = Nullable.preComputeHasValue dotnetType
            let getValue = Nullable.preComputeGetValue dotnetType
            fun nullableObj ->
                if hasValue nullableObj
                then listInfo.ResolveValues (getValue nullableObj)
                else null
        create dotnetType isOptional elementInfo resolveValues

    let ofOption (dotnetType: Type) (listInfo: ListInfo) =
        let isOptional = true
        let elementInfo = listInfo.ElementInfo
        let resolveValues =
            let isSome = Option.preComputeIsSome dotnetType
            let getValue = Option.preComputeGetValue dotnetType
            fun optionObj ->
                if isSome optionObj
                then listInfo.ResolveValues (getValue optionObj)
                else null
        create dotnetType isOptional elementInfo resolveValues

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
        resolveFieldValues createValue createNullValue =
        { RecordInfo.DotnetType = dotnetType
          RecordInfo.IsOptional = isOptional
          RecordInfo.Fields = fields
          RecordInfo.ResolveFieldValues = resolveFieldValues
          RecordInfo.CreateValue = createValue
          RecordInfo.CreateNullValue = createNullValue }

    let ofRecord dotnetType =
        let isOptional = false
        let fields =
            FSharpType.GetRecordFields(dotnetType)
            |> Array.mapi FieldInfo.ofField
        let resolveFieldValues = FSharpValue.PreComputeRecordReader(dotnetType)
        let createValue = FSharpValue.PreComputeRecordConstructor(dotnetType)
        let createNullValue () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional fields
            resolveFieldValues createValue createNullValue

    let ofNullable dotnetType (recordInfo: RecordInfo) =
        let isOptional = true
        let fields = recordInfo.Fields
        let resolveFieldValues =
            let hasValue = Nullable.preComputeHasValue dotnetType
            let getValue = Nullable.preComputeGetValue dotnetType
            fun nullableObj ->
                if hasValue nullableObj
                then recordInfo.ResolveFieldValues (getValue nullableObj)
                else null
        let createValue =
            let createValue = Nullable.preComputeCreateValue dotnetType
            fun (fieldValueObjs: obj[]) ->
                let recordObj = recordInfo.CreateValue fieldValueObjs
                createValue recordObj
        let createNullValue = Nullable.createNull
        create dotnetType isOptional fields
            resolveFieldValues createValue createNullValue

    let ofOption dotnetType (recordInfo: RecordInfo) =
        let isOptional = true
        let fields = recordInfo.Fields
        let resolveFieldValues =
            let isSome = Option.preComputeIsSome dotnetType
            let getValue = Option.preComputeGetValue dotnetType
            fun optionObj ->
                if isSome optionObj
                then recordInfo.ResolveFieldValues (getValue optionObj)
                else null
        let createValue =
            let createSome = Option.preComputeCreateSome dotnetType
            fun (fieldValueObjs: obj[]) ->
                let recordObj = recordInfo.CreateValue fieldValueObjs
                createSome recordObj
        let createNullValue = Option.preComputeCreateNone dotnetType
        create dotnetType isOptional fields
            resolveFieldValues createValue createNullValue
