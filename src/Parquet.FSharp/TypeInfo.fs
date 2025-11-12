namespace rec Parquet.FSharp

open FSharp.Reflection
open System
open System.Collections
open System.Collections.Generic
open System.Reflection
open System.Text

type ValueInfo =
    | Atomic of AtomicInfo
    | List of ListInfo
    | Record of RecordInfo

type LogicalType =
    | Bool
    | Int32
    | Int64
    | UInt32
    | UInt64
    | Float32
    | Float64
    | Decimal of DecimalType
    | Timestamp of TimestampType
    | String
    | Uuid

type DecimalType = {
    Precision: int
    Scale: int }

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
    | Float32
    | Float64
    | ByteArray
    | FixedLengthByteArray

type AtomicInfo = {
    DotnetType: Type
    IsOptional: bool
    LogicalType: LogicalType
    PrimitiveType: PrimitiveType
    IsNullValue: obj -> bool
    GetPrimitiveValue: obj -> obj
    CreateFromPrimitiveValue: obj -> obj
    CreateNullValue: unit -> obj }

type ListInfo = {
    DotnetType: Type
    IsOptional: bool
    ElementInfo: ValueInfo
    IsNullValue: obj -> bool
    GetElementValues: obj -> IList
    CreateFromElementValues: IList -> obj
    CreateNullValue: unit -> obj }

type FieldInfo = {
    Index: int
    Name: string
    ValueInfo: ValueInfo }

type RecordInfo = {
    DotnetType: Type
    IsOptional: bool
    Fields: FieldInfo[]
    IsNullValue: obj -> bool
    GetFieldValues: obj -> obj[]
    CreateFromFieldValues: obj[] -> obj
    CreateNullValue: unit -> obj }

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
        dotnetType isOptional logicalType primitiveType
        isNullValue getPrimitiveValue createFromPrimitiveValue createNullValue =
        { AtomicInfo.DotnetType = dotnetType
          AtomicInfo.IsOptional = isOptional
          AtomicInfo.LogicalType = logicalType
          AtomicInfo.PrimitiveType = primitiveType
          AtomicInfo.IsNullValue = isNullValue
          AtomicInfo.GetPrimitiveValue = getPrimitiveValue
          AtomicInfo.CreateFromPrimitiveValue = createFromPrimitiveValue
          AtomicInfo.CreateNullValue = createNullValue }

    let private ofPrimitive<'Value> logicalType primitiveType =
        let dotnetType = typeof<'Value>
        let isOptional = false
        let isNullValue = fun _ -> false
        let getPrimitiveValue = id
        let createFromPrimitiveValue = id
        let createNullValue () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional logicalType primitiveType
            isNullValue getPrimitiveValue createFromPrimitiveValue createNullValue

    let Bool = ofPrimitive<bool> LogicalType.Bool PrimitiveType.Bool
    let Int32 = ofPrimitive<int> LogicalType.Int32 PrimitiveType.Int32
    let Int64 = ofPrimitive<int64> LogicalType.Int64 PrimitiveType.Int64
    let Float32 = ofPrimitive<float32> LogicalType.Float32 PrimitiveType.Float32
    let Float64 = ofPrimitive<float> LogicalType.Float64 PrimitiveType.Float64

    let UInt32 =
        let dotnetType = typeof<uint>
        let isOptional = false
        let logicalType = LogicalType.UInt32
        let primitiveType = PrimitiveType.Int32
        let isNullValue = fun _ -> false
        let getPrimitiveValue (valueObj: obj) =
            int (valueObj :?> uint) :> obj
        let createFromPrimitiveValue (primitiveValueObj: obj) =
            uint (primitiveValueObj :?> int) :> obj
        let createNullValue () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional logicalType primitiveType
            isNullValue getPrimitiveValue createFromPrimitiveValue createNullValue

    let UInt64 =
        let dotnetType = typeof<uint64>
        let isOptional = false
        let logicalType = LogicalType.UInt64
        let primitiveType = PrimitiveType.Int64
        let isNullValue = fun _ -> false
        let getPrimitiveValue (valueObj: obj) =
            int64 (valueObj :?> uint64) :> obj
        let createFromPrimitiveValue (primitiveValueObj: obj) =
            uint64 (primitiveValueObj :?> int64) :> obj
        let createNullValue () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional logicalType primitiveType
            isNullValue getPrimitiveValue createFromPrimitiveValue createNullValue

    let Decimal =
        let dotnetType = typeof<decimal>
        let isOptional = false
        // To allow accurate representation of all possible {decimal} values we
        // need to determine the maximum precision and scale. The maximum scale
        // is determined by looking at the smallest value that is greater than
        // zero, which is 1E-28. This value has 28 digits to the right of the
        // decimal place, so a scale of 28 is sufficient to represent all
        // {decimal} values and the precision must be at least 28. The maximum
        // precision is determined by looking at the maximum possible value,
        // which is ~7.9228E28. This value has 29 digits to the left of the
        // decimal place so an additional 29 digits of precision are required to
        // represent all {decimal} values. This gives a total precision of 57,
        // consisting of 29 digits to the right of the decimal place and 28
        // digits to the left.
        let logicalType = LogicalType.Decimal { Precision = 57; Scale = 28 }
        let primitiveType = PrimitiveType.FixedLengthByteArray
        let isNullValue = fun _ -> false
        let getPrimitiveValue (valueObj: obj) =
            let value = valueObj :?> decimal
            // Get the decimal value's scale. The last element of the array
            // returned by the {GetBits} method contains the scale in bits
            // 16-23. Extract these bits by shifting and then using a bitwise
            // AND operation.
            let scale = System.Decimal.GetBits(value)[3] >>> 16 &&& 0b01111111
            // The unscaled integer value is the decimal value multiplied by
            // 10^maxScale = 10^28. We don't want to lose precision by using
            // floating point arithmetic, so we need to calculate this scale
            // factor using bigints. Unfortunately, there is no operator defined
            // for multiplying decimal and bigint values so we can't apply this
            // bigint scale factor to the decimal value unless we first convert
            // it to a bigint. Since the decimal value may contain a fractional
            // component, converting it straight to a bigint would result in
            // truncating the value. To solve this problem we do the scaling in
            // two steps. The first step shifts the decimal value such that
            // there is no fractional component using the scale determined
            // above. The second step converts the shifted decimal value to a
            // bigint and applies the remaining scale.
            let shiftedValue = value * decimal (10I ** scale)
            let unscaledValue = bigint shiftedValue * 10I ** (28 - scale)
            // Get the binary representation of the unscaled value. The
            // {GetBytes} method returns the bytes in little-endian format but
            // the Parquet format specifies that they must be big-endian so
            // reverse the returned byte array.
            let valueBytes = unscaledValue.ToByteArray() |> Array.rev
            // Based on the two's compliment representation, negative numbers
            // should have all pad bits set to one and positive numbers (and
            // zero) should have all pad bits set to zero.
            let padByte = if unscaledValue.Sign < 0 then 0b11111111uy else 0b00000000uy
            let paddingBytes = Array.create (24 - valueBytes.Length) padByte
            Array.append paddingBytes valueBytes :> obj
        let createFromPrimitiveValue (primitiveValueObj: obj) =
            failwith "not implemented!"
        let createNullValue () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional logicalType primitiveType
            isNullValue getPrimitiveValue createFromPrimitiveValue createNullValue

    let DateTimeOffset =
        let dotnetType = typeof<DateTimeOffset>
        let isOptional = false
        let logicalType =
            LogicalType.Timestamp {
                IsUtc = true
                Unit = TimeUnit.Milliseconds }
        let primitiveType = PrimitiveType.Int64
        let isNullValue = fun _ -> false
        let getPrimitiveValue (valueObj: obj) =
            let value = valueObj :?> DateTimeOffset
            value.ToUnixTimeMilliseconds() :> obj
        let createFromPrimitiveValue (primitiveValueObj: obj) =
            let primitiveValue = primitiveValueObj :?> int64
            System.DateTimeOffset.FromUnixTimeMilliseconds(primitiveValue) :> obj
        let createNullValue () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional logicalType primitiveType
            isNullValue getPrimitiveValue createFromPrimitiveValue createNullValue

    let String =
        let dotnetType = typeof<string>
        let isOptional = true
        let logicalType = LogicalType.String
        let primitiveType = PrimitiveType.ByteArray
        let isNullValue = isNull
        let getPrimitiveValue (valueObj: obj) =
            let value = valueObj :?> string
            Encoding.UTF8.GetBytes(value) :> obj
        let createFromPrimitiveValue (primitiveValueObj: obj) =
            let bytes = primitiveValueObj :?> byte[]
            Encoding.UTF8.GetString(bytes) :> obj
        let createNullValue = fun () -> null
        create dotnetType isOptional logicalType primitiveType
            isNullValue getPrimitiveValue createFromPrimitiveValue createNullValue

    let Guid =
        let dotnetType = typeof<Guid>
        let isOptional = false
        let logicalType = LogicalType.Uuid
        let primitiveType = PrimitiveType.FixedLengthByteArray
        let isNullValue = fun _ -> false
        let getPrimitiveValue (valueObj: obj) =
            let value = valueObj :?> Guid
            let littleEndianBytes = value.ToByteArray()
            let bigEndianBytes =
                Array.concat [|
                    Array.rev littleEndianBytes[0..3]
                    Array.rev littleEndianBytes[4..5]
                    Array.rev littleEndianBytes[6..7]
                    littleEndianBytes[8..15] |]
            bigEndianBytes :> obj
        let createFromPrimitiveValue (primitiveValueObj: obj) =
            let bigEndianBytes = primitiveValueObj :?> byte[]
            let littleEndianBytes =
                Array.concat [|
                    Array.rev bigEndianBytes[0..3]
                    Array.rev bigEndianBytes[4..5]
                    Array.rev bigEndianBytes[6..7]
                    bigEndianBytes[8..15] |]
            System.Guid(littleEndianBytes) :> obj
        let createNullValue () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional logicalType primitiveType
            isNullValue getPrimitiveValue createFromPrimitiveValue createNullValue

    let ofNullable (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let logicalType = valueInfo.LogicalType
        let primitiveType = valueInfo.PrimitiveType
        let isNullValue = Nullable.isNull'
        let getPrimitiveValue =
            let getValue = Nullable.preComputeGetValue dotnetType
            fun nullableObj ->
                let valueObj = getValue nullableObj
                valueInfo.GetPrimitiveValue valueObj
        let createFromPrimitiveValue =
            let createValue = Nullable.preComputeCreateValue dotnetType
            fun primitiveValueObj ->
                let valueObj = valueInfo.CreateFromPrimitiveValue primitiveValueObj
                createValue valueObj
        let createNullValue = Nullable.createNull
        create dotnetType isOptional logicalType primitiveType
            isNullValue getPrimitiveValue createFromPrimitiveValue createNullValue

    let ofOption (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let logicalType = valueInfo.LogicalType
        let primitiveType = valueInfo.PrimitiveType
        let isNullValue = Option.preComputeIsNone dotnetType
        let getPrimitiveValue =
            let getValue = Option.preComputeGetValue dotnetType
            fun optionObj ->
                let valueObj = getValue optionObj
                valueInfo.GetPrimitiveValue valueObj
        let createFromPrimitiveValue =
            let createSome = Option.preComputeCreateSome dotnetType
            fun primitiveValueObj ->
                let valueObj = valueInfo.CreateFromPrimitiveValue primitiveValueObj
                createSome valueObj
        let createNullValue = Option.preComputeCreateNone dotnetType
        create dotnetType isOptional logicalType primitiveType
            isNullValue getPrimitiveValue createFromPrimitiveValue createNullValue

module private ListInfo =
    let private create
        dotnetType isOptional elementInfo
        isNullValue getElementValues createFromElementValues createNullValue =
        { ListInfo.DotnetType = dotnetType
          ListInfo.IsOptional = isOptional
          ListInfo.ElementInfo = elementInfo
          ListInfo.IsNullValue = isNullValue
          ListInfo.GetElementValues = getElementValues
          ListInfo.CreateFromElementValues = createFromElementValues
          ListInfo.CreateNullValue = createNullValue }

    let ofArray1d (dotnetType: Type) =
        let isOptional = true
        let elementDotnetType = dotnetType.GetElementType()
        let elementInfo = ValueInfo.ofType elementDotnetType
        let isNullValue = isNull
        let getElementValues (arrayObj: obj) =
            arrayObj :?> IList
        let createFromElementValues (elementValues: IList) =
            let array = Array.CreateInstance(elementDotnetType, elementValues.Count)
            elementValues.CopyTo(array, 0)
            array :> obj
        let createNullValue = fun () -> null
        create dotnetType isOptional elementInfo
            isNullValue getElementValues createFromElementValues createNullValue

    let ofList (dotnetType: Type) =
        let isOptional = false
        let elementDotnetType = dotnetType.GetGenericArguments()[0]
        let elementInfo = ValueInfo.ofType elementDotnetType
        let isNullValue = fun _ -> false
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
        let createNullValue () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional elementInfo
            isNullValue getElementValues createFromElementValues createNullValue

    let ofNullable (dotnetType: Type) (listInfo: ListInfo) =
        let isOptional = true
        let elementInfo = listInfo.ElementInfo
        let isNullValue = Nullable.isNull'
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
        let createNullValue = Nullable.createNull
        create dotnetType isOptional elementInfo
            isNullValue getElementValues createFromElementValues createNullValue

    let ofOption (dotnetType: Type) (listInfo: ListInfo) =
        let isOptional = true
        let elementInfo = listInfo.ElementInfo
        let isNullValue = Option.preComputeIsNone dotnetType
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
        let createNullValue = Option.preComputeCreateNone dotnetType
        create dotnetType isOptional elementInfo
            isNullValue getElementValues createFromElementValues createNullValue

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
        isNullValue getFieldValues createFromFieldValues createNullValue =
        { RecordInfo.DotnetType = dotnetType
          RecordInfo.IsOptional = isOptional
          RecordInfo.Fields = fields
          RecordInfo.IsNullValue = isNullValue
          RecordInfo.GetFieldValues = getFieldValues
          RecordInfo.CreateFromFieldValues = createFromFieldValues
          RecordInfo.CreateNullValue = createNullValue }

    let ofRecord dotnetType =
        let isOptional = false
        let fields =
            FSharpType.GetRecordFields(dotnetType)
            |> Array.mapi FieldInfo.ofField
        let isNullValue = fun _ -> false
        let getFieldValues = FSharpValue.PreComputeRecordReader(dotnetType)
        let createFromFieldValues = FSharpValue.PreComputeRecordConstructor(dotnetType)
        let createNullValue () =
            failwith $"type '{dotnetType.FullName}' is not optional"
        create dotnetType isOptional fields
            isNullValue getFieldValues createFromFieldValues createNullValue

    let ofNullable dotnetType (recordInfo: RecordInfo) =
        let isOptional = true
        let fields = recordInfo.Fields
        let isNullValue = Nullable.isNull'
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
        let createNullValue = Nullable.createNull
        create dotnetType isOptional fields
            isNullValue getFieldValues createFromFieldValues createNullValue

    let ofOption dotnetType (recordInfo: RecordInfo) =
        let isOptional = true
        let fields = recordInfo.Fields
        let isNullValue = Option.preComputeIsNone dotnetType
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
        let createNullValue = Option.preComputeCreateNone dotnetType
        create dotnetType isOptional fields
            isNullValue getFieldValues createFromFieldValues createNullValue
