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
    ConvertValueToPrimitive: obj -> obj }

type ListInfo = {
    DotnetType: Type
    IsOptional: bool
    ElementInfo: ValueInfo
    GetValues: obj -> IList }

type FieldInfo = {
    Name: string
    ValueInfo: ValueInfo
    GetValue: obj -> obj }

type RecordInfo = {
    DotnetType: Type
    IsOptional: bool
    Fields: FieldInfo[] }

module private Nullable =
    let preComputeGetValue (dotnetType: Type) =
        let getValueMethod = dotnetType.GetProperty("Value").GetGetMethod()
        fun (nullableValue: obj) -> getValueMethod.Invoke(nullableValue, [||])

module private Option =
    let preComputeIsNone dotnetType =
        let unionCases = FSharpType.GetUnionCases(dotnetType)
        let noneCase = unionCases |> Array.find _.Name.Equals("None")
        let getOptionTag = FSharpValue.PreComputeUnionTagReader(dotnetType)
        fun (valueOption: obj) -> getOptionTag valueOption = noneCase.Tag

    let preComputeGetValue dotnetType =
        let unionCases = FSharpType.GetUnionCases(dotnetType)
        let someCase = unionCases |> Array.find _.Name.Equals("Some")
        let getSomeFields = FSharpValue.PreComputeUnionReader(someCase)
        fun (valueOption: obj) -> Array.head (getSomeFields valueOption)

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
        dotnetType isOptional logicalType primitiveType convertValueToPrimitive =
        { AtomicInfo.DotnetType = dotnetType
          AtomicInfo.IsOptional = isOptional
          AtomicInfo.LogicalType = logicalType
          AtomicInfo.PrimitiveType = primitiveType
          AtomicInfo.ConvertValueToPrimitive = convertValueToPrimitive }

    let private ofPrimitive<'Value> logicalType primitiveType =
        let dotnetType = typeof<'Value>
        let isOptional = false
        let convertValueToPrimitive = id
        create dotnetType isOptional logicalType primitiveType convertValueToPrimitive

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
        let convertValueToPrimitive (dateTimeOffsetValue: obj) =
            let dateTimeOffset = dateTimeOffsetValue :?> DateTimeOffset
            box (dateTimeOffset.ToUnixTimeMilliseconds())
        create dotnetType isOptional logicalType primitiveType convertValueToPrimitive

    let String =
        let dotnetType = typeof<string>
        let isOptional = true
        let logicalType = LogicalType.String
        let primitiveType = PrimitiveType.ByteArray
        let convertValueToPrimitive (stringValue: obj) =
            if isNull stringValue
            then null
            else box (Encoding.UTF8.GetBytes(stringValue :?> string))
        create dotnetType isOptional logicalType primitiveType convertValueToPrimitive

    let ofNullable (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let logicalType = valueInfo.LogicalType
        let primitiveType = valueInfo.PrimitiveType
        let getValue = Nullable.preComputeGetValue dotnetType
        let convertValueToPrimitive nullableValue =
            if isNull nullableValue
            then null
            else valueInfo.ConvertValueToPrimitive (getValue nullableValue)
        create dotnetType isOptional logicalType primitiveType convertValueToPrimitive

    let ofOption (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let logicalType = valueInfo.LogicalType
        let primitiveType = valueInfo.PrimitiveType
        let isNone = Option.preComputeIsNone dotnetType
        let getValue = Option.preComputeGetValue dotnetType
        let convertValueToPrimitive (valueOption: obj) =
            if isNone valueOption
            then null
            else valueInfo.ConvertValueToPrimitive (getValue valueOption)
        create dotnetType isOptional logicalType primitiveType convertValueToPrimitive

module private ListInfo =
    let private create dotnetType isOptional elementInfo getValues =
        { ListInfo.DotnetType = dotnetType
          ListInfo.IsOptional = isOptional
          ListInfo.ElementInfo = elementInfo
          ListInfo.GetValues = getValues }

    let ofArray1d (dotnetType: Type) =
        let isOptional = true
        let elementDotnetType = dotnetType.GetElementType()
        let elementInfo = ValueInfo.ofType elementDotnetType
        let getValues (arrayValue: obj) =
            arrayValue :?> IList
        create dotnetType isOptional elementInfo getValues

    let ofNullable (dotnetType: Type) (listInfo: ListInfo) =
        let isOptional = true
        let elementInfo = listInfo.ElementInfo
        let getValue = Nullable.preComputeGetValue dotnetType
        let getValues (nullableValue: obj) =
            if isNull nullableValue
            then null
            else listInfo.GetValues (getValue nullableValue)
        create dotnetType isOptional elementInfo getValues

    let ofOption (dotnetType: Type) (listInfo: ListInfo) =
        let isOptional = true
        let elementInfo = listInfo.ElementInfo
        let isNone = Option.preComputeIsNone dotnetType
        let getValue = Option.preComputeGetValue dotnetType
        let getValues (valueOption: obj) =
            if isNone valueOption
            then null
            else listInfo.GetValues (getValue valueOption)
        create dotnetType isOptional elementInfo getValues

module private FieldInfo =
    let private create name valueInfo getValue =
        { FieldInfo.Name = name
          FieldInfo.ValueInfo = valueInfo
          FieldInfo.GetValue = getValue }

    let ofField (field: PropertyInfo) =
        let name = field.Name
        let valueInfo = ValueInfo.ofType field.PropertyType
        let getValue = FSharpValue.PreComputeRecordFieldReader(field)
        create name valueInfo getValue

module private RecordInfo =
    let private create dotnetType isOptional fields =
        { RecordInfo.DotnetType = dotnetType
          RecordInfo.IsOptional = isOptional
          RecordInfo.Fields = fields }

    let ofNullable dotnetType (valueInfo: RecordInfo) =
        let isOptional = true
        let getValue = Nullable.preComputeGetValue dotnetType
        let fields =
            valueInfo.Fields
            |> Array.map (fun fieldInfo ->
                let getValue (nullableRecord: obj) =
                    if isNull nullableRecord
                    then null
                    else fieldInfo.GetValue (getValue nullableRecord)
                { fieldInfo with GetValue = getValue })
        create dotnetType isOptional fields

    let ofOption dotnetType (valueInfo: RecordInfo) =
        let isOptional = true
        let isNone = Option.preComputeIsNone dotnetType
        let getValue = Option.preComputeGetValue dotnetType
        let fields =
            valueInfo.Fields
            |> Array.map (fun fieldInfo ->
                let getValue (recordOption: obj) =
                    if isNone recordOption
                    then null
                    else fieldInfo.GetValue (getValue recordOption)
                { fieldInfo with GetValue = getValue })
        create dotnetType isOptional fields

    let ofRecord dotnetType =
        let isOptional = false
        let fields =
            FSharpType.GetRecordFields(dotnetType)
            |> Array.map FieldInfo.ofField
        create dotnetType isOptional fields
