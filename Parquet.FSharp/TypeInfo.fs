namespace rec Parquet.FSharp

open FSharp.Reflection
open System
open System.Reflection

type PrimitiveType =
    | Bool
    | Int32

type AtomicInfo = {
    DotnetType: Type
    IsOptional: bool
    PrimitiveType: PrimitiveType
    ConvertValueToPrimitive: obj -> obj }

type ValueInfo =
    | Atomic of AtomicInfo
    | Record of RecordInfo

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

module private AtomicInfo =
    let private create dotnetType isOptional primitiveType convertValueToPrimitive =
        { AtomicInfo.DotnetType = dotnetType
          AtomicInfo.IsOptional = isOptional
          AtomicInfo.PrimitiveType = primitiveType
          AtomicInfo.ConvertValueToPrimitive = convertValueToPrimitive }

    let private ofPrimitive dotnetType primitiveType =
        let isOptional = false
        let convertValueToPrimitive = id
        create dotnetType isOptional primitiveType convertValueToPrimitive

    let Bool = ofPrimitive typeof<bool> PrimitiveType.Bool
    let Int32 = ofPrimitive typeof<int> PrimitiveType.Int32

    let ofNullable (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let primitiveType = valueInfo.PrimitiveType
        let getValue = Nullable.preComputeGetValue dotnetType
        let convertValueToPrimitive nullableValue =
            if isNull nullableValue
            then null
            else valueInfo.ConvertValueToPrimitive (getValue nullableValue)
        create dotnetType isOptional primitiveType convertValueToPrimitive

    let ofOption (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let primitiveType = valueInfo.PrimitiveType
        let isNone = Option.preComputeIsNone dotnetType
        let getValue = Option.preComputeGetValue dotnetType
        let convertValueToPrimitive (valueOption: obj) =
            if isNone valueOption
            then null
            else valueInfo.ConvertValueToPrimitive (getValue valueOption)
        create dotnetType isOptional primitiveType convertValueToPrimitive

module ValueInfo =
    let private ofNullable (dotnetType: Type) =
        let valueDotnetType = Nullable.GetUnderlyingType(dotnetType)
        match ValueInfo.ofType valueDotnetType with
        | ValueInfo.Atomic atomicInfo ->
            let nullableAtomicInfo = AtomicInfo.ofNullable dotnetType atomicInfo
            ValueInfo.Atomic nullableAtomicInfo
        | ValueInfo.Record recordInfo ->
            let nullableRecordInfo = RecordInfo.ofNullable dotnetType recordInfo
            ValueInfo.Record nullableRecordInfo

    let private ofOption (dotnetType: Type) =
        let valueDotnetType = dotnetType.GetGenericArguments()[0]
        match ValueInfo.ofType valueDotnetType with
        | ValueInfo.Atomic atomicInfo ->
            let atomicOptionInfo = AtomicInfo.ofOption dotnetType atomicInfo
            ValueInfo.Atomic atomicOptionInfo
        | ValueInfo.Record recordInfo ->
            let recordOptionInfo = RecordInfo.ofOption dotnetType recordInfo
            ValueInfo.Record recordOptionInfo

    let ofType (dotnetType: Type) =
        match dotnetType with
        | DotnetType.Bool -> ValueInfo.Atomic AtomicInfo.Bool
        | DotnetType.Int32 -> ValueInfo.Atomic AtomicInfo.Int32
        | DotnetType.Nullable -> ofNullable dotnetType
        | DotnetType.Option -> ofOption dotnetType
        | DotnetType.Record -> ValueInfo.Record (RecordInfo.ofRecord dotnetType)
        | _ -> failwith $"unsupported type '{dotnetType.FullName}'"

    let of'<'Value> =
        ofType typeof<'Value>

module private FieldInfo =
    let create name valueInfo getValue =
        { FieldInfo.Name = name
          FieldInfo.ValueInfo = valueInfo
          FieldInfo.GetValue = getValue }

    let ofField (field: PropertyInfo) =
        let name = field.Name
        let valueInfo = ValueInfo.ofType field.PropertyType
        let getValue = FSharpValue.PreComputeRecordFieldReader(field)
        create name valueInfo getValue

module private RecordInfo =
    let create dotnetType isOptional fields =
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
