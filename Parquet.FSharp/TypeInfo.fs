namespace rec Parquet.FSharp

open FSharp.Reflection
open System
open System.Reflection

type PrimitiveType =
    | Bool
    | Int32

type Primitive = {
    Type: PrimitiveType
    DotnetType: Type
    Schema: Schema.ValueType }

type AtomicInfo = {
    DotnetType: Type
    IsOptional: bool
    Primitive: Primitive
    ConvertValueToPrimitive: obj -> obj
    Schema: Schema.Value }

type ValueInfo =
    | Atomic of AtomicInfo
    | Record of RecordInfo

type FieldInfo = {
    Name: string
    DotnetType: Type
    ValueInfo: ValueInfo
    GetValue: obj -> obj
    Schema: Schema.Field }

type RecordInfo = {
    DotnetType: Type
    IsOptional: bool
    Fields: FieldInfo[]
    Schema: Schema.Value }

module private Primitive =
    let private create type' dotnetType schema =
        { Primitive.Type = type'
          Primitive.DotnetType = dotnetType
          Primitive.Schema = schema }

    let Bool = create PrimitiveType.Bool typeof<bool> Schema.ValueType.Bool
    let Int32 = create PrimitiveType.Int32 typeof<int> Schema.ValueType.Int32

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
    let private create dotnetType isOptional primitive convertValueToPrimitive schema =
        { AtomicInfo.DotnetType = dotnetType
          AtomicInfo.IsOptional = isOptional
          AtomicInfo.Primitive = primitive
          AtomicInfo.ConvertValueToPrimitive = convertValueToPrimitive
          AtomicInfo.Schema = schema }

    let private ofPrimitive (primitive: Primitive) =
        let dotnetType = primitive.DotnetType
        let isOptional = false
        let convertValueToPrimitive = id
        let schema = Schema.Value.create primitive.Schema isOptional
        create dotnetType isOptional primitive convertValueToPrimitive schema

    let Bool = ofPrimitive Primitive.Bool
    let Int32 = ofPrimitive Primitive.Int32

    let ofNullable (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let primitive = valueInfo.Primitive
        let getValue = Nullable.preComputeGetValue dotnetType
        let convertValueToPrimitive nullableValue =
            if isNull nullableValue
            then null
            else valueInfo.ConvertValueToPrimitive (getValue nullableValue)
        let schema = Schema.Value.create valueInfo.Schema.Type isOptional
        create dotnetType isOptional primitive convertValueToPrimitive schema

    let ofOption (dotnetType: Type) (valueInfo: AtomicInfo) =
        let isOptional = true
        let primitive = valueInfo.Primitive
        let isNone = Option.preComputeIsNone dotnetType
        let getValue = Option.preComputeGetValue dotnetType
        let convertValueToPrimitive (valueOption: obj) =
            if isNone valueOption
            then null
            else valueInfo.ConvertValueToPrimitive (getValue valueOption)
        let schema = Schema.Value.create valueInfo.Schema.Type isOptional
        create dotnetType isOptional primitive convertValueToPrimitive schema

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

    let schema valueInfo =
        match valueInfo with
        | ValueInfo.Atomic atomicInfo -> atomicInfo.Schema
        | ValueInfo.Record recordInfo -> recordInfo.Schema

module private FieldInfo =
    let create name dotnetType valueInfo getValue schema =
        { FieldInfo.Name = name
          FieldInfo.DotnetType = dotnetType
          FieldInfo.ValueInfo = valueInfo
          FieldInfo.GetValue = getValue
          FieldInfo.Schema = schema }

    let ofField (field: PropertyInfo) =
        let name = field.Name
        let dotnetType = field.PropertyType
        let valueInfo = ValueInfo.ofType dotnetType
        let getValue = FSharpValue.PreComputeRecordFieldReader(field)
        let schema =
            let value = ValueInfo.schema valueInfo
            Schema.Field.create name value
        create name dotnetType valueInfo getValue schema

module private RecordInfo =
    let create dotnetType isOptional fields schema =
        { RecordInfo.DotnetType = dotnetType
          RecordInfo.IsOptional = isOptional
          RecordInfo.Fields = fields
          RecordInfo.Schema = schema }

    let private createSchema isOptional (fields: FieldInfo[]) =
        let fields = fields |> Array.map _.Schema
        let valueType = Schema.ValueType.record fields
        Schema.Value.create valueType isOptional

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
        let schema = createSchema isOptional fields
        create dotnetType isOptional fields schema

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
        let schema = createSchema isOptional fields
        create dotnetType isOptional fields schema

    let ofRecord dotnetType =
        let isOptional = false
        let fields =
            FSharpType.GetRecordFields(dotnetType)
            |> Array.map FieldInfo.ofField
        let schema = createSchema isOptional fields
        create dotnetType isOptional fields schema
