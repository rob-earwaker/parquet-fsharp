namespace rec Parquet.FSharp

open System
open System.Reflection

type internal DelegateFieldSettingsPolicy(recordType, fieldName, applyFieldSettings) =
    member val RecordType = recordType with get
    member val FieldName = fieldName with get

    member this.ApplyFieldSettings(fieldSettings) =
        applyFieldSettings fieldSettings

    interface IFieldSettingsPolicy with
        member this.RecordType = this.RecordType
        member this.FieldName = this.FieldName
        member this.ApplyFieldSettings(fieldSettings) =
            this.ApplyFieldSettings(fieldSettings)

type internal DelegateValueSettingsPolicy(valueType, applyValueSettings) =
    member val ValueType = valueType with get

    member this.ApplyValueSettings(valueSettings) =
        applyValueSettings valueSettings

    interface IValueSettingsPolicy with
        member this.ValueType = this.ValueType
        member this.ApplyValueSettings(valueSettings) =
            this.ApplyValueSettings(valueSettings)

// Add module suffix so we can define the module in a different file to the type.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal ValueSettings =
    let Default = {
        ValueSettings.ForceOptional = false
        ValueSettings.ForceRequired = false
        ValueSettings.AllowNullValues = false
        ValueSettings.DecimalScale = 18
        ValueSettings.DecimalPrecision = 38
        ValueSettings.UseLocalDateTime = false
        ValueSettings.IgnoreDateTimeKind = false
        ValueSettings.DateTimeUnit = TimeUnit.Microseconds
        ValueSettings.UnionCaseTypeFieldName = "Type"
        ValueSettings.AlwaysIncludeUnionCaseTypeField = false
        ValueSettings.AlwaysIncludeNestedUnionCaseRecord = false }

    let forceOptional value (settings: ValueSettings) =
        { settings with ForceOptional = value }

    let forceRequired value (settings: ValueSettings) =
        { settings with ForceRequired = value }

    let allowNullValues value (settings: ValueSettings) =
        { settings with AllowNullValues = value }

    let useLocalDateTime value (settings: ValueSettings) =
        { settings with UseLocalDateTime = value }

    let ignoreDateTimeKind value (settings: ValueSettings) =
        { settings with IgnoreDateTimeKind = value }

    let dateTimeUnit value (settings: ValueSettings) =
        { settings with DateTimeUnit = value }

    let unionCaseTypeFieldName value (settings: ValueSettings) =
        { settings with UnionCaseTypeFieldName = value }

// Add module suffix so we can define the module in a different file to the type.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal FieldSettings =
    let Default = {
        FieldSettings.NameOverride = Option.None
        FieldSettings.ValueSettings = ValueSettings.Default }

    let nameOverride value (settings: FieldSettings) =
        { settings with NameOverride = value }

    let nameOverrideValue value settings =
        nameOverride (Option.Some value) settings

    let valueSettings value (settings: FieldSettings) =
        { settings with ValueSettings = value }

    let updateValueSettings update (settings: FieldSettings) =
        { settings with ValueSettings = update settings.ValueSettings }

// Add module suffix so we can define the module in a different file to the type.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal Settings =
    let addConverter valueConverter (settings: Settings) =
        let valueConverters = valueConverter :: settings.ValueConverters
        { settings with ValueConverters = valueConverters }

    let overrideForValues valueType applyValueSettings (settings: Settings) =
        let valuePolicy =
            DelegateValueSettingsPolicy(valueType, applyValueSettings)
            :> IValueSettingsPolicy
        let valuePolicies = valuePolicy :: settings.ValuePolicies
        { settings with ValuePolicies = valuePolicies }

    let overrideForField recordType fieldName applyFieldSettings (settings: Settings) =
        let fieldPolicy =
            DelegateFieldSettingsPolicy(recordType, fieldName, applyFieldSettings)
            :> IFieldSettingsPolicy
        let fieldPolicies = fieldPolicy :: settings.FieldPolicies
        { settings with FieldPolicies = fieldPolicies }

    let resolveForValue (valueType: Type) (settings: Settings) =
        let attributes =
            valueType.GetCustomAttributes<ParquetValueAttribute>(``inherit`` = true)
            |> List.ofSeq
        let valuePolicies =
            settings.ValuePolicies
            |> List.filter (fun policy -> policy.ValueType = valueType)
        // Start with the default settings.
        ValueSettings.Default
        // Apply attributes first to allow settings to be overridden at the
        // serialization call-site. This ensures that serialization of types
        // defined in third-party assemblies can be customized regardless of
        // whether they have attributes.
        // TODO: Does the order matter here? Easier to read with foldBack, so if
        // order does matter then maybe best to reverse above.
        |> List.foldBack
            (fun (attribute: ParquetValueAttribute) -> attribute.ApplyValueSettings)
            attributes
        // Apply any configured policies. We want policies to apply in the order
        // that they were added. Since policies are prepended when added, we
        // apply them in reverse order.
        |> List.foldBack
            (fun (policy: IValueSettingsPolicy) -> policy.ApplyValueSettings)
            valuePolicies

    let resolveForField (recordType: Type) fieldName (settings: Settings) =
        let field = recordType.GetProperty(fieldName)
        let attributes =
            field.GetCustomAttributes<ParquetFieldAttribute>(``inherit`` = true)
            |> List.ofSeq
        let fieldPolicies =
            settings.FieldPolicies
            |> List.filter (fun policy ->
                policy.RecordType = recordType
                && policy.FieldName = fieldName)
        // Start with the default settings.
        FieldSettings.Default
        // Apply attributes first to allow settings to be overridden at the
        // serialization call-site. This ensures that serialization of types
        // defined in third-party assemblies can be customized regardless of
        // whether they have attributes.
        // TODO: Does the order matter here? Easier to read with foldBack, so if
        // order does matter then maybe best to reverse above.
        |> List.foldBack
            (fun (attribute: ParquetFieldAttribute) -> attribute.ApplyFieldSettings)
            attributes
        // Apply any configured policies. We want policies to apply in the order
        // that they were added. Since policies are prepended when added, we
        // apply them in reverse order.
        |> List.foldBack
            (fun (policy: IFieldSettingsPolicy) -> policy.ApplyFieldSettings)
            fieldPolicies

[<AttributeUsage(AttributeTargets.Class ||| AttributeTargets.Struct)>]
type internal ParquetValueAttribute() =
    inherit Attribute()

    let mutable optional = Option<bool>.None
    let mutable required = Option<bool>.None
    let mutable allowNulls = Option<bool>.None

    member this.Optional
        with set value =
            optional <- Option.Some value

    member this.Required
        with set value =
            required <- Option.Some value

    member this.AllowNulls
        with set value =
            allowNulls <- Option.Some value

    abstract member ApplyValueSettings : valueSettings:ValueSettings -> ValueSettings

    default this.ApplyValueSettings(valueSettings) =
        let forceOptional = optional |> Option.defaultValue valueSettings.ForceOptional
        let forceRequired = required |> Option.defaultValue valueSettings.ForceRequired
        let allowNullValues = allowNulls |> Option.defaultValue valueSettings.AllowNullValues
        valueSettings
        |> ValueSettings.forceOptional forceOptional
        |> ValueSettings.forceRequired forceRequired
        |> ValueSettings.allowNullValues allowNullValues

type internal ParquetUnionAttribute() =
    inherit ParquetValueAttribute()

    let mutable caseTypeFieldName = Option<string>.None

    member this.CaseTypeFieldName
        with set value =
            caseTypeFieldName <- Option.Some value

    override this.ApplyValueSettings(valueSettings) =
        let valueSettings = base.ApplyValueSettings(valueSettings)
        let unionCaseTypeFieldName =
            caseTypeFieldName
            |> Option.defaultValue valueSettings.UnionCaseTypeFieldName
        valueSettings
        |> ValueSettings.unionCaseTypeFieldName unionCaseTypeFieldName

[<AttributeUsage(AttributeTargets.Property)>]
type internal ParquetFieldAttribute() =
    inherit Attribute()

    let mutable name = Option<string>.None
    let mutable optional = Option<bool>.None
    let mutable required = Option<bool>.None
    let mutable allowNulls = Option<bool>.None

    member this.Name
        with set value =
            name <- Option.Some value

    member this.Optional
        with set value =
            optional <- Option.Some value

    member this.Required
        with set value =
            required <- Option.Some value

    member this.AllowNulls
        with set value =
            allowNulls <- Option.Some value

    abstract member ApplyFieldSettings : fieldSettings:FieldSettings -> FieldSettings

    default this.ApplyFieldSettings(fieldSettings) =
        let valueSettings = fieldSettings.ValueSettings
        let nameOverride = name |> Option.orElse fieldSettings.NameOverride
        let forceOptional = optional |> Option.defaultValue valueSettings.ForceOptional
        let forceRequired = required |> Option.defaultValue valueSettings.ForceRequired
        let allowNullValues = allowNulls |> Option.defaultValue valueSettings.AllowNullValues
        fieldSettings
        |> FieldSettings.nameOverride nameOverride
        |> FieldSettings.updateValueSettings (fun valueSettings ->
            valueSettings
            |> ValueSettings.forceOptional forceOptional
            |> ValueSettings.forceRequired forceRequired
            |> ValueSettings.allowNullValues allowNullValues)

type internal ParquetDateTimeFieldAttribute() =
    inherit ParquetFieldAttribute()

    let mutable local = Option<bool>.None
    let mutable ignoreKind = Option<bool>.None
    let mutable unit = Option<TimeUnit>.None

    member this.Local
        with set value =
            local <- Option.Some value

    member this.IgnoreKind
        with set value =
            ignoreKind <- Option.Some value

    member this.Unit
        with set value =
            unit <- Option.Some value

    override this.ApplyFieldSettings(fieldSettings) =
        let fieldSettings = base.ApplyFieldSettings(fieldSettings)
        let valueSettings = fieldSettings.ValueSettings
        let useLocalDateTime = local |> Option.defaultValue valueSettings.UseLocalDateTime
        let ignoreDateTimeKind = ignoreKind |> Option.defaultValue valueSettings.IgnoreDateTimeKind
        let dateTimeUnit = unit |> Option.defaultValue valueSettings.DateTimeUnit
        fieldSettings
        |> FieldSettings.updateValueSettings (fun valueSettings ->
            valueSettings
            |> ValueSettings.useLocalDateTime useLocalDateTime
            |> ValueSettings.ignoreDateTimeKind ignoreDateTimeKind
            |> ValueSettings.dateTimeUnit dateTimeUnit)
