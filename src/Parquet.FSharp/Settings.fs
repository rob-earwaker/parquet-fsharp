namespace rec Parquet.FSharp

open System
open System.Reflection

type internal DelegateFieldSettingsPolicy(isValidFor, applyFieldSettings) =
    interface IFieldSettingsPolicy with
        member this.IsValidFor(field) = isValidFor field
        member this.ApplyFieldSettings(fieldSettings) = applyFieldSettings fieldSettings

type internal DelegateValueSettingsPolicy(isValidFor, applyValueSettings) =
    interface IValueSettingsPolicy with
        member this.IsValidFor(valueType) = isValidFor valueType
        member this.ApplyValueSettings(valueSettings) = applyValueSettings valueSettings

// Add module suffix so we can define the module in a different file to the type.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal ValueSettings =
    let Default = {
        ValueSettings.Converter = Option.None
        ValueSettings.ListElementSettings = ValueSettings.Default
        ValueSettings.OptionalValueSettings = ValueSettings.Default }

    let converterOption converter (settings: ValueSettings) =
        { settings with Converter = converter }

    let converter converter (settings: ValueSettings) =
        converterOption (Option.Some converter) settings

    let updateListElementSettings update (settings: ValueSettings) =
        { settings with ListElementSettings = update settings.ListElementSettings }

    let updateOptionalValueSettings update (settings: ValueSettings) =
        { settings with OptionalValueSettings = update settings.OptionalValueSettings }

// Add module suffix so we can define the module in a different file to the type.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal FieldSettings =
    let Default = {
        FieldSettings.Name = Option.None
        FieldSettings.ValueSettings = ValueSettings.Default }

    let nameOption name (settings: FieldSettings) =
        { settings with Name = name }

    let name name settings =
        nameOption (Option.Some name) settings

    let valueSettings valueSettings (settings: FieldSettings) =
        { settings with ValueSettings = valueSettings }

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
            let isValidFor = fun valueType' -> valueType' = valueType
            DelegateValueSettingsPolicy(isValidFor, applyValueSettings)
            :> IValueSettingsPolicy
        let valuePolicies = valuePolicy :: settings.ValuePolicies
        { settings with ValuePolicies = valuePolicies }

    let overrideForField field applyFieldSettings (settings: Settings) =
        let fieldPolicy =
            let isValidFor = fun field' -> field' = field
            DelegateFieldSettingsPolicy(isValidFor, applyFieldSettings)
            :> IFieldSettingsPolicy
        let fieldPolicies = fieldPolicy :: settings.FieldPolicies
        { settings with FieldPolicies = fieldPolicies }

    let resolveForValue (valueType: Type) (settings: Settings) =
        let valueAttributes =
            valueType.GetCustomAttributes<ParquetValueAttribute>(``inherit`` = true)
            |> List.ofSeq
        let valuePolicies =
            settings.ValuePolicies
            |> List.filter (fun policy -> policy.IsValidFor(valueType))
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
            valueAttributes
        // Apply any configured policies. We want policies to apply in the order
        // that they were added. Since policies are prepended when added, we
        // apply them in reverse order.
        |> List.foldBack
            (fun (policy: IValueSettingsPolicy) -> policy.ApplyValueSettings)
            valuePolicies

    let resolveForField (field: PropertyInfo) settings =
        let valueSettings = Settings.resolveForValue field.PropertyType settings
        let fieldAttributes =
            field.GetCustomAttributes<ParquetFieldAttribute>(``inherit`` = true)
            |> List.ofSeq
        let fieldPolicies =
            settings.FieldPolicies
            |> List.filter (fun policy -> policy.IsValidFor(field))
        // Start with the default settings.
        FieldSettings.Default
        // Apply resolved value settings based on the value type. This will
        // include settings from attributes applied to the field value's type
        // and settings from value policies.
        |> FieldSettings.valueSettings valueSettings
        // Apply attributes first to allow settings to be overridden at the
        // serialization call-site. This ensures that serialization of types
        // defined in third-party assemblies can be customized regardless of
        // whether they have attributes.
        // TODO: Does the order matter here? Easier to read with foldBack, so if
        // order does matter then maybe best to reverse above.
        |> List.foldBack
            (fun (attribute: ParquetFieldAttribute) -> attribute.ApplyFieldSettings)
            fieldAttributes
        // Apply any configured policies. We want policies to apply in the order
        // that they were added. Since policies are prepended when added, we
        // apply them in reverse order.
        |> List.foldBack
            (fun (policy: IFieldSettingsPolicy) -> policy.ApplyFieldSettings)
            fieldPolicies

[<AbstractClass>]
[<AttributeUsage(AttributeTargets.Class ||| AttributeTargets.Struct)>]
type ParquetValueAttribute() =
    inherit Attribute()
    abstract member ApplyValueSettings : valueSettings:ValueSettings -> ValueSettings

[<AttributeUsage(AttributeTargets.Property)>]
type ParquetFieldAttribute() =
    inherit ParquetValueAttribute()

    let mutable name = Option<string>.None

    member this.Name
        with set value =
            name <- Option.Some value

    override this.ApplyValueSettings(valueSettings) =
        valueSettings

    member this.ApplyFieldSettings(fieldSettings) =
        let name = name |> Option.orElse fieldSettings.Name
        fieldSettings
        |> FieldSettings.nameOption name
        |> FieldSettings.updateValueSettings this.ApplyValueSettings
