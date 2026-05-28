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
        ValueSettings.NestedValueSettings = ValueSettings.Default }

    let converterOption converter (settings: ValueSettings) =
        { settings with Converter = converter }

    let converter converter (settings: ValueSettings) =
        converterOption (Option.Some converter) settings

// Add module suffix so we can define the module in a different file to the type.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal FieldSettings =
    let Default = {
        FieldSettings.Name = Option.None
        FieldSettings.ValueSettings = ValueSettings.Default }

    let nameOption name (settings: FieldSettings) =
        { settings with Name = name }

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
        let valueSettingsAttributes =
            valueType.GetCustomAttributes<ParquetValueSettingsAttribute>(``inherit`` = true)
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
            (fun (attribute: ParquetValueSettingsAttribute) -> attribute.ApplyValueSettings)
            valueSettingsAttributes
        // Apply any configured policies. We want policies to apply in the order
        // that they were added. Since policies are prepended when added, we
        // apply them in reverse order.
        |> List.foldBack
            (fun (policy: IValueSettingsPolicy) -> policy.ApplyValueSettings)
            valuePolicies

    let resolveForField (field: PropertyInfo) settings =
        let valueSettings = Settings.resolveForValue field.PropertyType settings
        let fieldSettingsAttributes =
            field.GetCustomAttributes<ParquetFieldSettingsAttribute>(``inherit`` = true)
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
            (fun (attribute: ParquetFieldSettingsAttribute) -> attribute.ApplyFieldSettings)
            fieldSettingsAttributes
        // Apply any configured policies. We want policies to apply in the order
        // that they were added. Since policies are prepended when added, we
        // apply them in reverse order.
        |> List.foldBack
            (fun (policy: IFieldSettingsPolicy) -> policy.ApplyFieldSettings)
            fieldPolicies

[<AbstractClass>]
[<AttributeUsage(AttributeTargets.Class ||| AttributeTargets.Struct)>]
type ParquetValueSettingsAttribute() =
    inherit Attribute()
    abstract member ApplyValueSettings : valueSettings:ValueSettings -> ValueSettings

[<AbstractClass>]
[<AttributeUsage(AttributeTargets.Property)>]
type ParquetFieldSettingsAttribute() =
    inherit Attribute()
    abstract member ApplyFieldSettings : fieldSettings:FieldSettings -> FieldSettings

type ParquetFieldAttribute() =
    inherit ParquetFieldSettingsAttribute()

    let mutable name = Option<string>.None

    member this.Name
        with set value =
            name <- Option.Some value

    abstract member ApplyValueSettings : valueSettings:ValueSettings -> ValueSettings

    default this.ApplyValueSettings(valueSettings) =
        valueSettings

    override this.ApplyFieldSettings(fieldSettings) =
        let name = name |> Option.orElse fieldSettings.Name
        fieldSettings
        |> FieldSettings.nameOption name
        // TODO: Naming - 'update' vs 'apply'
        |> FieldSettings.updateValueSettings this.ApplyValueSettings

[<AbstractClass>]
type ParquetNestedValueAttribute() =
    inherit ParquetFieldSettingsAttribute()

    // The minimum nesting level, equivalent to a single level of nesting, i.e. a value nested
    // directly inside an optional or list field. For example, in a field of type {option<int>} or
    // {list<int>}, the {int} values are nested by one level and so are at the minimum nesting level.
    // A nesting level of zero would imply no nesting at all, so is not a valid value.
    static let [<Literal>] MinNestingLevel = 1

    member val Level = MinNestingLevel with get, set

    abstract member ApplyNestedValueSettings : valueSettings:ValueSettings -> ValueSettings

    override this.ApplyFieldSettings(fieldSettings) =
        // We need to recurse down through the field value settings until we reach the
        // {NestedValueSettings} at the configured level. We can then update these settings using
        // the abstract {ApplyNestedValueSettings} method and then roll back up the levels, updating
        // them as we go. We do this using a recursive function.
        let rec updateNestedValueSettings currentLevel (valueSettings: ValueSettings) =
            let nestedValueSettings = valueSettings.NestedValueSettings
            // Update the existing nested value settings based on the current level.
            let updatedNestedValueSettings =
                // If we haven't yet reached the configured level then we continue recursing down by
                // incrementing the current level and passing down the nested value settings.
                if currentLevel < this.Level
                then updateNestedValueSettings (currentLevel + 1) nestedValueSettings
                // If we have reached the configured level then we don't need to continue recursing
                // and our updated nested value settings are the result of calling the
                // {ApplyNestedValueSettings} method on the settings from the current level.
                elif currentLevel = this.Level
                then this.ApplyNestedValueSettings(nestedValueSettings)
                // Otherwise, the level is greater than the configured level. This shouldn't really
                // happen unless there's a misconfiguration and the configured level is less than
                // the {MinNestingLevel}, but we handle it anyway by just leaving the nested value
                // settings unmodified.
                else nestedValueSettings
            // Now that we've resolved the updated nested value settings at this level, we update
            // the value settings for this level and return them.
            { valueSettings with NestedValueSettings = updatedNestedValueSettings }
        // Update the field value settings using the recursive function above, starting at the
        // minimum nesting level.
        fieldSettings
        |> FieldSettings.updateValueSettings (updateNestedValueSettings MinNestingLevel)
