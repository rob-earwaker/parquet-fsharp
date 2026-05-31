namespace rec Parquet.FSharp

open System
open System.Linq
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
    let Default = { ValueSettings.Converter = Option.None }

    let converterOption converter (settings: ValueSettings) =
        { settings with Converter = converter }

    let converter converter (settings: ValueSettings) =
        converterOption (Option.Some converter) settings

// Add module suffix so we can define the module in a different file to the type.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module internal FieldSettings =
    let Default = { FieldSettings.Name = Option.None }

    let nameOption name (settings: FieldSettings) =
        { settings with Name = name }

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

    let resolveForValue (value: ValueDefinition) (settings: Settings) =
        // To resolve the settings for a value we have to combine both
        // attributes from the dotnet type information and relevant policies
        // from the serialization call site settings. Value settings attributes
        // can be defined both on the type and on the field so we read them from
        // both. For attributes read from the field we must ensure the nesting
        // level defined in the attribute matches the nesting level of this
        // value in the type heirarchy. This filtering is not necessary for
        // attributes applied to the type.
        let typeValueSettingsAttributes =
            value.Attributes.OfType<ParquetValueSettingsAttribute>()
            |> List.ofSeq
        let fieldValueSettingsAttributes =
            value.Field.Attributes.OfType<ParquetValueSettingsAttribute>()
            |> Seq.filter _.NestingLevel.Equals(value.NestingLevel)
            |> List.ofSeq
        let valueSettingsPolicies =
            settings.ValuePolicies
            |> List.filter (fun policy -> policy.IsValidFor(value))
        // Now that we have all the relevant attributes and policies we need to
        // apply them to the default settings. Settings that are applied later
        // can override settings applied earlier, so we do this in a specific
        // order to allow settings to be overriden at convenient places.
        // 
        //   1. Attributes from the type.
        //   2. Attributes from the field.
        //   3. Policies from the serialization call site settings.
        //
        // We want policies defined at the serialization call site to override
        // all attributes since it may not be possible to modify attributes,
        // e.g. if the types are defined in third-party assemblies. We want
        // these policies to apply in the order that they were added. Since
        // policies are prepended when added, we reverse the order.
        //
        // We want field attributes to override type attributes. Type attributes
        // are global modifiers for a given type, but we may want to customize
        // specific fields of that type independently.
        //
        // Create a list of settings update functions in the above order and
        // apply them to the default settings in turn.
        List.concat [
            typeValueSettingsAttributes |> List.map _.ApplyValueSettings
            fieldValueSettingsAttributes |> List.map _.ApplyValueSettings
            valueSettingsPolicies |> List.map _.ApplyValueSettings |> List.rev ]
        |> List.fold
            (fun valueSettings update -> update valueSettings)
            ValueSettings.Default

    let resolveForField (field: FieldDefinition) settings =
        // To resolve the settings for a field we have to combine attributes
        // from the field with relevant policies from the serialization call
        // site settings.
        let fieldSettingsAttributes =
            field.Attributes.OfType<ParquetFieldSettingsAttribute>()
            |> List.ofSeq
        let fieldSettingsPolicies =
            settings.FieldPolicies
            |> List.filter (fun policy -> policy.IsValidFor(field))
        // Now that we have all the relevant attributes and policies we need to
        // apply them to the default settings. Settings that are applied later
        // can override settings applied earlier, so we do this in a specific
        // order to allow settings to be overriden at convenient places.
        // 
        //   1. Attributes from the field.
        //   2. Policies from the serialization call site settings.
        //
        // We want policies defined at the serialization call site to override
        // attributes since it may not be possible to modify attributes, e.g. if
        // the types are defined in third-party assemblies. We want these
        // policies to apply in the order that they were added. Since policies
        // are prepended when added, we reverse the order.
        //
        // Create a list of settings update functions in the above order and
        // apply them to the default settings in turn.
        List.concat [
            fieldSettingsAttributes |> List.map _.ApplyFieldSettings
            fieldSettingsPolicies |> List.map _.ApplyFieldSettings |> List.rev ]
        |> List.fold
            (fun fieldSettings update -> update fieldSettings)
            FieldSettings.Default

[<AbstractClass>]
[<AttributeUsage(AttributeTargets.Class ||| AttributeTargets.Struct ||| AttributeTargets.Property)>]
type ParquetValueSettingsAttribute() =
    inherit Attribute()
    // TODO: Nesting level isn't really relevant for attributes applied to types.
    // Should we have a different base class for these?
    member val NestingLevel = 0 with get, set
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

    override this.ApplyFieldSettings(fieldSettings) =
        let name = name |> Option.orElse fieldSettings.Name
        fieldSettings |> FieldSettings.nameOption name

// TODO: Attribute for ignoring a field?
