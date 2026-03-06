namespace Parquet.FSharp

open System
open System.Reflection

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
        ValueSettings.TimeUnit = TimeUnit.Microseconds
        ValueSettings.UnionCaseTypeFieldName = "Type" }

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

    let timeUnit value (settings: ValueSettings) =
        { settings with TimeUnit = value }

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
    let Default = {
        Settings.ValueConverters = [
            DefaultBoolConverter.Instance
            DefaultInt8Converter.Instance
            DefaultInt16Converter.Instance
            DefaultInt32Converter.Instance
            DefaultInt64Converter.Instance
            DefaultUInt8Converter.Instance
            DefaultUInt16Converter.Instance
            DefaultUInt32Converter.Instance
            DefaultUInt64Converter.Instance
            DefaultFloat32Converter.Instance
            DefaultFloat64Converter.Instance
            DefaultDecimalConverter.Instance
            DefaultGuidConverter.Instance
            DefaultEnumConverter.Instance
            DefaultTimeSpanConverter.Instance
            DefaultDateTimeConverter.Instance
            DefaultDateTimeOffsetConverter.Instance
            DefaultStringConverter.Instance
            // This must come before the generic array type since byte arrays
            // are supported as a primitive type in Parquet and are therefore
            // handled as atomic values rather than lists.
            DefaultByteArrayConverter.Instance
            DefaultListConverter.Instance
            DefaultArray1dConverter.Instance
            DefaultResizeArrayConverter.Instance
            DefaultRecordConverter.Instance
            // This must come before the generic union type since option types
            // are handled in a special way.
            DefaultOptionConverter.Instance
            DefaultNullableConverter.Instance
            DefaultUnionConverter.Instance ]
        Settings.ValueSettingOverrides = []
        Settings.FieldSettingOverrides = [] }

    let addConverter valueConverter (settings: Settings) =
        { settings with
            ValueConverters = valueConverter :: settings.ValueConverters }

    let overrideForType dotnetType overrideSettings (settings: Settings) =
        { settings with
            ValueSettingOverrides =
                (dotnetType, overrideSettings) :: settings.ValueSettingOverrides }

    let overrideForField fieldInfo overrideSettings (settings: Settings) =
        { settings with
            FieldSettingOverrides =
                (fieldInfo, overrideSettings) :: settings.FieldSettingOverrides }

    //let resolveSerializer ...
    //let resolveDeserializer ...
    //let resolveValueTypeSettings ...
    //let resolveFieldSettings ...
