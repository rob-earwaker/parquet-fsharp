namespace Parquet.FSharp

open System

[<AttributeUsage(AttributeTargets.Class ||| AttributeTargets.Struct)>]
type internal ParquetValueTypeAttribute() =
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

    abstract member ModifyValueSettings : valueSettings:ValueSettings -> ValueSettings

    default this.ModifyValueSettings(valueSettings) =
        let forceOptional = optional |> Option.defaultValue valueSettings.ForceOptional
        let forceRequired = required |> Option.defaultValue valueSettings.ForceRequired
        let allowNullValues = allowNulls |> Option.defaultValue valueSettings.AllowNullValues
        valueSettings
        |> ValueSettings.forceOptional forceOptional
        |> ValueSettings.forceRequired forceRequired
        |> ValueSettings.allowNullValues allowNullValues

    interface IValueSettingsModifier with
        member this.ModifyValueSettings(valueSettings) =
            this.ModifyValueSettings(valueSettings)

type internal ParquetUnionAttribute() =
    inherit ParquetValueTypeAttribute()

    let mutable caseTypeFieldName = Option<string>.None

    member this.CaseTypeFieldName
        with set value =
            caseTypeFieldName <- Option.Some value

    override this.ModifyValueSettings(valueSettings) =
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

    abstract member ModifyFieldSettings : fieldSettings:FieldSettings -> FieldSettings

    default this.ModifyFieldSettings(fieldSettings) =
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

    interface IFieldSettingsModifier with
        member this.ModifyFieldSettings(fieldSettings) =
            this.ModifyFieldSettings(fieldSettings)

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

    override this.ModifyFieldSettings(fieldSettings) =
        let fieldSettings = base.ModifyFieldSettings(fieldSettings)
        let valueSettings = fieldSettings.ValueSettings
        let useLocalDateTime = local |> Option.defaultValue valueSettings.UseLocalDateTime
        let ignoreDateTimeKind = ignoreKind |> Option.defaultValue valueSettings.IgnoreDateTimeKind
        let timeUnit = unit |> Option.defaultValue valueSettings.TimeUnit
        fieldSettings
        |> FieldSettings.updateValueSettings (fun valueSettings ->
            valueSettings
            |> ValueSettings.useLocalDateTime useLocalDateTime
            |> ValueSettings.ignoreDateTimeKind ignoreDateTimeKind
            |> ValueSettings.timeUnit timeUnit)
