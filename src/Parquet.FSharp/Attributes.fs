namespace Parquet.FSharp

open System
open System.Reflection

type ParquetUnionAttribute() =
    inherit ParquetValueAttribute()

    let default' = UnionConverterSettings.Default

    member val CaseTypeFieldName = default'.CaseTypeFieldName with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            UnionConverter {
                CaseTypeFieldName = this.CaseTypeFieldName }
        valueSettings |> ValueSettings.converter converter

type ParquetBoolFieldAttribute() =
    inherit ParquetFieldAttribute()

    let default' = BoolConverterSettings.Default

    member val Optional = default'.Optional with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            BoolConverter {
                Optional = this.Optional }
        valueSettings |> ValueSettings.converter converter

type ParquetListFieldAttribute() =
    inherit ParquetFieldAttribute()

    let default' = ListConverterSettings.Default

    member val Optional = default'.Optional with get, set
    member val AllowNulls = default'.AllowNulls with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            ListConverter {
                Optional = this.Optional
                AllowNulls = this.AllowNulls }
        valueSettings |> ValueSettings.converter converter

type ParquetUnionFieldAttribute() =
    inherit ParquetFieldAttribute()

    let default' = UnionConverterSettings.Default

    member val CaseTypeFieldName = default'.CaseTypeFieldName with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            UnionConverter {
                CaseTypeFieldName = this.CaseTypeFieldName }
        valueSettings |> ValueSettings.converter converter

//type internal ParquetDateTimeFieldAttribute() =
//    inherit ParquetFieldAttribute()

//    let mutable local = Option<bool>.None
//    let mutable ignoreKind = Option<bool>.None
//    let mutable unit = Option<TimeUnit>.None

//    member this.Local
//        with set value =
//            local <- Option.Some value

//    member this.IgnoreKind
//        with set value =
//            ignoreKind <- Option.Some value

//    member this.Unit
//        with set value =
//            unit <- Option.Some value

//    override this.ApplyValueSettings(valueSettings) =
//        let valueSettings = base.ApplyValueSettings(valueSettings)
//        let useLocalDateTime = local |> Option.defaultValue valueSettings.UseLocalDateTime
//        let ignoreDateTimeKind = ignoreKind |> Option.defaultValue valueSettings.IgnoreDateTimeKind
//        let dateTimeUnit = unit |> Option.defaultValue valueSettings.DateTimeUnit
//        valueSettings
//        |> ValueSettings.useLocalDateTime useLocalDateTime
//        |> ValueSettings.ignoreDateTimeKind ignoreDateTimeKind
//        |> ValueSettings.dateTimeUnit dateTimeUnit

//type internal ParquetOptionFieldAttribute() =
//    inherit Attribute()

//    // TODO: These are probably common across a lot of different converters. Maybe
//    // still worth keeping them separate?
//    let mutable optional = Option<bool>.None
//    let mutable required = Option<bool>.None
//    let mutable allowNulls = Option<bool>.None

//    member this.Optional
//        with set value =
//            optional <- Option.Some value

//    member this.Required
//        with set value =
//            required <- Option.Some value

//    member this.AllowNulls
//        with set value =
//            allowNulls <- Option.Some value

//    abstract member ApplyValueSettings : valueSettings:ValueSettings -> ValueSettings

//    default this.ApplyValueSettings(valueSettings) =
//        let forceOptional = optional |> Option.defaultValue valueSettings.ForceOptional
//        let forceRequired = required |> Option.defaultValue valueSettings.ForceRequired
//        let allowNullValues = allowNulls |> Option.defaultValue valueSettings.AllowNullValues
//        valueSettings
//        |> ValueSettings.forceOptional forceOptional
//        |> ValueSettings.forceRequired forceRequired
//        |> ValueSettings.allowNullValues allowNullValues
