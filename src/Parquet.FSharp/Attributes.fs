namespace Parquet.FSharp

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

type ParquetDateTimeFieldAttribute() =
    inherit ParquetFieldAttribute()

    let default' = DateTimeConverterSettings.Default

    member val Optional = default'.Optional with get, set
    member val Unit = default'.Unit with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            DateTimeConverter {
                Optional = this.Optional
                Unit = this.Unit }
        valueSettings |> ValueSettings.converter converter

type ParquetListFieldAttribute() =
    inherit ParquetFieldAttribute()

    let default' = ListConverterSettings.Default

    member val Optional = default'.Optional with get, set
    member val AllowNull = default'.AllowNull with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            ListConverter {
                Optional = this.Optional
                AllowNull = this.AllowNull }
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

type ParquetOptionFieldAttribute() =
    inherit ParquetFieldAttribute()

    let default' = OptionConverterSettings.Default

    member val Required = default'.Required with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            OptionConverter {
                Required = this.Required }
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
