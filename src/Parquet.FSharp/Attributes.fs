namespace Parquet.FSharp

/// Configures the settings used to serialize a `bool` value.
type ParquetBoolAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = BoolConverterSettings.Default

    member val Optional = default'.Optional with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            BoolConverter {
                Optional = this.Optional }
        valueSettings |> ValueSettings.converter converter

/// Configures the settings used to serialize a `DateTime` value.
type ParquetDateTimeAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = DateTimeConverterSettings.Default
    
    member val Unit = default'.Unit with get, set
    member val Local = default'.Local with get, set
    member val Optional = default'.Optional with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            DateTimeConverter {
                Unit = this.Unit
                Local = this.Local
                Optional = this.Optional }
        valueSettings |> ValueSettings.converter converter

/// Configures the settings used to serialize a `string` value.
type ParquetStringAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = StringConverterSettings.Default
    
    member val Optional = default'.Optional with get, set
    member val AllowNull = default'.AllowNull with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            StringConverter {
                Optional = this.Optional
                AllowNull = this.AllowNull }
        valueSettings |> ValueSettings.converter converter

/// Configures the settings used to serialize a `byte[]` value.
type ParquetByteArrayAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = ByteArrayConverterSettings.Default
    
    member val Optional = default'.Optional with get, set
    member val AllowNull = default'.AllowNull with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            ByteArrayConverter {
                Optional = this.Optional
                AllowNull = this.AllowNull }
        valueSettings |> ValueSettings.converter converter

/// Configures the settings used to serialize an F# list value.
type ParquetListAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = ListConverterSettings.Default

    member val Optional = default'.Optional with get, set
    member val AllowNull = default'.AllowNull with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            ListConverter {
                Optional = this.Optional
                AllowNull = this.AllowNull }
        valueSettings |> ValueSettings.converter converter

/// Configures the settings used to serialize a one-dimensional array value.
type ParquetArray1dAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = Array1dConverterSettings.Default

    member val Optional = default'.Optional with get, set
    member val AllowNull = default'.AllowNull with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            Array1dConverter {
                Optional = this.Optional
                AllowNull = this.AllowNull }
        valueSettings |> ValueSettings.converter converter

/// Configures the settings used to serialize a `ResizeArray` value.
type ParquetResizeArrayAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = ResizeArrayConverterSettings.Default

    member val Optional = default'.Optional with get, set
    member val AllowNull = default'.AllowNull with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            ResizeArrayConverter {
                Optional = this.Optional
                AllowNull = this.AllowNull }
        valueSettings |> ValueSettings.converter converter

/// Configures the settings used to serialize an F# record value.
type ParquetRecordAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = RecordConverterSettings.Default
    
    member val Optional = default'.Optional with get, set
    member val AllowNull = default'.AllowNull with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            RecordConverter {
                Optional = this.Optional
                AllowNull = this.AllowNull }
        valueSettings |> ValueSettings.converter converter

/// Configures the settings used to serialize an F# record struct value.
type ParquetRecordStructAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = RecordStructConverterSettings.Default
    
    member val Optional = default'.Optional with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            RecordStructConverter {
                Optional = this.Optional }
        valueSettings |> ValueSettings.converter converter

/// Configures the settings used to serialize an F# option value.
type ParquetOptionAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = OptionConverterSettings.Default

    member val Required = default'.Required with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            OptionConverter {
                Required = this.Required }
        valueSettings |> ValueSettings.converter converter

/// Configures the settings used to serialize an F# union value with no
/// associated data fields for any of the cases.
type ParquetEnumUnionAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = EnumUnionConverterSettings.Default
    
    member val Optional = default'.Optional with get, set
    member val AllowNull = default'.AllowNull with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            EnumUnionConverter {
                Optional = this.Optional
                AllowNull = this.AllowNull }
        valueSettings |> ValueSettings.converter converter

/// Configures the settings used to serialize an F# union value with a single case.
type ParquetSingleCaseUnionAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = SingleCaseUnionConverterSettings.Default
    
    member val Optional = default'.Optional with get, set
    member val AllowNull = default'.AllowNull with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            SingleCaseUnionConverter {
                Optional = this.Optional
                AllowNull = this.AllowNull }
        valueSettings |> ValueSettings.converter converter

/// Configures the settings used to serialize an F# union value with multiple
/// cases where at least one case has one or more associated data fields.
type ParquetMultiCaseUnionAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = MultiCaseUnionConverterSettings.Default
    
    member val Optional = default'.Optional with get, set
    member val AllowNull = default'.AllowNull with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            MultiCaseUnionConverter {
                Optional = this.Optional
                AllowNull = this.AllowNull }
        valueSettings |> ValueSettings.converter converter
