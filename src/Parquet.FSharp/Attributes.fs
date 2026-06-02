namespace Parquet.FSharp

type ParquetBoolAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = BoolConverterSettings.Default

    member val Optional = default'.Optional with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            BoolConverter {
                Optional = this.Optional }
        valueSettings |> ValueSettings.converter converter

type ParquetDateTimeAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = DateTimeConverterSettings.Default
    
    member val Unit = default'.Unit with get, set
    member val Optional = default'.Optional with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            DateTimeConverter {
                Unit = this.Unit
                Optional = this.Optional }
        valueSettings |> ValueSettings.converter converter

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

type ParquetRecordStructAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = RecordStructConverterSettings.Default
    
    member val Optional = default'.Optional with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            RecordStructConverter {
                Optional = this.Optional }
        valueSettings |> ValueSettings.converter converter

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

type ParquetOptionAttribute() =
    inherit ParquetValueSettingsAttribute()

    let default' = OptionConverterSettings.Default

    member val Required = default'.Required with get, set

    override this.ApplyValueSettings(valueSettings) =
        let converter =
            OptionConverter {
                Required = this.Required }
        valueSettings |> ValueSettings.converter converter
