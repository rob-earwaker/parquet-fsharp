namespace Parquet.FSharp

type internal RecordConverterSettings = {
    Optional: bool
    AllowNull: bool }
    with
    static member val Default = {
        RecordConverterSettings.Optional = false
        RecordConverterSettings.AllowNull = false }

type internal RecordConverter(converterSettings: RecordConverterSettings) =
    let createRequiredSerializer (recordInfo: RecordInfo) settings =
        let fieldSerializers =
            recordInfo.Fields
            |> Array.map (fun fieldInfo ->
                FieldSerializer.ofField fieldInfo converterSettings.Optional settings)
        Serializer.record recordInfo.Type fieldSerializers

    let createOptionalSerializer recordInfo settings =
        createRequiredSerializer recordInfo settings
        |> Serializer.optionalNullableTypeWrapper converterSettings.AllowNull

    let tryCreateRequiredDeserializer schema (recordInfo: RecordInfo) settings =
        let fieldDeserializers =
            recordInfo.Fields
            |> Array.choose (fun fieldInfo ->
                FieldDeserializer.tryOfField schema fieldInfo settings)
        if fieldDeserializers.Length < recordInfo.Fields.Length
        then Option.None
        else
            Deserializer.record
                recordInfo.Type fieldDeserializers recordInfo.CreateFromFieldValues
            |> Option.Some

    let tryCreateOptionalDeserializer schema recordInfo settings =
        tryCreateRequiredDeserializer schema recordInfo settings
        |> Option.map (Deserializer.optionalNullableTypeWrapper converterSettings.AllowNull)

    static member val Default = RecordConverter(RecordConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            match sourceValue.Type with
            | DotnetType.Record recordInfo ->
                if converterSettings.Optional
                then Option.Some (createOptionalSerializer recordInfo settings)
                else Option.Some (createRequiredSerializer recordInfo settings)
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            match targetValue.Type with
            | DotnetType.Record recordInfo ->
                match sourceSchema.Type with
                | ValueTypeSchema.Record recordSchema ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then tryCreateOptionalDeserializer recordSchema recordInfo settings
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then tryCreateRequiredDeserializer recordSchema recordInfo settings
                    else Option.None
                | _ -> Option.None
            | _ -> Option.None
