namespace Parquet.FSharp

type internal RecordStructConverterSettings = {
    Optional: bool }
    with
    static member val Default = {
        RecordStructConverterSettings.Optional = false }

type internal RecordStructConverter(converterSettings: RecordStructConverterSettings) =
    let createRequiredSerializer (recordInfo: RecordInfo) settings =
        let fieldSerializers =
            recordInfo.Fields
            |> Array.map (fun fieldInfo ->
                FieldSerializer.ofStructField fieldInfo settings)
        Serializer.record recordInfo.Type fieldSerializers

    let createOptionalSerializer recordInfo settings =
        createRequiredSerializer recordInfo settings
        |> Serializer.optionalNonNullableTypeWrapper

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
        |> Option.map Deserializer.optionalNonNullableTypeWrapper

    static member val Default = RecordStructConverter(RecordStructConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            match sourceValue.Type with
            | DotnetType.Record recordInfo
                when recordInfo.IsStruct ->
                if converterSettings.Optional
                then Option.Some (createOptionalSerializer recordInfo settings)
                else Option.Some (createRequiredSerializer recordInfo settings)
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            match targetValue.Type with
            | DotnetType.Record recordInfo
                when recordInfo.IsStruct ->
                match sourceSchema.Type with
                | ValueTypeSchema.Record recordSchema ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then tryCreateOptionalDeserializer recordSchema recordInfo settings
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then tryCreateRequiredDeserializer recordSchema recordInfo settings
                    else Option.None
                | _ -> Option.None
            | _ -> Option.None
