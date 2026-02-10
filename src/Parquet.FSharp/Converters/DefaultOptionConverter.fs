namespace Parquet.FSharp

type internal DefaultOptionConverter private () =
    let tryCreateSerializer (optionInfo: OptionInfo) settings =
        let valueSerializer = Serializer.resolve optionInfo.ValueType settings
        // Parquet doesn't support nested optional values, so if the value is
        // optional then we can't serialize it.
        if valueSerializer.IsOptional
        then Option.None
        else
            let dotnetType = optionInfo.Type
            let isNull = optionInfo.IsNull
            let getValue = optionInfo.GetValue
            Serializer.optional dotnetType valueSerializer isNull getValue
            |> Option.Some

    // Create a deserializer for a required field value. There's no need to wrap
    // the value deserializer in an {OptionalDeserializer} in this case since
    // there will never be any NULL values, but we do need to wrap any values we
    // deserialize in {Option.Some} cases so that they can be assigned to the
    // target option field.
    let createRequiredDeserializer
        sourceSchema (optionInfo: OptionInfo) settings =
        let wrapValue = optionInfo.CreateFromValue
        // Resolve the value deserializer. The value schema is just the same as
        // the source schema since we're dealing with a required field value.
        let valueDeserializer =
            Deserializer.resolve sourceSchema optionInfo.ValueType settings
        Deserializer.wrapAs optionInfo.Type valueDeserializer wrapValue

    // Create a deserializer for an optional field value. In this situation we
    // need to wrap the value deserializer in an {OptionalDeserializer} to
    // handle NULL values. When we read a NULL value we convert it to the
    // {Option.None} case. When we read a NOTNULL value we wrap it in the
    // {Option.Some} case.
    let createOptionalDeserializer
        (sourceSchema: ValueSchema) (optionInfo: OptionInfo) settings =
        // Resolve the value deserializer. Since we're dealing with an optional
        // field value and we're going to deal with this optionality by wrapping
        // the value deserializer in an {OptionalDeserializer}, we want to pass
        // down an equivalent non-optional value schema.
        let valueSchema = sourceSchema.MakeRequired()
        let valueDeserializer =
            Deserializer.resolve valueSchema optionInfo.ValueType settings
        // Build the {OptionalDeserializer} wrapper.
        let dotnetType = optionInfo.Type
        let createNull = optionInfo.CreateNull
        let createFromValue = optionInfo.CreateFromValue
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    static member Instance = DefaultOptionConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            match sourceType with
            | DotnetType.Option optionInfo ->
                tryCreateSerializer optionInfo settings
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            match targetType with
            | DotnetType.Option optionInfo ->
                let deserializer =
                    if sourceSchema.IsOptional
                    then createOptionalDeserializer sourceSchema optionInfo settings
                    else createRequiredDeserializer sourceSchema optionInfo settings
                Option.Some deserializer
            | _ -> Option.None
