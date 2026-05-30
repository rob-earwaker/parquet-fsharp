namespace Parquet.FSharp

type internal NullableConverter private () =
    let tryCreateSerializer (optionalInfo: OptionalInfo) optionalValue settings =
        let value = optionalValue |> ValueDefinition.forNestedValue optionalInfo.ValueType
        let valueSerializer = Serializer.resolve value settings
        // Parquet doesn't support nested optional values, so if the value is
        // optional then we can't serialize it.
        if valueSerializer.IsOptional
        then Option.None
        else
            let dotnetType = optionalInfo.Type
            let isNull = optionalInfo.IsNull
            let getValue = optionalInfo.GetValue
            Serializer.optional dotnetType valueSerializer isNull getValue
            |> Option.Some

    // Create a deserializer for an optional field value. In this situation we
    // need to wrap the value deserializer in an {OptionalDeserializer} to
    // handle NULL values. When we read a NULL value we create a NULL valued
    // {Nullable}. When we read a NOTNULL value we wrap it as a {Nullable}.
    let createOptionalDeserializer
        (sourceSchema: ValueSchema) (optionalInfo: OptionalInfo) optionalValue settings =
        // Resolve the value deserializer. Since we're dealing with an optional
        // field value and we're going to deal with this optionality by wrapping
        // the value deserializer in an {OptionalDeserializer}, we want to pass
        // down an equivalent non-optional value schema.
        let valueSchema = sourceSchema.MakeRequired()
        let value = optionalValue |> ValueDefinition.forNestedValue optionalInfo.ValueType
        let valueDeserializer = Deserializer.resolve valueSchema value settings
        // Build the {OptionalDeserializer} wrapper.
        let dotnetType = optionalInfo.Type
        let createNull = optionalInfo.CreateNull
        let createFromValue = optionalInfo.CreateFromValue
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    // Create a deserializer for a required field value. There's no need to wrap
    // the value deserializer in an {OptionalDeserializer} in this case since
    // there will never be any NULL values, but we do need to wrap any values we
    // deserialize as {Nullable} values so that they can be assigned to the
    // target field.
    let createRequiredDeserializer
        sourceSchema (optionalInfo: OptionalInfo) optionalValue settings =
        // Resolve the value deserializer. The value schema is just the same as
        // the source schema since we're dealing with a required field value.
        let value = optionalValue |> ValueDefinition.forNestedValue optionalInfo.ValueType
        let valueDeserializer = Deserializer.resolve sourceSchema value settings
        let wrapValue = optionalInfo.CreateFromValue
        Deserializer.wrapAs optionalInfo.Type valueDeserializer wrapValue

    static member val Default = NullableConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            match sourceValue.Type with
            | DotnetType.Nullable optionalInfo ->
                tryCreateSerializer optionalInfo sourceValue settings
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            match targetValue.Type with
            | DotnetType.Nullable optionalInfo ->
                let deserializer =
                    if sourceSchema.IsOptional
                    then createOptionalDeserializer sourceSchema optionalInfo targetValue settings
                    else createRequiredDeserializer sourceSchema optionalInfo targetValue settings
                Option.Some deserializer
            | _ -> Option.None
