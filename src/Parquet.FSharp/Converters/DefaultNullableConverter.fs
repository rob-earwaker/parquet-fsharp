namespace Parquet.FSharp

type internal DefaultNullableConverter private () =
    let tryCreateSerializer (nullableInfo: NullableInfo) settings =
        let valueSerializer = Serializer.resolve nullableInfo.ValueType settings
        // Parquet doesn't support nested optional values, so if the value is
        // optional then we can't serialize it.
        if valueSerializer.IsOptional
        then Option.None
        else
            let dotnetType = nullableInfo.Type
            let isNull = nullableInfo.IsNull
            let getValue = nullableInfo.GetValue
            Serializer.optional dotnetType valueSerializer isNull getValue
            |> Option.Some

    // Create a deserializer for a required field value. There's no need to wrap
    // the value deserializer in an {OptionalDeserializer} in this case since
    // there will never be any NULL values, but we do need to wrap any values we
    // deserialize as {Nullable} values so that they can be assigned to the
    // target field.
    let createRequiredDeserializer
        sourceSchema (nullableInfo: NullableInfo) settings =
        let wrapValue = nullableInfo.CreateFromValue
        // Resolve the value deserializer. The value schema is just the same as
        // the source schema since we're dealing with a required field value.
        let valueDeserializer =
            Deserializer.resolve sourceSchema nullableInfo.ValueType settings
        Deserializer.wrapAs nullableInfo.Type valueDeserializer wrapValue

    // Create a deserializer for an optional field value. In this situation we
    // need to wrap the value deserializer in an {OptionalDeserializer} to
    // handle NULL values. When we read a NULL value we create a NULL valued
    // {Nullable}. When we read a NOTNULL value we wrap it as a {Nullable}.
    let createOptionalDeserializer
        (sourceSchema: ValueSchema) (nullableInfo: NullableInfo) settings =
        // Resolve the value deserializer. Since we're dealing with an optional
        // field value and we're going to deal with this optionality by wrapping
        // the value deserializer in an {OptionalDeserializer}, we want to pass
        // down an equivalent non-optional value schema.
        let valueSchema = sourceSchema.MakeRequired()
        let valueDeserializer =
            Deserializer.resolve valueSchema nullableInfo.ValueType settings
        // Build the {OptionalDeserializer} wrapper.
        let dotnetType = nullableInfo.Type
        let createNull = nullableInfo.CreateNull
        let createFromValue = nullableInfo.CreateFromValue
        Deserializer.optional
            dotnetType valueDeserializer createNull createFromValue

    static member Instance = DefaultNullableConverter()

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            match sourceType with
            | DotnetType.Nullable nullableInfo ->
                tryCreateSerializer nullableInfo settings
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            match targetType with
            | DotnetType.Nullable nullableInfo ->
                let deserializer =
                    if sourceSchema.IsOptional
                    then createOptionalDeserializer sourceSchema nullableInfo settings
                    else createRequiredDeserializer sourceSchema nullableInfo settings
                Option.Some deserializer
            | _ -> Option.None
