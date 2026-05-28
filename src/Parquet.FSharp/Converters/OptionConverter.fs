namespace Parquet.FSharp

open System.Linq.Expressions

// TODO: This is almost identical to the ValueOption and Nullable converters, so
// worth considering extracting the common functionality unless this changes
// with the settings implementation.

type internal OptionConverterSettings = {
    Required: bool }
    with
    static member val Default = {
        OptionConverterSettings.Required = false }

type internal OptionConverter(converterSettings: OptionConverterSettings) =
    let tryCreateOptionalSerializer
        (optionalInfo: OptionalInfo) (optionalSettings: ValueSettings) settings =
        let valueSettings = optionalSettings.NestedValueSettings
        let valueSerializer =
            Serializer.resolveWithValueSettings
                optionalInfo.ValueType valueSettings settings
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

    // Create a serializer for an option type that is always treated as
    // required, i.e. can never be NULL. Optional values are allowed in this
    // situation because they don't result in nested optionals.
    let createRequiredSerializer
        (optionalInfo: OptionalInfo) (optionalSettings: ValueSettings) settings =
        let valueSettings = optionalSettings.NestedValueSettings
        let valueSerializer =
            Serializer.resolveWithValueSettings optionalInfo.ValueType valueSettings settings
        let unwrapValue (option: Expression) =
            // if option.IsNone then
            //     raise SerializationException(...)
            // option.Value
            Expression.Block(
                Expression.IfThen(
                    optionalInfo.IsNull option,
                    // TODO: Some of these exceptions are a little misleading
                    // since it might be the field that's been configured rather
                    // than the type. Can we include the field path?
                    Expression.FailWith<SerializationException>(
                        $"null value encountered during serialization for type '{optionalInfo.Type}'"
                        + " which has been configured as required")),
                optionalInfo.GetValue(option))
            :> Expression
        Serializer.wrapAs optionalInfo.Type valueSerializer unwrapValue

    // Attempt to create a deserializer for an optional field value. In this
    // situation we need to wrap the value deserializer in an
    // {OptionalDeserializer} to handle NULL values. When we read a NULL value
    // we convert it to the {Option.None} case. When we read a NOTNULL value we
    // wrap it in the {Option.Some} case.
    let tryCreateOptionalDeserializer
        (sourceSchema: ValueSchema) (optionalInfo: OptionalInfo)
        (optionalSettings: ValueSettings) settings =
        if not sourceSchema.IsOptional
        then Option.None
        else
            // Resolve the value deserializer. Since we're dealing with an optional
            // field value and we're going to deal with this optionality by wrapping
            // the value deserializer in an {OptionalDeserializer}, we want to pass
            // down an equivalent non-optional value schema.
            let valueSchema = sourceSchema.MakeRequired()
            let valueSettings = optionalSettings.NestedValueSettings
            let valueDeserializer =
                Deserializer.resolveWithValueSettings
                    valueSchema optionalInfo.ValueType valueSettings settings
            // Build the {OptionalDeserializer} wrapper.
            let dotnetType = optionalInfo.Type
            let createNull = optionalInfo.CreateNull
            let createFromValue = optionalInfo.CreateFromValue
            let deserializer =
                Deserializer.optional
                    dotnetType valueDeserializer createNull createFromValue
            Option.Some deserializer

    // Create a deserializer for a required field value. There's no need to wrap
    // the value deserializer in an {OptionalDeserializer} in this case since
    // there will never be any NULL values, but we do need to wrap any values we
    // deserialize in {Option.Some} cases so that they can be assigned to the
    // target option field.
    let createRequiredDeserializer
        sourceSchema (optionalInfo: OptionalInfo) (optionalSettings: ValueSettings) settings =
        // Resolve the value deserializer. The value schema is just the same as
        // the source schema since we're dealing with a required field value.
        let valueSettings = optionalSettings.NestedValueSettings
        let valueDeserializer =
            Deserializer.resolveWithValueSettings
                sourceSchema optionalInfo.ValueType valueSettings settings
        let wrapValue = optionalInfo.CreateFromValue
        Deserializer.wrapAs optionalInfo.Type valueDeserializer wrapValue

    static member val Default = OptionConverter(OptionConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, valueSettings, settings) =
            match sourceType with
            | DotnetType.Option optionalInfo ->
                if converterSettings.Required
                then Option.Some (createRequiredSerializer optionalInfo valueSettings settings)
                else tryCreateOptionalSerializer optionalInfo valueSettings settings
            | _ -> Option.None

        member this.TryCreateDeserializer(sourceSchema, targetType, valueSettings, settings) =
            match targetType with
            | DotnetType.Option optionalInfo ->
                if converterSettings.Required
                then Option.Some (createRequiredDeserializer sourceSchema optionalInfo valueSettings settings)
                else tryCreateOptionalDeserializer sourceSchema optionalInfo valueSettings settings
            | _ -> Option.None
