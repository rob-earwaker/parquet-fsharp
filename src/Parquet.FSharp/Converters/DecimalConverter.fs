namespace Parquet.FSharp

type internal DecimalConverterSettings = {
    Precision: int
    Scale: int
    Optional: bool }
    with
    static member val Default = {
        DecimalConverterSettings.Precision = 38
        DecimalConverterSettings.Scale = 18
        DecimalConverterSettings.Optional = false }

type internal DecimalConverter(converterSettings: DecimalConverterSettings) =
    let dotnetType = typeof<decimal>
    let dataDotnetType = typeof<decimal>
    let precision = converterSettings.Precision
    let scale = converterSettings.Scale

    let requiredSerializer =
        let schema = ValueTypeSchema.decimal precision scale
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let optionalSerializer =
        requiredSerializer
        |> Serializer.optionalNonNullableTypeWrapper

    let requiredDeserializer =
        let schema = ValueTypeSchema.decimal precision scale
        let createFromDataValue = id
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    static member val Default = DecimalConverter(DecimalConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceValue, settings) =
            if sourceValue.Type <> dotnetType
            then Option.None
            else
                if converterSettings.Optional
                then Option.Some optionalSerializer
                else Option.Some requiredSerializer

        member this.TryCreateDeserializer(sourceSchema, targetValue, settings) =
            if targetValue.Type <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Decimal decimalSchema
                    when decimalSchema.Precision = precision
                        && decimalSchema.Scale = scale ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then Option.Some optionalDeserializer
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then Option.Some requiredDeserializer
                    else Option.None
                | _ -> Option.None
