namespace Parquet.FSharp

type internal BoolConverterSettings = {
    Optional: bool }
    with
    static member val Default = {
        BoolConverterSettings.Optional = false }

type internal BoolConverter(converterSettings: BoolConverterSettings) =
    let dotnetType = typeof<bool>
    let dataDotnetType = dotnetType

    // TODO: Use static fields or pull out into module?

    let requiredSerializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let getDataValue = id
        Serializer.atomic schema dotnetType dataDotnetType getDataValue

    let optionalSerializer =
        requiredSerializer
        |> Serializer.optionalNonNullableTypeWrapper

    let requiredDeserializer =
        let schema = ValueTypeSchema.primitive dataDotnetType
        let createFromDataValue = id
        Deserializer.atomic schema dotnetType dataDotnetType createFromDataValue

    let optionalDeserializer =
        requiredDeserializer
        |> Deserializer.optionalNonNullableTypeWrapper

    static member val Default = BoolConverter(BoolConverterSettings.Default)

    interface IValueConverter with
        member this.TryCreateSerializer(sourceType, settings) =
            if sourceType <> dotnetType
            then Option.None
            else
                if converterSettings.Optional
                then option.Some optionalSerializer
                else Option.Some requiredSerializer

        member this.TryCreateDeserializer(sourceSchema, targetType, settings) =
            if targetType <> dotnetType
            then Option.None
            else
                match sourceSchema.Type with
                | ValueTypeSchema.Primitive primitiveSchema
                    when primitiveSchema.DataDotnetType = dotnetType ->
                    if sourceSchema.IsOptional && converterSettings.Optional
                    then Option.Some optionalDeserializer
                    elif not sourceSchema.IsOptional && not converterSettings.Optional
                    then Option.Some requiredDeserializer
                    else Option.None
                | _ -> Option.None
